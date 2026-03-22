"""BaseWorker: shared state, counters, 2-phase shutdown, recording logic.

v2: Accepts channel_name, rate, channel_index directly. Supports
pattern_latency_accum for dual-write latency accumulation.

Async version: uses asyncio.Event/Task instead of threading.Event/Thread.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING, Coroutine

from burnin import metrics_collector as mc
from burnin.payload import SizeDistribution, decode, verify_crc
from burnin.peak_rate import LatencyAccumulator, PeakRateTracker, SlidingRateTracker
from burnin.rate_limiter import AsyncRateLimiter
from burnin.timestamp_store import SendTimestampStore
from burnin.tracker import Tracker

if TYPE_CHECKING:
    from burnin.config import Config

logger = logging.getLogger("burnin")


class BaseWorker:
    """Base class for all 6 pattern workers.

    Provides 2-phase shutdown, rate limiting, sequence tracking,
    latency measurement, and all shared counters.

    All producer/consumer loops run as asyncio.Tasks on the event loop.
    """

    def __init__(
        self,
        pattern: str,
        cfg: Config,
        channel_name: str,
        rate: int,
        channel_index: int = 0,
    ) -> None:
        self.pattern = pattern
        self.cfg = cfg
        self.channel_name = channel_name
        self.channel_index = channel_index
        self._benchmark = cfg.mode == "benchmark"

        # 2-phase shutdown events (asyncio)
        self._producer_stop = asyncio.Event()
        self._consumer_stop = asyncio.Event()
        self._consumer_ready = asyncio.Event()

        # Tracking
        self._tracker = Tracker(reorder_window=cfg.message.reorder_window)
        self._latency_accum = LatencyAccumulator()
        self._rpc_latency_accum = LatencyAccumulator()
        self._peak_rate = PeakRateTracker()
        self._sliding_rate = SlidingRateTracker()
        self._ts_store = SendTimestampStore()

        # Pattern-level latency accumulator (set by PatternGroup)
        self.pattern_latency_accum: LatencyAccumulator | None = None
        self.pattern_rpc_latency_accum: LatencyAccumulator | None = None

        # Rate limiting (async)
        self._limiter = AsyncRateLimiter(rate)

        # Size distribution
        if cfg.message.size_mode == "distribution":
            self._size_dist = SizeDistribution(cfg.message.size_distribution)
        else:
            self._size_dist = None

        # Counters (no locks needed — single-threaded async)
        self._sent = 0
        self._received = 0
        self._corrupted = 0
        self._errors = 0
        self._reconnections = 0
        self._rpc_success = 0
        self._rpc_timeout = 0
        self._rpc_error = 0
        self._bytes_sent = 0
        self._bytes_received = 0
        self._unconfirmed = 0

        # Per-producer/consumer stats for REST API per-worker breakdown
        self._producer_stats: dict[str, dict[str, int]] = {}
        self._consumer_stats: dict[str, dict[str, int]] = {}
        self._sender_stats: dict[str, dict[str, int]] = {}
        self._responder_stats: dict[str, dict[str, int]] = {}

        # Downtime tracking (no lock needed — single-threaded)
        self._downtime_start: float | None = None
        self._downtime_total = 0.0

        # Reconnection duplicate cooldown
        self._recently_reconnected = False
        self._reconn_dup_cooldown = 0

        # Optional reconnection callback (set by engine for pattern state tracking)
        self._on_reconnection_cb: object = None

        # Tasks (replaces threads)
        self._tasks: list[asyncio.Task] = []

    # --- Properties ---

    @property
    def tracker(self) -> Tracker:
        return self._tracker

    @property
    def latency_accum(self) -> LatencyAccumulator:
        return self._latency_accum

    @property
    def rpc_latency_accum(self) -> LatencyAccumulator:
        return self._rpc_latency_accum

    @property
    def peak_rate(self) -> PeakRateTracker:
        return self._peak_rate

    @property
    def sliding_rate(self) -> SlidingRateTracker:
        return self._sliding_rate

    @property
    def ts_store(self) -> SendTimestampStore:
        return self._ts_store

    @property
    def sent_count(self) -> int:
        return self._sent

    @property
    def received_count(self) -> int:
        return self._received

    @property
    def error_count(self) -> int:
        return self._errors

    @property
    def reconnection_count(self) -> int:
        return self._reconnections

    @property
    def corrupted_count(self) -> int:
        return self._corrupted

    @property
    def rpc_success_count(self) -> int:
        return self._rpc_success

    @property
    def rpc_timeout_count(self) -> int:
        return self._rpc_timeout

    @property
    def rpc_error_count(self) -> int:
        return self._rpc_error

    @property
    def bytes_sent(self) -> int:
        return self._bytes_sent

    @property
    def bytes_received(self) -> int:
        return self._bytes_received

    @property
    def unconfirmed_count(self) -> int:
        return self._unconfirmed

    @property
    def producer_stats(self) -> dict[str, dict[str, int]]:
        return dict(self._producer_stats)

    @property
    def consumer_stats(self) -> dict[str, dict[str, int]]:
        return dict(self._consumer_stats)

    @property
    def sender_stats(self) -> dict[str, dict[str, int]]:
        return dict(self._sender_stats)

    @property
    def responder_stats(self) -> dict[str, dict[str, int]]:
        return dict(self._responder_stats)

    @property
    def consumer_ready_event(self) -> asyncio.Event:
        return self._consumer_ready

    # --- Message size ---

    def message_size(self) -> int:
        """Get message size (fixed or from distribution)."""
        if self._size_dist:
            return self._size_dist.select_size()
        return self.cfg.message.size_bytes

    # --- Rate limiting ---

    async def wait_for_rate(self) -> bool:
        """Wait for rate limiter. Returns False if producer should stop."""
        return await self._limiter.wait(self._producer_stop)

    # --- Backpressure ---

    def backpressure_check(self) -> bool:
        """Returns True if backpressure is active (producer should pause)."""
        lag = self._sent - self._received
        return lag > self.cfg.queue.max_depth

    # --- Recording methods (sync — pure in-memory, no await needed) ---

    def record_send(self, producer_id: str, seq: int, byte_count: int = 0) -> None:
        """Record a successful send (Lesson 7: only after success check)."""
        if self._benchmark:
            self._sent += 1
            self._bytes_sent += byte_count
            return
        self._sent += 1
        self._bytes_sent += byte_count
        ps = self._producer_stats.setdefault(producer_id, {"sent": 0, "errors": 0})
        ps["sent"] += 1
        self._ts_store.store(producer_id, seq)
        self._peak_rate.record()
        self._sliding_rate.record()
        mc.inc_sent(self.pattern, producer_id, byte_count)

    def record_receive(
        self,
        consumer_id: str,
        body: bytes,
        crc_tag: str,
        producer_id: str,
        seq: int,
    ) -> None:
        """Record a received message with CRC verification and sequence tracking."""
        if self._benchmark:
            # Benchmark mode: counters only, no CRC/tracker/latency/metrics
            self._received += 1
            self._bytes_received += len(body)
            return

        self._received += 1
        self._bytes_received += len(body)
        cs = self._consumer_stats.setdefault(consumer_id, {
            "received": 0, "lost": 0, "duplicated": 0, "corrupted": 0, "errors": 0,
        })
        cs["received"] += 1
        mc.inc_received(self.pattern, consumer_id, len(body))

        # CRC verification
        if not verify_crc(body, crc_tag):
            self._corrupted += 1
            cs["corrupted"] += 1
            mc.inc_corrupted(self.pattern)
            logger.warning("corrupted message: pattern=%s producer=%s seq=%d", self.pattern, producer_id, seq)
            return

        # Sequence tracking
        is_dup, is_ooo = self._tracker.record(producer_id, seq)

        if is_dup:
            cs["duplicated"] += 1
            mc.inc_duplicated(self.pattern)
            if self._recently_reconnected:
                mc.inc_reconnection_duplicates(self.pattern)
                self._reconn_dup_cooldown += 1
                if self._reconn_dup_cooldown >= 100:
                    self._recently_reconnected = False
                    self._reconn_dup_cooldown = 0
            return

        if is_ooo:
            mc.inc_out_of_order(self.pattern)

        # Latency measurement — dual-write to per-channel AND pattern-level accumulators
        send_time = self._ts_store.load_and_delete(producer_id, seq)
        if send_time is not None:
            latency = time.monotonic() - send_time
            self._latency_accum.record(latency)
            # Dual-write: pattern-level accumulator
            if self.pattern_latency_accum is not None:
                self.pattern_latency_accum.record(latency)
            mc.observe_latency(self.pattern, latency)

    def record_error(self, error_type: str, worker_id: str | None = None) -> None:
        """Record an error."""
        self._errors += 1
        mc.inc_error(self.pattern, error_type)
        if worker_id and worker_id in self._producer_stats:
            self._producer_stats[worker_id]["errors"] += 1

    def inc_reconnection(self) -> None:
        """Record a reconnection event."""
        self._reconnections += 1
        self._recently_reconnected = True
        self._reconn_dup_cooldown = 0
        mc.inc_reconnections(self.pattern)
        if self._on_reconnection_cb is not None:
            self._on_reconnection_cb(self.pattern)

    def inc_rpc_success(self, sender_id: str | None = None) -> None:
        self._rpc_success += 1
        mc.inc_rpc_response(self.pattern, "success")
        if sender_id:
            ss = self._sender_stats.setdefault(sender_id, {
                "sent": 0, "responses_success": 0, "responses_timeout": 0, "responses_error": 0,
            })
            ss["sent"] += 1
            ss["responses_success"] += 1

    def inc_rpc_timeout(self, sender_id: str | None = None) -> None:
        self._rpc_timeout += 1
        mc.inc_rpc_response(self.pattern, "timeout")
        if sender_id:
            ss = self._sender_stats.setdefault(sender_id, {
                "sent": 0, "responses_success": 0, "responses_timeout": 0, "responses_error": 0,
            })
            ss["responses_timeout"] += 1

    def inc_rpc_error(self, sender_id: str | None = None) -> None:
        self._rpc_error += 1
        mc.inc_rpc_response(self.pattern, "error")
        if sender_id:
            ss = self._sender_stats.setdefault(sender_id, {
                "sent": 0, "responses_success": 0, "responses_timeout": 0, "responses_error": 0,
            })
            ss["responses_error"] += 1

    def inc_responder_responded(self, responder_id: str) -> None:
        rs = self._responder_stats.setdefault(responder_id, {"responded": 0, "errors": 0})
        rs["responded"] += 1

    def inc_responder_error(self, responder_id: str) -> None:
        rs = self._responder_stats.setdefault(responder_id, {"responded": 0, "errors": 0})
        rs["errors"] += 1

    def record_rpc_latency(self, duration_seconds: float) -> None:
        """Record RPC latency to both per-channel and pattern-level accumulators."""
        self._rpc_latency_accum.record(duration_seconds)
        if self.pattern_rpc_latency_accum is not None:
            self.pattern_rpc_latency_accum.record(duration_seconds)

    # --- Downtime ---

    def start_downtime(self) -> None:
        if self._downtime_start is None:
            self._downtime_start = time.monotonic()

    def stop_downtime(self) -> None:
        if self._downtime_start is not None:
            self._downtime_total += time.monotonic() - self._downtime_start
            self._downtime_start = None

    @property
    def downtime_seconds(self) -> float:
        total = self._downtime_total
        if self._downtime_start is not None:
            total += time.monotonic() - self._downtime_start
        return total

    # --- Lifecycle ---

    async def start(self) -> None:
        """Start the worker (subclasses override to start tasks)."""
        raise NotImplementedError

    async def start_consumers_only(self) -> None:
        """Start only consumer/responder tasks (Gap #3: startup ordering)."""
        raise NotImplementedError

    async def start_producers_only(self) -> None:
        """Start only producer/sender tasks (Gap #3: startup ordering)."""
        raise NotImplementedError

    def stop_producers(self) -> None:
        """Signal all producer tasks to stop."""
        self._producer_stop.set()

    def stop_consumers(self) -> None:
        """Signal all consumer tasks to stop."""
        self._consumer_stop.set()

    async def stop(self) -> None:
        """Stop all tasks."""
        self._producer_stop.set()
        self._consumer_stop.set()
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    def reset_after_warmup(self) -> None:
        """Reset all counters and trackers after warmup period."""
        self._sent = 0
        self._received = 0
        self._corrupted = 0
        self._errors = 0
        self._reconnections = 0
        self._rpc_success = 0
        self._rpc_timeout = 0
        self._rpc_error = 0
        self._bytes_sent = 0
        self._bytes_received = 0
        self._unconfirmed = 0
        self._downtime_total = 0.0
        self._downtime_start = None
        self._producer_stats.clear()
        self._consumer_stats.clear()
        self._sender_stats.clear()
        self._responder_stats.clear()
        self._tracker.reset()
        self._latency_accum.reset()
        self._rpc_latency_accum.reset()
        self._peak_rate.reset()
        self._sliding_rate.reset()

    def _start_task(self, name: str, coro: Coroutine) -> asyncio.Task:
        """Start and track an asyncio task."""
        async def _wrapper() -> None:
            try:
                await coro
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("task %s failed", name)

        task = asyncio.create_task(_wrapper(), name=name)
        self._tasks.append(task)
        return task
