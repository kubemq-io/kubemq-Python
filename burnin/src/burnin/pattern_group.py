"""PatternGroup: coordinator for N ChannelWorkers per pattern (v2 multi-channel).

Each PatternGroup holds all workers for one pattern across all channels.
Provides lifecycle control (start/stop consumers/producers) and
aggregate stat methods for report/verdict.

Async version: start methods are async, stop methods stay sync (just set flags).
"""

from __future__ import annotations

import logging
from typing import Any

from burnin.config import PatternConfig, Config, RPC_PATTERNS, PUBSUB_PATTERNS
from burnin.peak_rate import LatencyAccumulator
from burnin.worker.base import BaseWorker
from burnin.worker.events import EventsWorker
from burnin.worker.events_store import EventsStoreWorker
from burnin.worker.queue_stream import QueueStreamWorker
from burnin.worker.queue_simple import QueueSimpleWorker
from burnin.worker.commands import CommandsWorker
from burnin.worker.queries import QueriesWorker

logger = logging.getLogger("burnin")

SDK = "python"


def _create_worker(
    pattern: str,
    cfg: Config,
    channel_name: str,
    rate: int,
    channel_index: int,
    pattern_config: PatternConfig,
    run_id: str,
    client: Any,
) -> BaseWorker:
    """Factory function to create the appropriate worker for a pattern."""
    kwargs = dict(
        cfg=cfg,
        client=client,
        channel_name=channel_name,
        rate=rate,
        channel_index=channel_index,
        pattern_config=pattern_config,
        run_id=run_id,
    )
    if pattern == "events":
        return EventsWorker(**kwargs)
    elif pattern == "events_store":
        return EventsStoreWorker(**kwargs)
    elif pattern == "queue_stream":
        return QueueStreamWorker(**kwargs)
    elif pattern == "queue_simple":
        return QueueSimpleWorker(**kwargs)
    elif pattern == "commands":
        return CommandsWorker(**kwargs)
    elif pattern == "queries":
        return QueriesWorker(**kwargs)
    else:
        raise ValueError(f"Unknown pattern: {pattern}")


class PatternGroup:
    """Holds N ChannelWorkers for a single pattern.

    Manages shared pattern-level LatencyAccumulator (dual-write),
    lifecycle methods, and aggregation across channels.
    """

    def __init__(
        self,
        pattern: str,
        pattern_config: PatternConfig,
        config: Config,
        run_id: str,
        clients: list[Any],
        sdk: str = SDK,
    ) -> None:
        self.pattern = pattern
        self.pattern_config = pattern_config
        self.config = config
        self.run_id = run_id

        # Pattern-level shared latency accumulators
        self.pattern_latency_accum = LatencyAccumulator()
        self.pattern_rpc_latency_accum = LatencyAccumulator()

        # Create workers for each channel, distributing across clients round-robin
        self.channel_workers: list[BaseWorker] = []
        self.channel_names: list[str] = []

        for i in range(pattern_config.channels):
            channel_index = i + 1  # 1-based
            channel_name = f"{sdk}_burnin_{run_id}_{pattern}_{channel_index:04d}"
            self.channel_names.append(channel_name)

            # Round-robin client assignment
            client = clients[i % len(clients)]

            worker = _create_worker(
                pattern=pattern,
                cfg=config,
                channel_name=channel_name,
                rate=pattern_config.rate,
                channel_index=channel_index,
                pattern_config=pattern_config,
                run_id=run_id,
                client=client,
            )
            # Wire up pattern-level accumulators
            worker.pattern_latency_accum = self.pattern_latency_accum
            worker.pattern_rpc_latency_accum = self.pattern_rpc_latency_accum
            self.channel_workers.append(worker)

    # --- Lifecycle ---

    async def start_consumers(self) -> None:
        """Start all consumers/responders across all channels."""
        for w in self.channel_workers:
            await w.start_consumers_only()
        logger.info("consumers started for %s (%d channels)", self.pattern, len(self.channel_workers))

    async def start_producers(self) -> None:
        """Start all producers/senders across all channels."""
        for w in self.channel_workers:
            await w.start_producers_only()
        logger.info("producers started for %s (%d channels)", self.pattern, len(self.channel_workers))

    def stop_producers(self) -> None:
        """Stop all producers across all channels."""
        for w in self.channel_workers:
            w.stop_producers()

    def stop_consumers(self) -> None:
        """Stop all consumers across all channels."""
        for w in self.channel_workers:
            w.stop_consumers()

    async def stop(self) -> None:
        """Stop all workers and await task completion."""
        for w in self.channel_workers:
            await w.stop()

    def reset_after_warmup(self) -> None:
        """Reset all counters across all channel workers and pattern-level accumulators."""
        for w in self.channel_workers:
            w.reset_after_warmup()
        self.pattern_latency_accum.reset()
        self.pattern_rpc_latency_accum.reset()

    def set_reconnection_callback(self, cb: Any) -> None:
        """Set reconnection callback for all channel workers."""
        for w in self.channel_workers:
            w._on_reconnection_cb = cb

    def set_client(self, client: Any) -> None:
        """Update client reference for all channel workers (after reconnect)."""
        for w in self.channel_workers:
            w.set_client(client)

    # --- Aggregation ---

    def total_sent(self) -> int:
        return sum(w.sent_count for w in self.channel_workers)

    def total_received(self) -> int:
        return sum(w.received_count for w in self.channel_workers)

    def total_lost(self) -> int:
        return sum(w.tracker.total_lost() for w in self.channel_workers)

    def total_duplicated(self) -> int:
        return sum(w.tracker.total_duplicates() for w in self.channel_workers)

    def total_corrupted(self) -> int:
        return sum(w.corrupted_count for w in self.channel_workers)

    def total_errors(self) -> int:
        return sum(w.error_count for w in self.channel_workers)

    def total_bytes_sent(self) -> int:
        return sum(w.bytes_sent for w in self.channel_workers)

    def total_bytes_received(self) -> int:
        return sum(w.bytes_received for w in self.channel_workers)

    def total_out_of_order(self) -> int:
        return sum(w.tracker.total_out_of_order() for w in self.channel_workers)

    def total_reconnections(self) -> int:
        """Connection-level: max across channels (shared connection)."""
        if not self.channel_workers:
            return 0
        return max(w.reconnection_count for w in self.channel_workers)

    def max_downtime_seconds(self) -> float:
        """Max downtime across channels (shared connection = same downtime)."""
        if not self.channel_workers:
            return 0.0
        return max(w.downtime_seconds for w in self.channel_workers)

    def total_rpc_success(self) -> int:
        return sum(w.rpc_success_count for w in self.channel_workers)

    def total_rpc_timeout(self) -> int:
        return sum(w.rpc_timeout_count for w in self.channel_workers)

    def total_rpc_error(self) -> int:
        return sum(w.rpc_error_count for w in self.channel_workers)

    def total_unconfirmed(self) -> int:
        return sum(w.unconfirmed_count for w in self.channel_workers)

    def max_peak_rate(self) -> float:
        """Max peak rate across all channels."""
        if not self.channel_workers:
            return 0.0
        return max(w.peak_rate.peak() for w in self.channel_workers)

    def aggregate_sliding_rate(self) -> float:
        """Sum of sliding rates across all channels."""
        return sum(w.sliding_rate.rate() for w in self.channel_workers)
