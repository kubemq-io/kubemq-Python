"""Engine: orchestrator for PatternGroups, warmup, periodic tasks, 2-phase shutdown.

v2 multi-channel: replaces single-worker-per-pattern with PatternGroup per pattern,
each containing N ChannelWorkers. Implements API-controlled lifecycle.

Async version: all worker operations run as asyncio.Tasks on a single event loop.
HTTP server stays as ThreadingHTTPServer in a daemon thread. Bridging via
run_coroutine_threadsafe / call_soon_threadsafe.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import json
import logging
import os
import platform
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Any

import psutil
from prometheus_client import CollectorRegistry, REGISTRY

from kubemq import (
    AsyncCQClient,
    AsyncCancellationToken,
    AsyncPubSubClient,
    AsyncQueuesClient,
    CQClient,
    CancellationToken,
    ClientConfig,
    CommandMessage,
    CommandReceived,
    CommandResponse,
    CommandsSubscription,
    EventMessage,
    EventStoreMessage,
    EventsStoreSubscription,
    EventsSubscription,
    PubSubClient,
    QueriesSubscription,
    QueryMessage,
    QueryReceived,
    QueryResponse,
    QueueMessage,
    QueuesClient,
    RetryPolicy,
)
from kubemq.pubsub.events_store_subscription import EventStoreStartPosition

from burnin import metrics_collector as mc
from burnin import payload
from burnin.config import (
    ALL_PATTERN_NAMES,
    PUBSUB_PATTERNS,
    QUEUE_PATTERNS,
    RPC_PATTERNS,
    Config,
    PatternConfig,
    PatternThresholds,
    RunContext,
    translate_api_config,
)
from burnin.disconnect import AsyncDisconnectManager
from burnin.http_server import BurninHTTPServer
from burnin.pattern_group import PatternGroup
from burnin.report import generate_verdict, print_console_report, write_json_report
from burnin.run_state import PatternState, RunState, StateMachine
from burnin.worker import ALL_PATTERNS
from burnin.worker.base import BaseWorker
from burnin.worker.commands import CommandsWorker
from burnin.worker.events import EventsWorker
from burnin.worker.events_store import EventsStoreWorker
from burnin.worker.queries import QueriesWorker
from burnin.worker.queue_simple import QueueSimpleWorker
from burnin.worker.queue_stream import QueueStreamWorker

logger = logging.getLogger("burnin")

CHANNEL_PREFIX = "python_burnin_"
WARMUP_MSG_COUNT = 3
MEMORY_BASELINE_SECONDS = 300  # 5 minutes
BURNIN_VERSION = "2.0.0"

# No-op callbacks for subscription constructors (required param, unused in async warmup)
_NOOP_EVENT_CB = lambda _: None
_NOOP_CMD_CB = lambda _: None
_NOOP_QUERY_CB = lambda _: None


class Engine:
    """Orchestrates burn-in PatternGroups with API-controlled lifecycle.

    Implements the EngineAPI protocol for the HTTP server.
    All worker operations run as asyncio.Tasks on the event loop.
    """

    def __init__(self, startup_cfg: Config) -> None:
        self._startup_cfg = startup_cfg
        self._state = StateMachine()
        self._boot_time = time.monotonic()
        self._boot_at = datetime.now(timezone.utc)

        # Event loop reference (set when async_main starts)
        self._loop: asyncio.AbstractEventLoop | None = None

        # Per-run state (reset on each new run)
        self._run_cfg: Config | None = None
        self._run_ctx: RunContext | None = None
        self._pattern_groups: dict[str, PatternGroup] = {}
        # Client pools: each pattern gets multiple clients to avoid
        # single-stream bottleneck at high channel counts.
        # ~10 channels per client keeps stream contention low.
        self._client_pools: dict[str, list] = {}
        self._all_clients: list = []  # flat list for close/cleanup
        self._run_started: float = 0.0
        self._run_started_at: datetime | None = None
        self._run_ended_at: datetime | None = None
        self._stop_event: asyncio.Event | None = None
        self._pattern_states: dict[str, str] = {}
        self._baseline_rss: float = 0.0
        self._peak_rss: float = 0.0
        self._peak_workers: int = 0
        self._memory_samples: list[float] = []
        self._test_started: float = 0.0
        self._producers_stopped: float = 0.0
        self._run_task: asyncio.Task | None = None
        self._periodic_tasks: list[asyncio.Task] = []
        self._warmup_active = False
        self._run_error: str = ""

        # Report from last completed run
        self._last_report: dict[str, Any] | None = None
        self._last_summary: dict[str, Any] | None = None

        # Snapshot of all pattern counters at producer-stop time (T2).
        self._producer_stop_snapshot: dict[str, dict[str, Any]] | None = None

        # Detect SDK version
        self._sdk_version = startup_cfg.output.sdk_version
        if not self._sdk_version:
            try:
                import kubemq
                self._sdk_version = getattr(kubemq, "__version__", "unknown")
            except Exception:
                self._sdk_version = "unknown"

    # ===================================================================
    # Helper: get all workers across all pattern groups
    # ===================================================================

    def _all_workers(self) -> list[BaseWorker]:
        """Return flat list of all workers across all pattern groups."""
        workers = []
        for pg in self._pattern_groups.values():
            workers.extend(pg.channel_workers)
        return workers

    # ===================================================================
    # EngineAPI protocol implementation (called by HTTP server thread)
    # All methods are sync — HTTP server calls them from its thread.
    # ===================================================================

    def get_state_name(self) -> str:
        return self._state.state.value

    def get_cors_origins(self) -> str:
        return self._startup_cfg.cors.origins

    def get_info(self) -> dict[str, Any]:
        return {
            "sdk": "python",
            "sdk_version": self._sdk_version,
            "burnin_version": BURNIN_VERSION,
            "burnin_spec_version": "2",
            "os": platform.system().lower(),
            "arch": platform.machine(),
            "runtime": f"python{sys.version.split()[0]}",
            "cpus": os.cpu_count() or 1,
            "memory_total_mb": int(psutil.virtual_memory().total / (1024 * 1024)),
            "pid": os.getpid(),
            "uptime_seconds": round(time.monotonic() - self._boot_time, 1),
            "started_at": self._boot_at.isoformat(),
            "state": self._state.state.value,
            "broker_address": self._startup_cfg.broker.address,
        }

    def get_broker_status(self) -> dict[str, Any]:
        """Ping broker using a temporary sync client (called from HTTP thread)."""
        address = self._startup_cfg.broker.address
        try:
            prefix = self._startup_cfg.broker.client_id_prefix
            client = PubSubClient(config=self._build_client_config(f"{prefix}-ping"))
            t0 = time.monotonic()
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(client.ping)
                info = future.result(timeout=3)
            latency_ms = (time.monotonic() - t0) * 1000
            client.close()
            version = str(info) if info else ""
            return {
                "connected": True,
                "address": address,
                "ping_latency_ms": round(latency_ms, 2),
                "server_version": version,
                "last_ping_at": datetime.now(timezone.utc).isoformat(),
            }
        except concurrent.futures.TimeoutError:
            return {
                "connected": False,
                "address": address,
                "error": "ping timed out (3s)",
            }
        except Exception as e:
            return {
                "connected": False,
                "address": address,
                "error": str(e),
            }

    def handle_run_start(self, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        ok, current = self._state.try_start()
        if not ok:
            resp: dict[str, Any] = {
                "message": "Run already active",
                "state": current.value,
            }
            if self._run_ctx:
                resp["run_id"] = self._run_ctx.run_id
                resp["started_at"] = self._run_started_at.isoformat() if self._run_started_at else ""
            return 409, resp

        # Validate and translate config
        try:
            cfg, ctx, errors = translate_api_config(body, self._startup_cfg)
        except Exception as e:
            self._state.set_error(str(e))
            return 400, {"message": f"Invalid JSON: {e}"}

        if errors:
            self._state.set_error("Configuration validation failed")
            return 400, {"message": "Configuration validation failed", "errors": errors}

        enabled_count = len(ctx.enabled_patterns)
        total_channels = sum(
            cfg.patterns[p].channels for p in ctx.enabled_patterns
        )
        self._run_cfg = cfg
        self._run_ctx = ctx

        # Clear previous run state
        self._last_report = None
        self._last_summary = None
        self._pattern_groups = {}
        self._run_error = ""
        self._run_started = time.monotonic()
        self._run_started_at = datetime.now(timezone.utc)
        self._run_ended_at = None
        self._baseline_rss = 0.0
        self._peak_rss = 0.0
        self._peak_workers = 0
        self._memory_samples = []
        self._test_started = 0.0
        self._producers_stopped = 0.0
        self._warmup_active = False
        self._pattern_states = {}
        self._producer_stop_snapshot = None
        self._periodic_tasks = []

        # Schedule async run on the event loop
        if self._loop and self._loop.is_running():
            # Create new stop event on the event loop thread
            self._loop.call_soon_threadsafe(self._schedule_run)
        else:
            self._state.set_error("Event loop not available")
            return 500, {"message": "Event loop not available"}

        return 202, {
            "status": "starting",
            "run_id": ctx.run_id,
            "message": f"run starting with {total_channels} channels across {enabled_count} patterns",
        }

    def _schedule_run(self) -> None:
        """Called on the event loop thread to create stop event and start run task."""
        self._stop_event = asyncio.Event()
        self._run_task = asyncio.create_task(self._execute_run(), name="run-executor")

    def handle_run_stop(self) -> tuple[int, dict[str, Any]]:
        ok, current = self._state.try_stop()
        if not ok:
            if current == RunState.STOPPING:
                resp: dict[str, Any] = {
                    "message": "Run is already stopping",
                    "state": "stopping",
                }
                if self._run_ctx:
                    resp["run_id"] = self._run_ctx.run_id
                return 409, resp
            resp = {"message": "No active run to stop", "state": current.value}
            return 409, resp

        # Set stop event from HTTP thread via call_soon_threadsafe
        if self._loop and self._stop_event is not None:
            self._loop.call_soon_threadsafe(self._stop_event.set)

        resp = {
            "state": "stopping",
            "message": "Graceful shutdown initiated",
        }
        if self._run_ctx:
            resp["run_id"] = self._run_ctx.run_id
        return 202, resp

    def get_run(self) -> dict[str, Any]:
        state = self._state.state
        if state == RunState.IDLE:
            return {"run_id": None, "state": "idle"}

        if not self._run_ctx or not self._run_cfg:
            return {"run_id": None, "state": state.value}

        result = self._build_run_response(state)
        if state == RunState.ERROR:
            result["error"] = self._state.error_message or self._run_error
        return result

    def get_run_status(self) -> dict[str, Any]:
        state = self._state.state
        if state == RunState.IDLE:
            return {"run_id": None, "state": "idle"}

        if state == RunState.ERROR:
            resp: dict[str, Any] = {"state": "error"}
            if self._run_ctx:
                resp["run_id"] = self._run_ctx.run_id
            resp["error"] = self._state.error_message or self._run_error
            return resp

        if not self._run_ctx:
            return {"run_id": None, "state": state.value}

        ref_time = self._test_started if self._test_started > 0 else self._run_started
        elapsed = time.monotonic() - ref_time if ref_time > 0 else 0
        duration_secs = self._run_cfg.duration_seconds if self._run_cfg else 0
        remaining = max(0, duration_secs - elapsed) if duration_secs > 0 else 0

        totals = self._compute_totals()

        # Add channels count per pattern
        pattern_info: dict[str, Any] = {}
        for pname, pg in self._pattern_groups.items():
            pattern_info[pname] = {
                "channels": pg.pattern_config.channels,
                "state": self._pattern_states.get(pname, "unknown"),
            }

        return {
            "run_id": self._run_ctx.run_id,
            "state": state.value,
            "started_at": self._run_started_at.isoformat() if self._run_started_at else "",
            "elapsed_seconds": round(elapsed, 1),
            "remaining_seconds": round(remaining, 1),
            "warmup_active": self._warmup_active,
            "totals": totals,
            "pattern_states": {
                pname: {
                    "state": pstate,
                    "channels": self._pattern_groups[pname].pattern_config.channels if pname in self._pattern_groups else 1,
                }
                for pname, pstate in self._pattern_states.items()
            },
            "patterns": pattern_info,
        }

    def get_run_config(self) -> tuple[int, dict[str, Any]]:
        if not self._run_cfg or not self._run_ctx:
            return 404, {"message": "No run configuration available"}

        state = self._state.state
        cfg = self._run_cfg
        ctx = self._run_ctx

        config_body: dict[str, Any] = {
            "version": "2",
            "mode": cfg.mode,
            "duration": cfg.duration,
            "run_id": ctx.run_id,
            "warmup": {
                "max_parallel_channels": cfg.warmup.max_parallel_channels,
                "timeout_per_channel_ms": cfg.warmup.timeout_per_channel_ms,
                "warmup_duration": cfg.warmup.warmup_duration,
            },
            "starting_timeout_seconds": ctx.starting_timeout_seconds,
            "broker": {
                "address": cfg.broker.address,
                "client_id_prefix": cfg.broker.client_id_prefix,
            },
            "patterns": {},
            "queue": {
                "poll_max_messages": cfg.queue.poll_max_messages,
                "poll_wait_timeout_seconds": cfg.queue.poll_wait_timeout_seconds,
                "auto_ack": cfg.queue.auto_ack,
                "max_depth": cfg.queue.max_depth,
            },
            "rpc": {"timeout_ms": cfg.rpc.timeout_ms},
            "message": {
                "size_mode": cfg.message.size_mode,
                "size_bytes": cfg.message.size_bytes,
                "size_distribution": cfg.message.size_distribution,
                "reorder_window": cfg.message.reorder_window,
            },
            "thresholds": {
                "max_loss_pct": cfg.thresholds.max_loss_pct,
                "max_events_loss_pct": cfg.thresholds.max_events_loss_pct,
                "max_duplication_pct": cfg.thresholds.max_duplication_pct,
                "max_p99_latency_ms": cfg.thresholds.max_p99_latency_ms,
                "max_p999_latency_ms": cfg.thresholds.max_p999_latency_ms,
                "max_error_rate_pct": cfg.thresholds.max_error_rate_pct,
                "max_memory_growth_factor": cfg.thresholds.max_memory_growth_factor,
                "max_downtime_pct": cfg.thresholds.max_downtime_pct,
                "min_throughput_pct": cfg.thresholds.min_throughput_pct,
                "max_duration": cfg.thresholds.max_duration,
            },
            "forced_disconnect": {
                "interval": cfg.forced_disconnect.interval,
                "duration": cfg.forced_disconnect.duration,
            },
            "shutdown": {
                "drain_timeout_seconds": cfg.shutdown.drain_timeout_seconds,
                "cleanup_channels": cfg.shutdown.cleanup_channels,
            },
            "metrics": {"report_interval": cfg.metrics.report_interval},
        }

        for pname in ALL_PATTERN_NAMES:
            enabled = pname in ctx.enabled_patterns
            pc = cfg.patterns.get(pname, PatternConfig(enabled=False))
            pt = ctx.pattern_thresholds.get(pname, PatternThresholds())
            pcfg: dict[str, Any] = {"enabled": enabled}
            if enabled:
                pcfg["channels"] = pc.channels
                pcfg["rate"] = pc.rate
                pcfg["thresholds"] = {
                    "max_loss_pct": pt.max_loss_pct,
                    "max_p99_latency_ms": pt.max_p99_latency_ms,
                    "max_p999_latency_ms": pt.max_p999_latency_ms,
                }
                # Channel names
                pg = self._pattern_groups.get(pname)
                if pg:
                    pcfg["channel_names"] = pg.channel_names

                if pname in RPC_PATTERNS:
                    pcfg["senders_per_channel"] = pc.senders_per_channel
                    pcfg["responders_per_channel"] = pc.responders_per_channel
                else:
                    pcfg["producers_per_channel"] = pc.producers_per_channel
                    pcfg["consumers_per_channel"] = pc.consumers_per_channel
                    if pname in PUBSUB_PATTERNS:
                        pcfg["consumer_group"] = pc.consumer_group
            config_body["patterns"][pname] = pcfg

        return 200, {
            "run_id": ctx.run_id,
            "state": state.value,
            "config": config_body,
        }

    def get_run_report(self) -> tuple[int, dict[str, Any]]:
        if self._last_report:
            return 200, self._last_report
        return 404, {"message": "No completed run report available"}

    def handle_cleanup(self) -> tuple[int, dict[str, Any]]:
        ok, current = self._state.can_cleanup()
        if not ok:
            resp: dict[str, Any] = {
                "message": "Cannot cleanup while a run is active",
                "state": current.value,
            }
            if self._run_ctx:
                resp["run_id"] = self._run_ctx.run_id
            return 409, resp

        try:
            # Cleanup uses sync clients (list/delete not available on async clients)
            deleted, failed = self._do_cleanup_sync()
            pattern_names = set()
            for ch in deleted:
                parts = ch.split("_burnin_", 1)
                if len(parts) == 2:
                    remainder = parts[1]
                    after_rid = remainder[9:] if len(remainder) > 9 else remainder
                    last_underscore = after_rid.rfind("_")
                    if last_underscore > 0:
                        pattern_names.add(after_rid[:last_underscore])
            return 200, {
                "deleted_channels": deleted,
                "failed_channels": failed,
                "message": f"cleaned {len(deleted)} channels across {len(pattern_names)} patterns",
            }
        except Exception as e:
            return 200, {
                "deleted_channels": [],
                "failed_channels": [],
                "message": f"Could not connect to broker: {e}",
            }

    # ===================================================================
    # Async run execution (runs on the event loop)
    # ===================================================================

    async def _execute_run(self) -> None:
        """Main run logic executed as asyncio task."""
        cfg = self._run_cfg
        ctx = self._run_ctx
        assert cfg is not None and ctx is not None

        # Starting timeout watchdog
        timeout_task = asyncio.create_task(
            self._starting_timeout_watchdog(ctx.starting_timeout_seconds),
            name="starting-timeout",
        )

        try:
            await self._create_clients()
            logger.info("clients created, pinging broker at %s", cfg.broker.address)
            info = await self._all_clients[0].ping()
            logger.info("broker ping ok: %s", info)
        except Exception as e:
            logger.error("broker connection failed: %s", e)
            self._run_error = f"Broker connection failed: {e}"
            self._state.set_error(f"Starting timeout exceeded ({ctx.starting_timeout_seconds}s): broker connection failed")
            self._run_ended_at = datetime.now(timezone.utc)
            self._generate_error_report()
            timeout_task.cancel()
            return

        if self._stop_event.is_set():
            await self._handle_stop_during_starting()
            timeout_task.cancel()
            return

        logger.info("skipping stale channel cleanup (channels auto-created on subscribe/send)")

        if self._stop_event.is_set():
            await self._handle_stop_during_starting()
            timeout_task.cancel()
            return

        # Set target rate gauges (v2: rate x channels)
        for pname in ctx.enabled_patterns:
            pc = cfg.patterns.get(pname, PatternConfig())
            mc.set_target_rate(pname, pc.rate * pc.channels)

        # Create PatternGroups
        self._create_pattern_groups(cfg, ctx)
        for pname in ctx.enabled_patterns:
            self._pattern_states[pname] = PatternState.STARTING.value

        # Start ALL consumers/responders across ALL patterns first
        for pname in sorted(ctx.enabled_patterns):
            pg = self._pattern_groups[pname]
            await pg.start_consumers()
            logger.info("consumers started: %s (%d channels)", pname, pg.pattern_config.channels)

        if self._stop_event.is_set():
            await self._handle_stop_during_starting()
            timeout_task.cancel()
            return

        # Warmup all channels
        await self._run_warmup(cfg)

        if self._stop_event.is_set():
            await self._handle_stop_during_starting()
            timeout_task.cancel()
            return

        # Start ALL producers/senders across ALL patterns
        for pname in sorted(ctx.enabled_patterns):
            pg = self._pattern_groups[pname]
            await pg.start_producers()
            logger.info("producers started: %s (%d channels)", pname, pg.pattern_config.channels)

        # Warmup period (benchmark mode)
        warmup_secs = cfg.warmup_duration_seconds
        if warmup_secs > 0:
            self._warmup_active = True
            mc.set_warmup_active(1)
            logger.info("warmup period: %.0fs", warmup_secs)
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=warmup_secs)
                # stop was set during warmup
                self._warmup_active = False
                mc.set_warmup_active(0)
                await self._handle_stop_during_starting()
                timeout_task.cancel()
                return
            except asyncio.TimeoutError:
                pass
            for pg in self._pattern_groups.values():
                pg.reset_after_warmup()
            self._warmup_active = False
            mc.set_warmup_active(0)
            logger.info("warmup complete, counters reset")

        # Transition to RUNNING
        timeout_task.cancel()
        self._test_started = time.monotonic()
        self._state.set_running()
        for p in ctx.enabled_patterns:
            self._pattern_states[p] = PatternState.RUNNING.value

        self._print_banner(cfg)
        self._start_periodic_tasks(cfg)

        # Disconnect manager
        disconnect_mgr = AsyncDisconnectManager(
            interval_seconds=cfg.forced_disconnect.interval_seconds,
            duration_seconds=cfg.forced_disconnect.duration_seconds,
            recreator=self,
        )
        if disconnect_mgr.enabled:
            disconnect_mgr.start()

        # Wait for duration, max_duration, or stop signal
        duration = cfg.duration_seconds
        max_dur = cfg.thresholds.max_duration_seconds
        effective = duration if duration > 0 else max_dur
        if effective <= 0:
            effective = max_dur
        logger.info("running for %.0fs (or until stopped)", effective)

        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=effective)
        except asyncio.TimeoutError:
            pass

        if disconnect_mgr.enabled:
            disconnect_mgr.stop()

        # Graceful shutdown
        await self._do_shutdown(cfg, ctx)

    async def _starting_timeout_watchdog(self, timeout_seconds: int) -> None:
        """Watchdog: if starting phase exceeds timeout, transition to error."""
        try:
            await asyncio.sleep(timeout_seconds)
            if self._state.state == RunState.STARTING:
                logger.error("starting timeout exceeded (%ds)", timeout_seconds)
                self._state.set_error(f"Starting timeout exceeded ({timeout_seconds}s)")
                self._stop_event.set()
                self._run_ended_at = datetime.now(timezone.utc)
                self._generate_error_report()
        except asyncio.CancelledError:
            pass

    async def _handle_stop_during_starting(self) -> None:
        """Handle stop signal received during starting phase."""
        logger.info("stop received during starting -- cleaning up")
        for pg in self._pattern_groups.values():
            await pg.stop()
        if self._run_cfg and self._run_cfg.shutdown.cleanup_channels and self._run_ctx:
            try:
                await asyncio.get_running_loop().run_in_executor(
                    None, self._clean_run_channels_sync, self._run_ctx.run_id,
                )
            except Exception:
                pass
        await self._close_clients()
        self._run_ended_at = datetime.now(timezone.utc)
        self._generate_minimal_report()
        self._state.set_stopped()

    async def _do_shutdown(self, cfg: Config, ctx: RunContext) -> None:
        """2-phase shutdown with drain timeout."""
        self._state.try_stop()
        self._run_ended_at = datetime.now(timezone.utc)
        logger.info("initiating 2-phase shutdown")

        drain = cfg.shutdown.drain_timeout_seconds

        # Phase 1: stop ALL producers across ALL patterns
        for p in ctx.enabled_patterns:
            self._pattern_states[p] = "draining"

        # Snapshot all counters BEFORE stopping producers
        self._producer_stop_snapshot = self._capture_pattern_snapshots()
        logger.info("producer-stop snapshot captured")

        self._producers_stopped = time.monotonic()
        for pg in self._pattern_groups.values():
            pg.stop_producers()

        logger.info("producers stopped, draining for %ds", drain)
        await asyncio.sleep(drain)

        # Phase 2: stop ALL consumers across ALL patterns
        for pg in self._pattern_groups.values():
            pg.stop_consumers()

        # Cancel periodic tasks
        for t in self._periodic_tasks:
            t.cancel()
        await asyncio.gather(*self._periodic_tasks, return_exceptions=True)

        # Wait for all worker tasks to complete
        all_tasks = []
        for w in self._all_workers():
            all_tasks.extend(w._tasks)
        if all_tasks:
            done, pending = await asyncio.wait(all_tasks, timeout=10)
            if pending:
                logger.info("cancelling %d stuck tasks", len(pending))
                for t in pending:
                    t.cancel()
                # Await cancelled tasks so CancelledError propagates
                await asyncio.gather(*pending, return_exceptions=True)

        for p in ctx.enabled_patterns:
            self._pattern_states[p] = PatternState.STOPPED.value
        logger.info("consumers stopped")

        if self._baseline_rss == 0:
            self._baseline_rss = self._get_rss_mb()

        summary = self._build_summary("stopped")
        report = self._build_report(summary, ctx)
        self._last_report = report
        self._last_summary = summary

        print_console_report(report)
        if cfg.output.report_file:
            write_json_report(report, cfg.output.report_file)

        # Close async clients first (with timeout to avoid hanging on stuck gRPC streams)
        try:
            await asyncio.wait_for(self._close_clients(), timeout=5)
        except asyncio.TimeoutError:
            logger.warning("client close timed out after 5s — forcing")

        # Cleanup channels using sync clients (separate connections)
        if cfg.shutdown.cleanup_channels:
            try:
                await asyncio.wait_for(
                    asyncio.get_running_loop().run_in_executor(
                        None, self._clean_run_channels_sync, ctx.run_id,
                    ),
                    timeout=30,
                )
            except asyncio.TimeoutError:
                logger.warning("channel cleanup timed out after 30s")
            except Exception as e:
                logger.warning("channel cleanup error: %s", e)

        self._state.set_stopped()
        logger.info("run completed, state=stopped")

    def _generate_error_report(self) -> None:
        """Generate report for error-from-startup."""
        ctx = self._run_ctx
        if not ctx:
            return
        summary = self._build_summary("error")
        error_msg = self._state.error_message or self._run_error
        verdict = generate_verdict(
            summary, self._run_cfg.thresholds if self._run_cfg else self._startup_cfg.thresholds,
            enabled_patterns=ctx.enabled_patterns,
            pattern_thresholds=ctx.pattern_thresholds,
            all_patterns_enabled=ctx.all_patterns_enabled,
            from_startup_error=error_msg,
        )
        report = self._build_report_from_verdict(summary, verdict, ctx)
        self._last_report = report

    def _generate_minimal_report(self) -> None:
        """Generate minimal report for stop-during-starting."""
        ctx = self._run_ctx
        if not ctx:
            return
        summary = self._build_summary("stopped")
        verdict = generate_verdict(
            summary, self._run_cfg.thresholds if self._run_cfg else self._startup_cfg.thresholds,
            enabled_patterns=ctx.enabled_patterns,
            pattern_thresholds=ctx.pattern_thresholds,
            all_patterns_enabled=ctx.all_patterns_enabled,
        )
        report = self._build_report_from_verdict(summary, verdict, ctx)
        self._last_report = report

    # ===================================================================
    # Legacy run() method for CLI-driven mode
    # ===================================================================

    def run(self, stop_event: threading.Event) -> None:
        """Legacy CLI entry point -- still used by --cleanup-only and direct CLI mode."""
        self._run_cfg = self._startup_cfg
        enabled = {p for p, pc in self._startup_cfg.patterns.items() if pc.enabled}
        ctx = RunContext(
            enabled_patterns=enabled,
            pattern_thresholds={},
            starting_timeout_seconds=60,
            all_patterns_enabled=len(enabled) == len(ALL_PATTERN_NAMES),
            run_id=self._startup_cfg.run_id,
        )
        self._run_ctx = ctx
        self._run_started = time.monotonic()
        self._run_started_at = datetime.now(timezone.utc)
        self._state.try_start()

        # Run async code from sync context
        async def _async_run() -> None:
            self._loop = asyncio.get_running_loop()
            self._stop_event = asyncio.Event()

            # Bridge: watch the threading.Event and set the asyncio.Event
            async def _watch_stop() -> None:
                while not stop_event.is_set():
                    await asyncio.sleep(0.1)
                self._stop_event.set()

            watch_task = asyncio.create_task(_watch_stop())
            try:
                await self._execute_run()
            finally:
                watch_task.cancel()

        asyncio.run(_async_run())

    def shutdown(self) -> bool:
        """Legacy shutdown -- returns True if verdict passed."""
        if self._last_report:
            verdict = self._last_report.get("verdict", {})
            return verdict.get("passed", False)
        return False

    # ===================================================================
    # Client management (async)
    # ===================================================================

    def _build_client_config(
        self, client_id: str, connection_pool_size: int = 1
    ) -> ClientConfig:
        recovery = self._startup_cfg.recovery
        initial_ms = int(recovery.reconnect_interval_seconds * 1000)
        max_ms = int(recovery.reconnect_max_interval_seconds * 1000)
        multiplier = recovery.reconnect_multiplier
        initial_ms = max(50, min(initial_ms, 5000))
        max_ms = max(1000, min(max_ms, 120000))
        multiplier = max(1.5, min(multiplier, 3.0))

        try:
            from kubemq.core.config import JitterType
            jitter = JitterType.FULL
        except ImportError:
            jitter = None

        retry_kwargs: dict[str, Any] = {
            "max_retries": 5,
            "initial_backoff_ms": initial_ms,
            "max_backoff_ms": max_ms,
            "backoff_multiplier": multiplier,
        }
        if jitter is not None:
            retry_kwargs["jitter"] = jitter

        return ClientConfig(
            address=self._startup_cfg.broker.address,
            client_id=client_id,
            auto_reconnect=True,
            reconnect_initial_delay_ms=initial_ms,
            reconnect_max_delay_ms=max_ms,
            reconnect_backoff_multiplier=multiplier,
            retry_policy=RetryPolicy(**retry_kwargs),
            connection_pool_size=connection_pool_size,
        )

    # Max channels per client — keeps bidi stream contention low
    CHANNELS_PER_CLIENT = 10

    async def _create_clients(self) -> None:
        """Create client pools for each pattern.

        Each pool has ceil(channels / CHANNELS_PER_CLIENT) clients so that
        no single bidi stream/gRPC channel serves more than ~10 channels.
        """
        cfg = self._run_cfg or self._startup_cfg
        ctx = self._run_ctx
        prefix = self._startup_cfg.broker.client_id_prefix
        self._client_pools = {}
        self._all_clients = []

        client_types = {
            "events": AsyncPubSubClient,
            "events_store": AsyncPubSubClient,
            "queue_stream": AsyncQueuesClient,
            "queue_simple": AsyncQueuesClient,
            "commands": AsyncCQClient,
            "queries": AsyncCQClient,
        }

        enabled = ctx.enabled_patterns if ctx else set()
        for pname in ALL_PATTERN_NAMES:
            if pname not in enabled:
                continue
            pc = cfg.patterns.get(pname, PatternConfig())
            n_channels = pc.channels
            n_clients = max(1, (n_channels + self.CHANNELS_PER_CLIENT - 1) // self.CHANNELS_PER_CLIENT)
            cls = client_types[pname]

            pool = []
            for i in range(n_clients):
                client = cls(config=self._build_client_config(
                    f"{prefix}-{pname}-{i}",
                ))
                await client.connect()
                if pname in ("commands", "queries"):
                    client.set_pipeline_concurrency(50)
                pool.append(client)
                self._all_clients.append(client)
            self._client_pools[pname] = pool
            logger.info("created %d %s client(s) for %s (%d channels)",
                        n_clients, cls.__name__.replace("Async", ""), pname, n_channels)

    async def _close_clients(self) -> None:
        """Close all async clients."""
        for client in self._all_clients:
            try:
                await client.close()
            except Exception as e:
                logger.debug("client close error: %s", e)
        self._all_clients = []
        self._client_pools = {}

    # --- AsyncClientRecreator protocol (for disconnect manager) ---

    async def close_clients_async(self) -> None:
        """Close clients (for disconnect manager)."""
        logger.info("closing all clients")
        for p in list(self._pattern_states):
            self._pattern_states[p] = PatternState.RECOVERING.value
            mc.set_active_connections(p, 0)
        await self._close_clients()

    async def recreate_clients_async(self) -> None:
        """Recreate clients (for disconnect manager)."""
        logger.info("recreating all clients")
        await self._create_clients()
        # Re-distribute pool clients to workers round-robin
        for pname, pg in self._pattern_groups.items():
            pool = self._client_pools.get(pname, [])
            if pool:
                for i, w in enumerate(pg.channel_workers):
                    w.set_client(pool[i % len(pool)])
        for p in list(self._pattern_states):
            self._pattern_states[p] = PatternState.RUNNING.value

    def _on_worker_reconnection(self, pattern: str) -> None:
        """Called by workers on subscription errors to set recovering state."""
        current = self._pattern_states.get(pattern)
        if current == PatternState.RUNNING.value:
            self._pattern_states[pattern] = PatternState.RECOVERING.value

            async def _restore_running() -> None:
                await asyncio.sleep(5)
                if self._pattern_states.get(pattern) == PatternState.RECOVERING.value:
                    self._pattern_states[pattern] = PatternState.RUNNING.value

            if self._loop and self._loop.is_running():
                asyncio.ensure_future(_restore_running())

    # ===================================================================
    # Channel management — sync clients for list/delete operations
    # ===================================================================

    def _clean_run_channels_sync(self, run_id: str) -> None:
        """Clean up channels for a specific run_id using sync clients."""
        prefix = f"python_burnin_{run_id}_"
        try:
            ps = PubSubClient(config=self._build_client_config("cleanup-pubsub"))
            qs = QueuesClient(config=self._build_client_config("cleanup-queues"))
            cq = CQClient(config=self._build_client_config("cleanup-cq"))
        except Exception as e:
            logger.warning("failed to create cleanup clients: %s", e)
            return

        count = 0
        for fn_list, fn_del in [
            (lambda: ps.list_events_channels(prefix),
             lambda n: ps.delete_events_channel(n)),
            (lambda: ps.list_events_store_channels(prefix),
             lambda n: ps.delete_events_store_channel(n)),
            (lambda: qs.list_queues_channels(prefix),
             lambda n: qs.delete_queues_channel(n)),
            (lambda: cq.list_commands_channels(prefix),
             lambda n: cq.delete_commands_channel(n)),
            (lambda: cq.list_queries_channels(prefix),
             lambda n: cq.delete_queries_channel(n)),
        ]:
            try:
                for ch in fn_list() or []:
                    try:
                        fn_del(ch.name)
                        count += 1
                    except Exception:
                        pass
            except Exception:
                pass

        for client in [ps, qs, cq]:
            try:
                client.close()
            except Exception:
                pass
        logger.info("cleaned %d channels for run %s", count, run_id)

    def _do_cleanup_sync(self) -> tuple[list[str], list[str]]:
        """Cleanup all python_burnin_* channels using sync clients."""
        ps = PubSubClient(config=self._build_client_config("cleanup-pubsub"))
        qs = QueuesClient(config=self._build_client_config("cleanup-queues"))
        cq = CQClient(config=self._build_client_config("cleanup-cq"))

        deleted: list[str] = []
        failed: list[str] = []
        for fn_list, fn_del in [
            (lambda: ps.list_events_channels(CHANNEL_PREFIX),
             lambda n: ps.delete_events_channel(n)),
            (lambda: ps.list_events_store_channels(CHANNEL_PREFIX),
             lambda n: ps.delete_events_store_channel(n)),
            (lambda: qs.list_queues_channels(CHANNEL_PREFIX),
             lambda n: qs.delete_queues_channel(n)),
            (lambda: cq.list_commands_channels(CHANNEL_PREFIX),
             lambda n: cq.delete_commands_channel(n)),
            (lambda: cq.list_queries_channels(CHANNEL_PREFIX),
             lambda n: cq.delete_queries_channel(n)),
        ]:
            try:
                for ch in fn_list() or []:
                    try:
                        fn_del(ch.name)
                        deleted.append(ch.name)
                    except Exception:
                        failed.append(ch.name)
            except Exception:
                pass

        for client in [ps, qs, cq]:
            try:
                client.close()
            except Exception:
                pass

        pattern_count = len(set(
            ch.rsplit("_", 1)[0].split("_burnin_", 1)[-1].rsplit("_", 1)[0]
            for ch in deleted
        )) if deleted else 0
        logger.info("cleaned %d channels across %d patterns", len(deleted), pattern_count)
        return deleted, failed

    # ===================================================================
    # PatternGroup creation (v2)
    # ===================================================================

    def _create_pattern_groups(self, cfg: Config, ctx: RunContext) -> None:
        """Create PatternGroups for all enabled patterns."""
        rid = ctx.run_id
        groups: dict[str, PatternGroup] = {}

        for pname in sorted(ctx.enabled_patterns):
            pc = cfg.patterns.get(pname, PatternConfig())
            clients = self._client_pools.get(pname, [])
            if not clients:
                logger.warning("no clients for pattern %s", pname)
                continue

            pg = PatternGroup(
                pattern=pname,
                pattern_config=pc,
                config=cfg,
                run_id=rid,
                clients=clients,
            )
            pg.set_reconnection_callback(self._on_worker_reconnection)
            groups[pname] = pg

        self._pattern_groups = groups

    # ===================================================================
    # Warmup (v2: async with semaphore for concurrency control)
    # ===================================================================

    async def _run_warmup(self, cfg: Config) -> None:
        """Run warmup verification across all channels of all patterns."""
        logger.info("running warmup verification")
        ctx = self._run_ctx
        if not ctx:
            return

        max_parallel = cfg.warmup.max_parallel_channels
        timeout_ms = cfg.warmup.timeout_per_channel_ms
        timeout_sec = timeout_ms / 1000.0
        sem = asyncio.Semaphore(max_parallel)

        failed_channels: list[str] = []

        for pname in sorted(ctx.enabled_patterns):
            pg = self._pattern_groups.get(pname)
            if not pg:
                continue

            # Warmup channels with concurrency limit
            channel_names = pg.channel_names

            async def _warmup_with_sem(ch: str, pat: str) -> tuple[str, bool]:
                async with sem:
                    ok = await self._warmup_single_channel(pat, ch, timeout_sec)
                    return ch, ok

            tasks = [
                asyncio.create_task(_warmup_with_sem(ch, pname))
                for ch in channel_names
            ]
            results = await asyncio.gather(*tasks)
            for ch, ok in results:
                if not ok:
                    failed_channels.append(ch)

            if failed_channels:
                break

        if failed_channels:
            error_msg = f"Warmup failed for channels: {', '.join(failed_channels[:5])}"
            if len(failed_channels) > 5:
                error_msg += f" (and {len(failed_channels) - 5} more)"
            logger.error(error_msg)
            self._state.set_error(error_msg)
            self._run_ended_at = datetime.now(timezone.utc)
            self._generate_error_report()
            raise RuntimeError(error_msg)

        logger.info("warmup verification complete for all channels")

    async def _warmup_single_channel(self, pattern: str, channel_name: str, timeout_sec: float) -> bool:
        """Warmup a single channel with up to 3 retries. Returns True on success."""
        for attempt in range(3):
            if self._stop_event.is_set():
                return False

            try:
                if pattern == "events":
                    ok = await self._warmup_events_channel(channel_name, timeout_sec)
                elif pattern == "events_store":
                    ok = await self._warmup_events_store_channel(channel_name, timeout_sec)
                elif pattern in ("queue_stream", "queue_simple"):
                    logger.info("skipping warmup for queue pattern %s channel %s", pattern, channel_name)
                    ok = True
                elif pattern in ("commands", "queries"):
                    ok = await self._warmup_rpc_channel(pattern, channel_name, timeout_sec)
                else:
                    ok = True

                if ok:
                    return True
                logger.warning("warmup attempt %d failed for %s", attempt + 1, channel_name)
            except Exception as e:
                logger.warning("warmup attempt %d error for %s: %s", attempt + 1, channel_name, e)

        logger.error("warmup failed after 3 retries for %s", channel_name)
        return False

    async def _warmup_events_channel(self, channel_name: str, timeout_sec: float) -> bool:
        received: list[bool] = []
        cancel = AsyncCancellationToken()

        sub = EventsSubscription(
            channel=channel_name,
            on_receive_event_callback=_NOOP_EVENT_CB,
        )

        # Start consumer task
        async def _consume() -> None:
            try:
                async for event in self._client_pools["events"][0].subscribe_to_events(sub, cancel):
                    tags = event.tags or {}
                    if tags.get("warmup") == "true":
                        received.append(True)
            except Exception:
                pass

        consumer_task = asyncio.create_task(_consume())
        await asyncio.sleep(0.5)

        for i in range(WARMUP_MSG_COUNT):
            body, crc_hex = payload.encode("python", "events", "warmup", i + 1, 128)
            msg = EventMessage(channel=channel_name, body=body, tags={"warmup": "true", "content_hash": crc_hex})
            try:
                await self._client_pools["events"][0].publish_event(msg)
            except Exception as e:
                logger.warning("warmup events send error on %s: %s", channel_name, e)
            await asyncio.sleep(0.1)

        # Wait for at least 1 received
        deadline = time.monotonic() + timeout_sec
        while not received and time.monotonic() < deadline:
            await asyncio.sleep(0.1)

        cancel.cancel()
        consumer_task.cancel()
        try:
            await consumer_task
        except (asyncio.CancelledError, Exception):
            pass

        ok = len(received) >= 1
        logger.info("warmup events %s: sent=%d received=%d ok=%s", channel_name, WARMUP_MSG_COUNT, len(received), ok)
        return ok

    async def _warmup_events_store_channel(self, channel_name: str, timeout_sec: float) -> bool:
        received: list[bool] = []
        cancel = AsyncCancellationToken()

        sub = EventsStoreSubscription(
            channel=channel_name,
            events_store_type=EventStoreStartPosition.StartFromNew,
            on_receive_event_callback=_NOOP_EVENT_CB,
        )

        async def _consume() -> None:
            try:
                async for event in self._client_pools["events_store"][0].subscribe_to_events_store(sub, cancel):
                    tags = event.tags or {}
                    if tags.get("warmup") == "true":
                        received.append(True)
            except Exception:
                pass

        consumer_task = asyncio.create_task(_consume())
        await asyncio.sleep(0.5)

        for i in range(WARMUP_MSG_COUNT):
            body, crc_hex = payload.encode("python", "events_store", "warmup", i + 1, 128)
            msg = EventStoreMessage(channel=channel_name, body=body, tags={"warmup": "true", "content_hash": crc_hex})
            try:
                result = await self._client_pools["events_store"][0].send_event_store(msg)
                if not result.sent:
                    logger.warning("warmup events_store not confirmed on %s: %s", channel_name, result.error)
            except Exception as e:
                logger.warning("warmup events_store send error on %s: %s", channel_name, e)
            await asyncio.sleep(0.1)

        deadline = time.monotonic() + timeout_sec
        while not received and time.monotonic() < deadline:
            await asyncio.sleep(0.1)

        cancel.cancel()
        consumer_task.cancel()
        try:
            await consumer_task
        except (asyncio.CancelledError, Exception):
            pass

        ok = len(received) >= 1
        logger.info("warmup events_store %s: sent=%d received=%d ok=%s", channel_name, WARMUP_MSG_COUNT, len(received), ok)
        return ok

    async def _warmup_rpc_channel(self, pattern: str, channel_name: str, timeout_sec: float) -> bool:
        cancel = AsyncCancellationToken()
        responded: list[bool] = []

        if pattern == "commands":
            sub = CommandsSubscription(
                channel=channel_name,
                on_receive_command_callback=_NOOP_CMD_CB,
            )

            async def _respond() -> None:
                try:
                    async for cmd in self._client_pools["commands"][0].subscribe_to_commands(sub, cancel):
                        try:
                            resp = CommandResponse(command_received=cmd, is_executed=True)
                            await self._client_pools["commands"][0].send_response(resp)
                            responded.append(True)
                        except Exception as e:
                            logger.warning("warmup %s respond error on %s: %s", pattern, channel_name, e)
                except Exception:
                    pass
        else:
            sub = QueriesSubscription(
                channel=channel_name,
                on_receive_query_callback=_NOOP_QUERY_CB,
            )

            async def _respond() -> None:
                try:
                    async for query in self._client_pools["queries"][0].subscribe_to_queries(sub, cancel):
                        try:
                            resp = QueryResponse(query_received=query, is_executed=True, body=query.body)
                            await self._client_pools["queries"][0].send_response(resp)
                            responded.append(True)
                        except Exception as e:
                            logger.warning("warmup %s respond error on %s: %s", pattern, channel_name, e)
                except Exception:
                    pass

        responder_task = asyncio.create_task(_respond())
        await asyncio.sleep(0.5)

        rpc_timeout = max(int(self._run_cfg.rpc.timeout_ms / 1000) if self._run_cfg else 5, 5)
        success = 0

        for i in range(WARMUP_MSG_COUNT):
            body, crc_hex = payload.encode("python", pattern, "warmup", i + 1, 128)
            try:
                if pattern == "commands":
                    cmd_msg = CommandMessage(channel=channel_name, body=body,
                                             tags={"warmup": "true", "content_hash": crc_hex},
                                             timeout_in_seconds=rpc_timeout)
                    await self._client_pools["commands"][0].send_command(cmd_msg)
                else:
                    query_msg = QueryMessage(channel=channel_name, body=body,
                                              tags={"warmup": "true", "content_hash": crc_hex},
                                              timeout_in_seconds=rpc_timeout)
                    await self._client_pools["queries"][0].send_query(query_msg)
                success += 1
            except Exception as e:
                logger.warning("warmup %s rpc error on %s: %s", pattern, channel_name, e)
            await asyncio.sleep(0.1)

        await asyncio.sleep(0.5)
        cancel.cancel()
        responder_task.cancel()
        try:
            await responder_task
        except (asyncio.CancelledError, Exception):
            pass

        ok = success >= 1
        logger.info("warmup %s %s: sent=%d responded=%d ok=%s", pattern, channel_name, success, len(responded), ok)
        return ok

    # ===================================================================
    # Periodic tasks (asyncio tasks instead of daemon threads)
    # ===================================================================

    def _start_periodic_tasks(self, cfg: Config) -> None:
        tasks = [
            asyncio.create_task(self._periodic_reporter(cfg), name="periodic-reporter"),
            asyncio.create_task(self._peak_rate_advancer(), name="peak-rate-advancer"),
            asyncio.create_task(self._uptime_tracker(), name="uptime-tracker"),
            asyncio.create_task(self._memory_tracker(), name="memory-tracker"),
            asyncio.create_task(self._timestamp_purger(), name="timestamp-purger"),
        ]
        self._periodic_tasks = tasks

    async def _periodic_reporter(self, cfg: Config) -> None:
        interval = cfg.metrics.report_interval_seconds
        try:
            while True:
                await asyncio.sleep(interval)
                if self._stop_event.is_set():
                    break

                ref_time = self._test_started if self._test_started > 0 else self._run_started
                elapsed = time.monotonic() - ref_time
                rss = self._get_rss_mb()

                for pg in self._pattern_groups.values():
                    for w in pg.channel_workers:
                        gaps = w.tracker.detect_gaps()
                        for pid, delta in gaps.items():
                            mc.inc_lost(w.pattern, delta)
                        lag = w.sent_count - w.received_count
                        mc.set_consumer_lag(w.pattern, max(0, lag))
                    mc.set_actual_rate(pg.pattern, pg.aggregate_sliding_rate())

                uptime_str = self._format_duration(elapsed)
                header = (f"BURN-IN STATUS | uptime={uptime_str} "
                          f"mode={cfg.mode} rss={rss:.0f}MB")
                lines = [header]
                for pname, pg in self._pattern_groups.items():
                    rate = pg.aggregate_sliding_rate()
                    ch_count = pg.pattern_config.channels
                    ch_label = f"({ch_count}ch)" if ch_count > 1 else ""
                    if pname in RPC_PATTERNS:
                        lines.append(
                            f"  {pname}{ch_label:<14s} sent={pg.total_sent():<8d} "
                            f"resp={pg.total_rpc_success():<8d} tout={pg.total_rpc_timeout():<4d} "
                            f"err={pg.total_errors():<4d} p99={pg.pattern_rpc_latency_accum.percentile_ms(99.0):.1f}ms "
                            f"rate={rate:.0f}/s"
                        )
                    else:
                        lines.append(
                            f"  {pname}{ch_label:<14s} sent={pg.total_sent():<8d} recv={pg.total_received():<8d} "
                            f"lost={pg.total_lost():<4d} dup={pg.total_duplicated():<4d} "
                            f"err={pg.total_errors():<4d} p99={pg.pattern_latency_accum.percentile_ms(99.0):.1f}ms "
                            f"rate={rate:.0f}/s"
                        )
                logger.info("\n".join(lines))
        except asyncio.CancelledError:
            pass

    @staticmethod
    def _format_duration(secs: float) -> str:
        s = int(secs)
        if s >= 3600:
            return f"{s // 3600}h{(s % 3600) // 60}m{s % 60}s"
        if s >= 60:
            return f"{s // 60}m{s % 60}s"
        return f"{s}s"

    async def _peak_rate_advancer(self) -> None:
        try:
            while True:
                await asyncio.sleep(1.0)
                if self._stop_event.is_set():
                    break
                for w in self._all_workers():
                    w.peak_rate.advance()
                    w.sliding_rate.advance()
        except asyncio.CancelledError:
            pass

    async def _uptime_tracker(self) -> None:
        try:
            while True:
                await asyncio.sleep(1.0)
                if self._stop_event.is_set():
                    break
                mc.set_uptime(time.monotonic() - self._boot_time)
                # Count active asyncio tasks instead of threads
                n_tasks = len(asyncio.all_tasks())
                mc.set_active_workers(n_tasks)
                self._peak_workers = max(self._peak_workers, n_tasks)
        except asyncio.CancelledError:
            pass

    async def _memory_tracker(self) -> None:
        try:
            while True:
                await asyncio.sleep(10.0)
                if self._stop_event.is_set():
                    break
                rss = self._get_rss_mb()
                self._memory_samples.append(rss)
                if rss > self._peak_rss:
                    self._peak_rss = rss
                elapsed = time.monotonic() - (self._test_started or self._run_started)
                if elapsed >= MEMORY_BASELINE_SECONDS and self._baseline_rss == 0:
                    self._baseline_rss = rss
                    logger.info("memory baseline set: %.1f MB at %.0fs", rss, elapsed)
        except asyncio.CancelledError:
            pass

    async def _timestamp_purger(self) -> None:
        try:
            while True:
                await asyncio.sleep(60.0)
                if self._stop_event.is_set():
                    break
                for w in self._all_workers():
                    w.ts_store.purge(60.0)
        except asyncio.CancelledError:
            pass

    # ===================================================================
    # Snapshot capture at producer-stop time (T2)
    # ===================================================================

    def _capture_pattern_snapshots(self) -> dict[str, dict[str, Any]]:
        """Read all live counters from every PatternGroup and return a snapshot dict."""
        snapshots: dict[str, dict[str, Any]] = {}
        for pname, pg in self._pattern_groups.items():
            snap: dict[str, Any] = {
                "sent": pg.total_sent(),
                "received": pg.total_received(),
                "lost": pg.total_lost(),
                "duplicated": pg.total_duplicated(),
                "corrupted": pg.total_corrupted(),
                "out_of_order": pg.total_out_of_order(),
                "errors": pg.total_errors(),
                "reconnections": pg.total_reconnections(),
                "bytes_sent": pg.total_bytes_sent(),
                "bytes_received": pg.total_bytes_received(),
                "rpc_success": pg.total_rpc_success(),
                "rpc_timeout": pg.total_rpc_timeout(),
                "rpc_error": pg.total_rpc_error(),
                "unconfirmed": pg.total_unconfirmed(),
                "peak_rate": pg.max_peak_rate(),
                "downtime_seconds": pg.max_downtime_seconds(),
                "latency_p50_ms": pg.pattern_latency_accum.percentile_ms(50.0),
                "latency_p95_ms": pg.pattern_latency_accum.percentile_ms(95.0),
                "latency_p99_ms": pg.pattern_latency_accum.percentile_ms(99.0),
                "latency_p999_ms": pg.pattern_latency_accum.percentile_ms(99.9),
                "rpc_latency_p50_ms": pg.pattern_rpc_latency_accum.percentile_ms(50.0),
                "rpc_latency_p95_ms": pg.pattern_rpc_latency_accum.percentile_ms(95.0),
                "rpc_latency_p99_ms": pg.pattern_rpc_latency_accum.percentile_ms(99.0),
                "rpc_latency_p999_ms": pg.pattern_rpc_latency_accum.percentile_ms(99.9),
                "_channel_workers_snapshot": [],
            }

            for w in pg.channel_workers:
                w_snap: dict[str, Any] = {
                    "sent_count": w.sent_count,
                    "received_count": w.received_count,
                    "corrupted_count": w.corrupted_count,
                    "error_count": w.error_count,
                    "bytes_sent": w.bytes_sent,
                    "bytes_received": w.bytes_received,
                    "rpc_success_count": w.rpc_success_count,
                    "rpc_timeout_count": w.rpc_timeout_count,
                    "rpc_error_count": w.rpc_error_count,
                    "unconfirmed_count": w.unconfirmed_count,
                    "reconnection_count": w.reconnection_count,
                    "downtime_seconds": w.downtime_seconds,
                    "tracker_lost": w.tracker.total_lost(),
                    "tracker_duplicates": w.tracker.total_duplicates(),
                    "tracker_out_of_order": w.tracker.total_out_of_order(),
                    "producer_stats": {k: dict(v) for k, v in w.producer_stats.items()},
                    "consumer_stats": {k: dict(v) for k, v in w.consumer_stats.items()},
                    "sender_stats": {k: dict(v) for k, v in w.sender_stats.items()},
                    "responder_stats": {k: dict(v) for k, v in w.responder_stats.items()},
                }
                snap["_channel_workers_snapshot"].append(w_snap)

            snapshots[pname] = snap
        return snapshots

    # ===================================================================
    # Summary / Response builders (v2: aggregate from PatternGroups)
    # ===================================================================

    def _build_summary(self, status: str = "running") -> dict[str, Any]:
        cfg = self._run_cfg or self._startup_cfg
        ctx = self._run_ctx
        ref_time = self._test_started if self._test_started > 0 else self._run_started
        end_time = self._producers_stopped if self._producers_stopped > 0 else time.monotonic()
        elapsed = end_time - ref_time if ref_time > 0 else 0
        patterns: dict[str, dict[str, Any]] = {}

        for pname, pg in self._pattern_groups.items():
            pc = pg.pattern_config
            target_rate = pc.rate * pc.channels

            psnap = self._producer_stop_snapshot.get(pname) if self._producer_stop_snapshot else None

            if psnap:
                sent = psnap["sent"]
                received = psnap["received"]
                lost = psnap["lost"]
                duplicated = psnap["duplicated"]
                corrupted = psnap["corrupted"]
                out_of_order = psnap["out_of_order"]
                errors = psnap["errors"]
                reconnections = psnap["reconnections"]
                downtime_seconds = psnap["downtime_seconds"]
                bytes_sent = psnap["bytes_sent"]
                bytes_received = psnap["bytes_received"]
                peak_rate = psnap["peak_rate"]
                lat_p50 = psnap["latency_p50_ms"]
                lat_p95 = psnap["latency_p95_ms"]
                lat_p99 = psnap["latency_p99_ms"]
                lat_p999 = psnap["latency_p999_ms"]
            else:
                sent = pg.total_sent()
                received = pg.total_received()
                lost = pg.total_lost()
                duplicated = pg.total_duplicated()
                corrupted = pg.total_corrupted()
                out_of_order = pg.total_out_of_order()
                errors = pg.total_errors()
                reconnections = pg.total_reconnections()
                downtime_seconds = pg.max_downtime_seconds()
                bytes_sent = pg.total_bytes_sent()
                bytes_received = pg.total_bytes_received()
                peak_rate = pg.max_peak_rate()
                lat_p50 = pg.pattern_latency_accum.percentile_ms(50.0)
                lat_p95 = pg.pattern_latency_accum.percentile_ms(95.0)
                lat_p99 = pg.pattern_latency_accum.percentile_ms(99.0)
                lat_p999 = pg.pattern_latency_accum.percentile_ms(99.9)

            loss_pct = (lost / sent * 100) if sent > 0 else 0.0
            avg_throughput = sent / elapsed if elapsed > 0 else 0.0

            ps: dict[str, Any] = {
                "enabled": True,
                "status": self._pattern_states.get(pname, "unknown"),
                "channels": pc.channels,
                "sent": sent,
                "received": received,
                "lost": lost,
                "duplicated": duplicated,
                "corrupted": corrupted,
                "out_of_order": out_of_order,
                "loss_pct": loss_pct,
                "errors": errors,
                "reconnections": reconnections,
                "downtime_seconds": downtime_seconds,
                "latency_p50_ms": lat_p50,
                "latency_p95_ms": lat_p95,
                "latency_p99_ms": lat_p99,
                "latency_p999_ms": lat_p999,
                "latency": {
                    "p50_ms": round(lat_p50, 1),
                    "p95_ms": round(lat_p95, 1),
                    "p99_ms": round(lat_p99, 1),
                    "p999_ms": round(lat_p999, 1),
                },
                "avg_throughput_msgs_sec": avg_throughput,
                "peak_throughput_msgs_sec": peak_rate,
                "target_rate": target_rate,
                "bytes_sent": bytes_sent,
                "bytes_received": bytes_received,
            }

            if pname in RPC_PATTERNS:
                ps["senders_per_channel"] = pc.senders_per_channel
                ps["responders_per_channel"] = pc.responders_per_channel
                if psnap:
                    ps["responses_success"] = psnap["rpc_success"]
                    ps["responses_timeout"] = psnap["rpc_timeout"]
                    ps["responses_error"] = psnap["rpc_error"]
                    rpc_p50 = psnap["rpc_latency_p50_ms"]
                    rpc_p95 = psnap["rpc_latency_p95_ms"]
                    rpc_p99 = psnap["rpc_latency_p99_ms"]
                    rpc_p999 = psnap["rpc_latency_p999_ms"]
                else:
                    ps["responses_success"] = pg.total_rpc_success()
                    ps["responses_timeout"] = pg.total_rpc_timeout()
                    ps["responses_error"] = pg.total_rpc_error()
                    rpc_p50 = pg.pattern_rpc_latency_accum.percentile_ms(50.0)
                    rpc_p95 = pg.pattern_rpc_latency_accum.percentile_ms(95.0)
                    rpc_p99 = pg.pattern_rpc_latency_accum.percentile_ms(99.0)
                    rpc_p999 = pg.pattern_rpc_latency_accum.percentile_ms(99.9)
                ps["rpc_p50_ms"] = rpc_p50
                ps["rpc_p95_ms"] = rpc_p95
                ps["rpc_p99_ms"] = rpc_p99
                ps["rpc_p999_ms"] = rpc_p999
                ps["latency"] = {
                    "p50_ms": round(rpc_p50, 1),
                    "p95_ms": round(rpc_p95, 1),
                    "p99_ms": round(rpc_p99, 1),
                    "p999_ms": round(rpc_p999, 1),
                }
            else:
                ps["producers_per_channel"] = pc.producers_per_channel
                ps["consumers_per_channel"] = pc.consumers_per_channel

            if pname in PUBSUB_PATTERNS:
                ps["consumer_group"] = pc.consumer_group
                ps["num_consumers"] = pc.consumers_per_channel

            if pname == "events_store":
                ps["unconfirmed"] = psnap["unconfirmed"] if psnap else pg.total_unconfirmed()

            ps["_channel_workers"] = pg.channel_workers

            patterns[pname] = ps

        if ctx:
            for pname in ALL_PATTERN_NAMES:
                if pname not in ctx.enabled_patterns:
                    patterns[pname] = {"enabled": False}

        baseline = self._baseline_rss or max(self._peak_rss, 1.0)
        peak = self._peak_rss or self._get_rss_mb()
        growth = peak / baseline if baseline > 0 else 1.0

        # Count active asyncio tasks
        try:
            active_tasks = len(asyncio.all_tasks(self._loop) if self._loop else set())
        except RuntimeError:
            active_tasks = 0

        return {
            "sdk": "python",
            "version": self._sdk_version,
            "sdk_version": self._sdk_version,
            "mode": cfg.mode,
            "broker_address": cfg.broker.address,
            "started_at": self._run_started_at.isoformat() if self._run_started_at else "",
            "ended_at": self._run_ended_at.isoformat() if self._run_ended_at else "",
            "duration_seconds": elapsed,
            "status": status,
            "patterns": patterns,
            "resources": {
                "peak_rss_mb": peak,
                "baseline_rss_mb": baseline,
                "memory_growth_factor": growth,
                "peak_workers": self._peak_workers,
                "rss_mb": self._get_rss_mb(),
                "active_workers": active_tasks,
            },
        }

    def _build_run_response(self, state: RunState) -> dict[str, Any]:
        """Build response for GET /run endpoint."""
        cfg = self._run_cfg or self._startup_cfg
        ctx = self._run_ctx
        ref_time = self._test_started if self._test_started > 0 else self._run_started
        elapsed = time.monotonic() - ref_time if ref_time > 0 else 0
        duration_secs = cfg.duration_seconds
        remaining = max(0, duration_secs - elapsed) if duration_secs > 0 else 0

        result: dict[str, Any] = {
            "run_id": ctx.run_id if ctx else None,
            "state": state.value,
            "mode": cfg.mode,
            "started_at": self._run_started_at.isoformat() if self._run_started_at else "",
            "elapsed_seconds": round(elapsed, 1),
            "remaining_seconds": round(remaining, 1),
            "duration": cfg.duration,
            "warmup_active": self._warmup_active,
            "broker_address": cfg.broker.address,
        }

        if state in (RunState.STOPPED,) and self._run_ended_at:
            result["ended_at"] = self._run_ended_at.isoformat()

        patterns_resp: dict[str, Any] = {}
        for pname, pg in self._pattern_groups.items():
            pc = pg.pattern_config
            sent = pg.total_sent()
            received = pg.total_received()
            lost = pg.total_lost()
            loss_pct = (lost / sent * 100) if sent > 0 else 0.0

            p: dict[str, Any] = {
                "enabled": True,
                "state": self._pattern_states.get(pname, "unknown"),
                "channels": pc.channels,
                "sent": sent,
                "received": received,
                "lost": lost,
                "duplicated": pg.total_duplicated(),
                "corrupted": pg.total_corrupted(),
                "out_of_order": pg.total_out_of_order(),
                "errors": pg.total_errors(),
                "reconnections": pg.total_reconnections(),
                "loss_pct": round(loss_pct, 5),
                "target_rate": pc.rate * pc.channels,
                "actual_rate": round(pg.aggregate_sliding_rate(), 1),
                "peak_rate": round(pg.max_peak_rate(), 1),
                "bytes_sent": pg.total_bytes_sent(),
                "bytes_received": pg.total_bytes_received(),
                "latency": {
                    "p50_ms": round(pg.pattern_latency_accum.percentile_ms(50.0), 1),
                    "p95_ms": round(pg.pattern_latency_accum.percentile_ms(95.0), 1),
                    "p99_ms": round(pg.pattern_latency_accum.percentile_ms(99.0), 1),
                    "p999_ms": round(pg.pattern_latency_accum.percentile_ms(99.9), 1),
                },
            }

            if pname in RPC_PATTERNS:
                p["senders_per_channel"] = pc.senders_per_channel
                p["responders_per_channel"] = pc.responders_per_channel
                p["responses_success"] = pg.total_rpc_success()
                p["responses_timeout"] = pg.total_rpc_timeout()
                p["responses_error"] = pg.total_rpc_error()
                p["latency"] = {
                    "p50_ms": round(pg.pattern_rpc_latency_accum.percentile_ms(50.0), 1),
                    "p95_ms": round(pg.pattern_rpc_latency_accum.percentile_ms(95.0), 1),
                    "p99_ms": round(pg.pattern_rpc_latency_accum.percentile_ms(99.0), 1),
                    "p999_ms": round(pg.pattern_rpc_latency_accum.percentile_ms(99.9), 1),
                }
            else:
                p["producers_per_channel"] = pc.producers_per_channel
                p["consumers_per_channel"] = pc.consumers_per_channel
                if pname in PUBSUB_PATTERNS:
                    p["consumer_group"] = pc.consumer_group
                if pname == "events_store":
                    p["unconfirmed"] = pg.total_unconfirmed()

            patterns_resp[pname] = p

        if ctx:
            for pname in ALL_PATTERN_NAMES:
                if pname not in ctx.enabled_patterns:
                    patterns_resp[pname] = {"enabled": False}

        result["patterns"] = patterns_resp

        rss = self._get_rss_mb()
        result["resources"] = {
            "rss_mb": round(rss, 1),
            "baseline_rss_mb": round(self._baseline_rss, 1),
            "memory_growth_factor": round(rss / self._baseline_rss, 2) if self._baseline_rss > 0 else 1.0,
            "active_workers": len(asyncio.all_tasks(self._loop)) if self._loop else 0,
        }

        return result

    def _compute_totals(self) -> dict[str, int]:
        sent = received = lost = duplicated = corrupted = out_of_order = errors = reconnections = 0
        for pname, pg in self._pattern_groups.items():
            sent += pg.total_sent()
            if pname in RPC_PATTERNS:
                received += pg.total_rpc_success()
                lost += pg.total_rpc_timeout() + pg.total_rpc_error()
            else:
                received += pg.total_received()
                lost += pg.total_lost()
            duplicated += pg.total_duplicated()
            corrupted += pg.total_corrupted()
            out_of_order += pg.total_out_of_order()
            errors += pg.total_errors()
            reconnections += pg.total_reconnections()
        return {
            "sent": sent, "received": received, "lost": lost,
            "duplicated": duplicated, "corrupted": corrupted,
            "out_of_order": out_of_order, "errors": errors,
            "reconnections": reconnections,
        }

    def _build_report(self, summary: dict[str, Any], ctx: RunContext) -> dict[str, Any]:
        """Build final report with verdict for GET /run/report."""
        cfg = self._run_cfg or self._startup_cfg
        verdict = generate_verdict(
            summary, cfg.thresholds,
            enabled_patterns=ctx.enabled_patterns,
            pattern_thresholds=ctx.pattern_thresholds,
            all_patterns_enabled=ctx.all_patterns_enabled,
        )
        return self._build_report_from_verdict(summary, verdict, ctx)

    def _build_report_from_verdict(
        self, summary: dict[str, Any], verdict: dict[str, Any], ctx: RunContext,
    ) -> dict[str, Any]:
        cfg = self._run_cfg or self._startup_cfg
        ref_time = self._test_started if self._test_started > 0 else self._run_started
        end_time = self._producers_stopped if self._producers_stopped > 0 else time.monotonic()
        elapsed = end_time - ref_time if ref_time > 0 else 0

        patterns_report: dict[str, Any] = {}
        for pname, pg in self._pattern_groups.items():
            pc = pg.pattern_config

            psnap = self._producer_stop_snapshot.get(pname) if self._producer_stop_snapshot else None

            if psnap:
                sent = psnap["sent"]
                received = psnap["received"]
                lost = psnap["lost"]
                duplicated = psnap["duplicated"]
                corrupted = psnap["corrupted"]
                out_of_order = psnap["out_of_order"]
                errors = psnap["errors"]
                reconnections = psnap["reconnections"]
                bytes_sent = psnap["bytes_sent"]
                bytes_received = psnap["bytes_received"]
                downtime_seconds = psnap["downtime_seconds"]
                peak_rate = psnap["peak_rate"]
                lat_p50 = psnap["latency_p50_ms"]
                lat_p95 = psnap["latency_p95_ms"]
                lat_p99 = psnap["latency_p99_ms"]
                lat_p999 = psnap["latency_p999_ms"]
            else:
                sent = pg.total_sent()
                received = pg.total_received()
                lost = pg.total_lost()
                duplicated = pg.total_duplicated()
                corrupted = pg.total_corrupted()
                out_of_order = pg.total_out_of_order()
                errors = pg.total_errors()
                reconnections = pg.total_reconnections()
                bytes_sent = pg.total_bytes_sent()
                bytes_received = pg.total_bytes_received()
                downtime_seconds = pg.max_downtime_seconds()
                peak_rate = pg.max_peak_rate()
                lat_p50 = pg.pattern_latency_accum.percentile_ms(50.0)
                lat_p95 = pg.pattern_latency_accum.percentile_ms(95.0)
                lat_p99 = pg.pattern_latency_accum.percentile_ms(99.0)
                lat_p999 = pg.pattern_latency_accum.percentile_ms(99.9)

            loss_pct = (lost / sent * 100) if sent > 0 else 0.0
            avg_rate = sent / elapsed if elapsed > 0 else 0.0

            pr: dict[str, Any] = {
                "enabled": True,
                "channels": pc.channels,
                "sent": sent,
                "received": received,
                "lost": lost,
                "duplicated": duplicated,
                "corrupted": corrupted,
                "out_of_order": out_of_order,
                "loss_pct": round(loss_pct, 5),
                "errors": errors,
                "reconnections": reconnections,
                "latency": {
                    "p50_ms": round(lat_p50, 1),
                    "p95_ms": round(lat_p95, 1),
                    "p99_ms": round(lat_p99, 1),
                    "p999_ms": round(lat_p999, 1),
                },
                "avg_rate": round(avg_rate, 1),
                "avg_throughput_msgs_sec": round(avg_rate, 1),
                "peak_rate": round(peak_rate, 1),
                "peak_throughput_msgs_sec": round(peak_rate, 1),
                "target_rate": pc.rate * pc.channels,
                "bytes_sent": bytes_sent,
                "bytes_received": bytes_received,
                "downtime_seconds": downtime_seconds,
            }

            if pname in RPC_PATTERNS:
                pr["senders_per_channel"] = pc.senders_per_channel
                pr["responders_per_channel"] = pc.responders_per_channel
                if psnap:
                    pr["responses_success"] = psnap["rpc_success"]
                    pr["responses_timeout"] = psnap["rpc_timeout"]
                    pr["responses_error"] = psnap["rpc_error"]
                    rpc_p50 = psnap["rpc_latency_p50_ms"]
                    rpc_p95 = psnap["rpc_latency_p95_ms"]
                    rpc_p99 = psnap["rpc_latency_p99_ms"]
                    rpc_p999 = psnap["rpc_latency_p999_ms"]
                else:
                    pr["responses_success"] = pg.total_rpc_success()
                    pr["responses_timeout"] = pg.total_rpc_timeout()
                    pr["responses_error"] = pg.total_rpc_error()
                    rpc_p50 = pg.pattern_rpc_latency_accum.percentile_ms(50.0)
                    rpc_p95 = pg.pattern_rpc_latency_accum.percentile_ms(95.0)
                    rpc_p99 = pg.pattern_rpc_latency_accum.percentile_ms(99.0)
                    rpc_p999 = pg.pattern_rpc_latency_accum.percentile_ms(99.9)
                pr["latency"] = {
                    "p50_ms": round(rpc_p50, 1),
                    "p95_ms": round(rpc_p95, 1),
                    "p99_ms": round(rpc_p99, 1),
                    "p999_ms": round(rpc_p999, 1),
                }
            else:
                pr["producers_per_channel"] = pc.producers_per_channel
                pr["consumers_per_channel"] = pc.consumers_per_channel
                if pname in PUBSUB_PATTERNS:
                    pr["consumer_group"] = pc.consumer_group
                if pname == "events_store":
                    pr["unconfirmed"] = pg.total_unconfirmed()

            patterns_report[pname] = pr

        for pname in ALL_PATTERN_NAMES:
            if pname not in ctx.enabled_patterns:
                patterns_report[pname] = {"enabled": False}

        baseline = self._baseline_rss or max(self._peak_rss, 1.0)
        peak = self._peak_rss or self._get_rss_mb()
        growth = peak / baseline if baseline > 0 else 1.0

        return {
            "run_id": ctx.run_id,
            "sdk": "python",
            "sdk_version": self._sdk_version,
            "mode": cfg.mode,
            "broker_address": cfg.broker.address,
            "started_at": self._run_started_at.isoformat() if self._run_started_at else "",
            "ended_at": self._run_ended_at.isoformat() if self._run_ended_at else "",
            "duration_seconds": round(elapsed, 1),
            "all_patterns_enabled": ctx.all_patterns_enabled,
            "patterns": patterns_report,
            "resources": {
                "peak_rss_mb": round(peak, 1),
                "baseline_rss_mb": round(baseline, 1),
                "memory_growth_factor": round(growth, 2),
                "peak_workers": self._peak_workers,
            },
            "verdict": verdict,
        }

    def _print_banner(self, cfg: Config) -> None:
        ctx = self._run_ctx
        print("=" * 67)
        print("  KUBEMQ BURN-IN TEST -- Python SDK v2 (async)")
        print("=" * 67)
        print(f"  Mode:     {cfg.mode}")
        print(f"  Broker:   {cfg.broker.address}")
        print(f"  Duration: {cfg.duration}")
        print(f"  Run ID:   {cfg.run_id}")
        print(f"  Patterns:")
        for pname in ALL_PATTERN_NAMES:
            pc = cfg.patterns.get(pname)
            if pc and pc.enabled:
                if pname in RPC_PATTERNS:
                    print(f"    {pname}: {pc.channels}ch x rate={pc.rate} "
                          f"senders={pc.senders_per_channel} responders={pc.responders_per_channel}")
                else:
                    print(f"    {pname}: {pc.channels}ch x rate={pc.rate} "
                          f"producers={pc.producers_per_channel} consumers={pc.consumers_per_channel}"
                          + (f" group={pc.consumer_group}" if pname in PUBSUB_PATTERNS else ""))
            else:
                print(f"    {pname}: disabled")
        if ctx and not ctx.all_patterns_enabled:
            disabled = set(ALL_PATTERN_NAMES) - ctx.enabled_patterns
            print(f"  Disabled: {', '.join(sorted(disabled))}")
        print("=" * 67)
        print()

    @staticmethod
    def _get_rss_mb() -> float:
        try:
            return psutil.Process().memory_info().rss / (1024 * 1024)
        except Exception:
            return 0.0


def cleanup_only(cfg: Config) -> None:
    """Delete all burn-in channels and exit."""
    engine = Engine(cfg)
    engine._do_cleanup_sync()
