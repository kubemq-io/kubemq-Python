"""CLI entry point: argument parsing, signal handling, exit codes.

Implements spec §2 boot sequence:
1. Load startup config (broker, CORS, recovery, logging)
2. Start HTTP server on BURNIN_METRICS_PORT
3. Pre-initialize Prometheus metrics
4. Enter idle state, wait for API commands or SIGTERM

Async version: main loop uses asyncio.run(). HTTP server stays in daemon thread.
Workers run as asyncio.Tasks on the event loop.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import sys
import time

from burnin import metrics_collector as mc
from burnin.config import load_config
from burnin.engine import Engine, cleanup_only
from burnin.http_server import BurninHTTPServer
from burnin.run_state import RunState


class _JsonFormatter(logging.Formatter):
    """Structured JSON log formatter."""

    def format(self, record: logging.LogRecord) -> str:
        obj = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%SZ"),
            "level": record.levelname.lower(),
            "msg": record.getMessage(),
        }
        return json.dumps(obj, default=str)


async def _run_profiler(engine, logger) -> None:
    """Background profiler: waits for running state, profiles 60s, dumps results."""
    try:
        import yappi
    except ImportError:
        logger.warning("yappi not installed — skipping profiler")
        return

    # Wait for run to be active
    from burnin.run_state import RunState
    for _ in range(60):
        if engine._state.state == RunState.RUNNING:
            break
        await asyncio.sleep(1)
    else:
        return

    await asyncio.sleep(15)  # let it stabilize
    logger.info("PROFILER: starting 60s wall-clock profile")
    yappi.set_clock_type("wall")
    yappi.start()
    await asyncio.sleep(60)
    yappi.stop()

    stats = yappi.get_func_stats()
    stats.sort("ttot", "desc")
    stats.print_all(out=open("/tmp/yappi_full.txt", "w"))

    lines = ["", "=" * 110, "YAPPI PROFILE: TOP 40 BY TOTAL WALL TIME", "=" * 110]
    for i, s in enumerate(stats):
        if i >= 40:
            break
        lines.append(f"  {s.ttot:>8.3f}s  {s.ncall:>10d} calls  {s.tavg*1000:>8.2f}ms/call  {s.full_name}")
    lines.append("")
    lines.append("=" * 110)
    lines.append("TOP 20 BY CALL COUNT")
    lines.append("=" * 110)
    stats.sort("ncall", "desc")
    for i, s in enumerate(stats):
        if i >= 20:
            break
        lines.append(f"  {s.ncall:>12d} calls  {s.ttot:>8.3f}s total  {s.tavg*1000:>6.3f}ms/call  {s.full_name}")
    lines.append(f"\nFull: /tmp/yappi_full.txt")

    for line in lines:
        logger.info(line)
    yappi.clear_stats()


async def async_main(cfg, logger) -> int:
    """Async entry point: runs the event loop with engine and HTTP server."""
    loop = asyncio.get_running_loop()

    # Create engine and set its loop reference
    engine = Engine(cfg)
    engine._loop = loop

    # Start HTTP server in daemon thread
    api_port = cfg.api.port
    srv = BurninHTTPServer(port=api_port, engine_api=engine)
    srv.start()
    logger.info("HTTP server started on port %d — app is idle, waiting for API commands", api_port)

    # Signal handling via asyncio
    exit_event = asyncio.Event()
    exit_code = 0

    def on_signal() -> None:
        nonlocal exit_code
        logger.info("received shutdown signal")

        state = engine._state.state
        if state in (RunState.STARTING, RunState.RUNNING):
            logger.info("stopping active run before exit")
            # Set stop event directly (we're on the event loop thread)
            if engine._stop_event is not None:
                engine._stop_event.set()
            # Also try state transition
            engine._state.try_stop()

        exit_event.set()

    # Register signal handlers
    try:
        loop.add_signal_handler(signal.SIGTERM, on_signal)
        loop.add_signal_handler(signal.SIGINT, on_signal)
    except NotImplementedError:
        # Windows: fall back to signal.signal
        signal.signal(signal.SIGTERM, lambda s, f: loop.call_soon_threadsafe(on_signal))
        signal.signal(signal.SIGINT, lambda s, f: loop.call_soon_threadsafe(on_signal))

    # Wait for shutdown signal
    await exit_event.wait()

    # Wait for active run to complete (if any)
    if engine._run_task is not None and not engine._run_task.done():
        logger.info("waiting for run to complete...")
        try:
            await asyncio.wait_for(engine._run_task, timeout=30)
        except asyncio.TimeoutError:
            logger.warning("run did not complete within 30s, forcing shutdown")
            engine._run_task.cancel()
            try:
                await engine._run_task
            except (asyncio.CancelledError, Exception):
                pass

    # Determine exit code from last report
    if engine._last_report:
        verdict = engine._last_report.get("verdict", {})
        result = verdict.get("result", "")
        if result == "FAILED":
            exit_code = 1
        else:
            exit_code = 0
        from burnin.report import print_console_report
        print_console_report(engine._last_report)
        if cfg.output.report_file:
            from burnin.report import write_json_report
            write_json_report(engine._last_report, cfg.output.report_file)

    # Stop HTTP server
    srv.stop()
    logger.info("burn-in app shutting down")
    return exit_code


def main() -> None:
    parser = argparse.ArgumentParser(description="KubeMQ Python SDK burn-in test")
    parser.add_argument("--config", "-c", default="", help="Path to YAML config file")
    parser.add_argument("--validate-config", action="store_true", help="Validate config and exit")
    parser.add_argument("--cleanup-only", action="store_true", help="Delete burn-in channels and exit")
    args = parser.parse_args()

    # Load startup config
    try:
        cfg = load_config(args.config)
    except Exception as e:
        print(f"ERROR: failed to load config: {e}", file=sys.stderr)
        sys.exit(2)

    # Setup logging
    level_map = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warn": logging.WARNING,
        "warning": logging.WARNING,
        "error": logging.ERROR,
    }
    log_level = level_map.get(cfg.logging.level, logging.INFO)

    handler = logging.StreamHandler()
    if cfg.logging.format == "json":
        handler.setFormatter(_JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-5s %(message)s", datefmt="%H:%M:%S"))

    logging.root.handlers.clear()
    logging.root.addHandler(handler)
    logging.root.setLevel(log_level)
    logger = logging.getLogger("burnin")

    # Validate startup config
    errors = cfg.validate()
    has_errors = False
    for e in errors:
        if e.startswith("WARNING"):
            logger.warning(e)
        else:
            logger.error("config error: %s", e)
            has_errors = True
    if has_errors:
        sys.exit(2)

    # --validate-config mode (no HTTP server)
    if args.validate_config:
        logger.info("config validation passed")
        logger.info("mode=%s duration=%s broker=%s run_id=%s",
                     cfg.mode, cfg.duration, cfg.broker.address, cfg.run_id)
        sys.exit(0)

    # --cleanup-only mode (no HTTP server)
    if args.cleanup_only:
        logger.info("running cleanup-only mode")
        try:
            cleanup_only(cfg)
            logger.info("cleanup complete")
        except Exception as e:
            logger.error("cleanup failed: %s", e)
            sys.exit(1)
        sys.exit(0)

    # --- API-controlled mode (spec §2 boot sequence) ---

    # Pre-initialize Prometheus metrics
    mc.pre_initialize()

    # Run async main loop
    exit_code = asyncio.run(async_main(cfg, logger))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
