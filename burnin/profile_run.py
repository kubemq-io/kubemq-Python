"""Profile a burnin run with yappi (async-aware CPU profiler).

Usage: uv run python profile_run.py
Starts burnin, runs for 2m with profiling, dumps top functions.
"""

import asyncio
import json
import time
import urllib.request

import yappi

BASE = "http://localhost:8889"


def api_post(path, body=None):
    data = json.dumps(body or {}).encode()
    req = urllib.request.Request(f"{BASE}{path}", data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def api_get(path):
    with urllib.request.urlopen(f"{BASE}{path}") as resp:
        return json.loads(resp.read())


def wait_for_state(target, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        st = api_get("/run/status").get("state", "?")
        if st == target:
            return True
        if st == "error":
            print(f"ERROR: {api_get('/run/status')}")
            return False
        time.sleep(2)
    return False


def main():
    # Wait for app ready
    for _ in range(10):
        try:
            api_get("/health")
            break
        except Exception:
            time.sleep(1)

    config = {
        "version": "2", "mode": "soak", "duration": "2m", "starting_timeout_seconds": 120,
        "warmup": {"warmup_duration": "10s", "max_parallel_channels": 10, "timeout_per_channel_ms": 10000},
        "patterns": {
            "events": {"enabled": True, "channels": 10, "rate": 200, "producers_per_channel": 1, "consumers_per_channel": 1, "consumer_group": False},
            "events_store": {"enabled": True, "channels": 10, "rate": 200, "producers_per_channel": 1, "consumers_per_channel": 1, "consumer_group": False},
            "queue_stream": {"enabled": True, "channels": 10, "rate": 100, "producers_per_channel": 1, "consumers_per_channel": 1},
            "queue_simple": {"enabled": True, "channels": 10, "rate": 100, "producers_per_channel": 1, "consumers_per_channel": 1},
            "commands": {"enabled": True, "channels": 10, "rate": 40, "senders_per_channel": 1, "responders_per_channel": 1},
            "queries": {"enabled": True, "channels": 10, "rate": 40, "senders_per_channel": 1, "responders_per_channel": 1},
        },
        "queue": {"poll_max_messages": 10, "poll_wait_timeout_seconds": 5, "auto_ack": False, "max_depth": 1000000},
        "rpc": {"timeout_ms": 5000},
        "message": {"size_mode": "fixed", "size_bytes": 1024, "reorder_window": 10000},
        "thresholds": {"max_duplication_pct": 0.1, "max_error_rate_pct": 1.0, "max_memory_growth_factor": 2.0,
                       "max_downtime_pct": 10, "min_throughput_pct": 50, "max_duration": "168h"},
        "forced_disconnect": {"interval": "0", "duration": "5s"},
        "shutdown": {"drain_timeout_seconds": 10, "cleanup_channels": True},
        "metrics": {"report_interval": "30s"},
    }

    print("Starting run...")
    resp = api_post("/run/start", config)
    print(f"  {resp}")

    print("Waiting for running state...")
    if not wait_for_state("running"):
        print("Failed to reach running state")
        return

    # Let it stabilize for 15s
    print("Stabilizing 15s...")
    time.sleep(15)

    # Start profiling
    print("=" * 60)
    print("PROFILING for 60s (yappi wall-clock mode)...")
    print("=" * 60)
    yappi.set_clock_type("wall")
    yappi.start()

    time.sleep(60)

    yappi.stop()
    print("Profiling complete. Results:\n")

    # Get function stats
    stats = yappi.get_func_stats()

    # Print top 40 by total time
    print("=" * 100)
    print("TOP 40 FUNCTIONS BY TOTAL TIME (wall-clock)")
    print("=" * 100)
    stats.sort("ttot", "desc")
    stats.print_all(out=open("/tmp/yappi_full.txt", "w"))
    for i, s in enumerate(stats):
        if i >= 40:
            break
        print(f"  {s.ttot:>8.3f}s  {s.ncall:>8d} calls  {s.tavg*1000:>8.2f}ms/call  {s.full_name}")

    print()
    print("=" * 100)
    print("TOP 30 FUNCTIONS BY CALL COUNT")
    print("=" * 100)
    stats.sort("ncall", "desc")
    for i, s in enumerate(stats):
        if i >= 30:
            break
        print(f"  {s.ncall:>10d} calls  {s.ttot:>8.3f}s total  {s.full_name}")

    print(f"\nFull stats written to /tmp/yappi_full.txt")

    # Stop run
    print("\nStopping run...")
    api_post("/run/stop")
    wait_for_state("stopped", timeout=60)

    # Final report
    report = api_get("/run/report")
    v = report.get("verdict", {})
    print(f"\nVerdict: {v.get('result')}")
    for pn in ["events", "events_store", "queue_stream", "queue_simple", "commands", "queries"]:
        p = report["patterns"].get(pn, {})
        if not p.get("enabled"):
            continue
        tgt = p.get("target_rate", 0)
        avg = p.get("avg_rate", 0) or p.get("avg_throughput_msgs_sec", 0)
        pct = (avg / tgt * 100) if tgt > 0 else 0
        print(f"  {pn:16s}: {avg:.0f}/{tgt}/s ({pct:.0f}%) lost={p.get('lost', 0)} errs={p.get('errors', 0)}")


if __name__ == "__main__":
    main()
