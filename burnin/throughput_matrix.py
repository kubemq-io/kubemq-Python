"""Run throughput matrix: 15 permutations all targeting ~3000 msg/s total.
Proves whether the event loop ceiling is consistent regardless of config.

Usage: uv run python throughput_matrix.py
"""
import json
import time
import urllib.request

BASE = "http://localhost:8889"
DURATION = "2m"
MEASURE_AT = 90  # seconds into run to measure


def api(method, path, body=None):
    data = json.dumps(body).encode() if body else None
    headers = {"Content-Type": "application/json"} if data else {}
    req = urllib.request.Request(f"{BASE}{path}", data=data, headers=headers, method=method)
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def wait_state(target, timeout=180):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        st = api("GET", "/run/status").get("state", "?")
        if st == target:
            return True
        if st == "error":
            print(f"  ERROR: {api('GET', '/run/status').get('error', '')}")
            return False
        time.sleep(3)
    return False


def make_config(patterns_cfg):
    """Build run config from pattern dict: {name: (channels, rate)}"""
    base = {
        "version": "2", "mode": "soak", "duration": DURATION,
        "starting_timeout_seconds": 180,
        "warmup": {"warmup_duration": "10s", "max_parallel_channels": 20, "timeout_per_channel_ms": 10000},
        "patterns": {},
        "queue": {"poll_max_messages": 10, "poll_wait_timeout_seconds": 5, "auto_ack": False, "max_depth": 1000000},
        "rpc": {"timeout_ms": 5000},
        "message": {"size_mode": "fixed", "size_bytes": 1024, "reorder_window": 10000},
        "thresholds": {"max_duplication_pct": 1, "max_error_rate_pct": 5, "max_memory_growth_factor": 5,
                       "max_downtime_pct": 50, "min_throughput_pct": 10, "max_duration": "168h"},
        "forced_disconnect": {"interval": "0", "duration": "5s"},
        "shutdown": {"drain_timeout_seconds": 5, "cleanup_channels": False},
        "metrics": {"report_interval": "30s"},
    }
    all_patterns = ["events", "events_store", "queue_stream", "queue_simple", "commands", "queries"]
    for pn in all_patterns:
        if pn in patterns_cfg:
            ch, rate = patterns_cfg[pn]
            p = {"enabled": True, "channels": ch, "rate": rate}
            if pn in ("commands", "queries"):
                p["senders_per_channel"] = 1
                p["responders_per_channel"] = 1
            else:
                p["producers_per_channel"] = 1
                p["consumers_per_channel"] = 1
                if pn in ("events", "events_store"):
                    p["consumer_group"] = False
        else:
            p = {"enabled": False}
        base["patterns"][pn] = p
    return base


TESTS = [
    ("1 pattern, 1ch×3000/s (events)",
     {"events": (1, 3000)}),
    ("1 pattern, 10ch×300/s (events)",
     {"events": (10, 300)}),
    ("1 pattern, 100ch×30/s (events)",
     {"events": (100, 30)}),
    ("2 patterns, 1ch×1500/s (ev+es)",
     {"events": (1, 1500), "events_store": (1, 1500)}),
    ("2 patterns, 10ch×150/s (ev+es)",
     {"events": (10, 150), "events_store": (10, 150)}),
    ("3 patterns, 10ch×100/s (ev+es+qs)",
     {"events": (10, 100), "events_store": (10, 100), "queue_stream": (10, 100)}),
    ("6 patterns, 1ch×500/s each",
     {"events": (1, 500), "events_store": (1, 500), "queue_stream": (1, 500),
      "queue_simple": (1, 500), "commands": (1, 500), "queries": (1, 500)}),
    ("6 patterns, 5ch×100/s each",
     {"events": (5, 100), "events_store": (5, 100), "queue_stream": (5, 100),
      "queue_simple": (5, 100), "commands": (5, 100), "queries": (5, 100)}),
    ("6 patterns, 10ch×50/s each",
     {"events": (10, 50), "events_store": (10, 50), "queue_stream": (10, 50),
      "queue_simple": (10, 50), "commands": (10, 50), "queries": (10, 50)}),
    ("6 patterns, 50ch×10/s each",
     {"events": (50, 10), "events_store": (50, 10), "queue_stream": (50, 10),
      "queue_simple": (50, 10), "commands": (50, 10), "queries": (50, 10)}),
    ("6 patterns, 100ch×5/s each",
     {"events": (100, 5), "events_store": (100, 5), "queue_stream": (100, 5),
      "queue_simple": (100, 5), "commands": (100, 5), "queries": (100, 5)}),
    ("events only, 1000ch×3/s",
     {"events": (1000, 3)}),
    ("events_store only, 30ch×100/s",
     {"events_store": (30, 100)}),
    ("queues only, 10ch×150/s each",
     {"queue_stream": (10, 150), "queue_simple": (10, 150)}),
    ("RPC only, 10ch×150/s each",
     {"commands": (10, 150), "queries": (10, 150)}),
]


def run_test(idx, name, patterns_cfg):
    total_target = sum(ch * rate for ch, rate in patterns_cfg.values())
    total_ch = sum(ch for ch, _ in patterns_cfg.values())
    n_patterns = len(patterns_cfg)

    print(f"\n{'='*80}")
    print(f"TEST {idx+1:2d}/15: {name}")
    print(f"  Target: {total_target} msg/s | {total_ch} channels | {n_patterns} patterns")
    print(f"{'='*80}")

    cfg = make_config(patterns_cfg)
    resp = api("POST", "/run/start", cfg)
    if resp.get("status") != "starting":
        print(f"  FAILED TO START: {resp}")
        return None

    run_id = resp.get("run_id", "?")
    if not wait_state("running"):
        print(f"  FAILED TO REACH RUNNING")
        # Try to stop
        try:
            api("POST", "/run/stop")
            wait_state("stopped", timeout=30)
        except Exception:
            pass
        return None

    # Wait for measurement point
    print(f"  Running... measuring at {MEASURE_AT}s")
    time.sleep(MEASURE_AT)

    # Measure
    d = api("GET", "/run")
    pats = d.get("patterns", {})
    elapsed = d.get("elapsed_seconds", 1)
    t = api("GET", "/run/status").get("totals", {})

    total_sent = t.get("sent", 0)
    total_rate = total_sent / max(elapsed, 1)
    lost = t.get("lost", 0)
    errs = t.get("errors", 0)

    pattern_lines = []
    for pn, (ch, rate) in patterns_cfg.items():
        p = pats.get(pn, {})
        sent = p.get("sent", 0)
        calc = sent / max(elapsed, 1)
        tgt = p.get("target_rate", 0)
        pct = (calc / tgt * 100) if tgt > 0 else 0
        p99 = p.get("latency", {}).get("p99_ms", 0)
        pattern_lines.append(f"    {pn:16s}({ch:>4d}ch): {calc:>6.0f}/{tgt}/s ({pct:>3.0f}%) P99={p99:.1f}ms")

    print(f"  Result at {elapsed:.0f}s: {total_rate:.0f}/{total_target} msg/s ({total_rate/total_target*100:.0f}%) lost={lost} errs={errs}")
    for line in pattern_lines:
        print(line)

    # Stop
    api("POST", "/run/stop")
    wait_state("stopped", timeout=60)

    return {
        "name": name,
        "target": total_target,
        "actual": round(total_rate),
        "pct": round(total_rate / total_target * 100),
        "channels": total_ch,
        "patterns": n_patterns,
        "lost": lost,
        "errs": errs,
    }


def main():
    # Check app is ready
    try:
        api("GET", "/health")
    except Exception:
        print("App not running on port 8889")
        return

    results = []
    for i, (name, pcfg) in enumerate(TESTS):
        r = run_test(i, name, pcfg)
        if r:
            results.append(r)
        time.sleep(5)  # breathing room between tests

    # Summary
    print("\n" + "=" * 100)
    print("THROUGHPUT MATRIX SUMMARY")
    print("=" * 100)
    print(f"{'#':>3s}  {'Test':50s} {'Target':>7s} {'Actual':>7s} {'Pct':>5s} {'Ch':>5s} {'Pat':>4s} {'Lost':>5s} {'Errs':>5s}")
    print("-" * 100)
    for i, r in enumerate(results):
        print(f"{i+1:3d}  {r['name']:50s} {r['target']:>7d} {r['actual']:>7d} {r['pct']:>4d}% {r['channels']:>5d} {r['patterns']:>4d} {r['lost']:>5d} {r['errs']:>5d}")
    print("-" * 100)

    actuals = [r["actual"] for r in results]
    if actuals:
        avg = sum(actuals) / len(actuals)
        mn = min(actuals)
        mx = max(actuals)
        print(f"Actual throughput: avg={avg:.0f} min={mn} max={mx} range={mx-mn}")
        pcts = [r["pct"] for r in results]
        print(f"Target hit %: avg={sum(pcts)/len(pcts):.0f}% min={min(pcts)}% max={max(pcts)}%")


if __name__ == "__main__":
    main()
