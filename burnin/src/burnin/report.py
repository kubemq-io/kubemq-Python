"""Report generation: per-pattern verdict checks, advisory, console + JSON output.

v2: Multi-channel aware. Per-channel fail-on-any checks for loss, broadcast,
duplication, corruption. Pattern-level latency from shared accumulator.
"""

from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger("burnin")

RESULT_PASSED = "PASSED"
RESULT_PASSED_WITH_WARNINGS = "PASSED_WITH_WARNINGS"
RESULT_FAILED = "FAILED"

PUBSUB_QUEUE_PATTERNS = {"events", "events_store", "queue_stream", "queue_simple"}
RPC_PATTERNS = {"commands", "queries"}
ALL_PATTERNS = PUBSUB_QUEUE_PATTERNS | RPC_PATTERNS


def generate_verdict(
    summary: dict[str, Any],
    thresholds: Any,
    *,
    enabled_patterns: set[str] | None = None,
    pattern_thresholds: dict[str, Any] | None = None,
    all_patterns_enabled: bool = True,
    from_startup_error: str = "",
) -> dict[str, Any]:
    """Generate spec-compliant verdict with per-pattern check keys.

    v2: Per-channel fail-on-any checks for message loss, broadcast,
    duplication, corruption. Pattern-level latency from shared accumulator.
    """
    patterns = summary.get("patterns", {})
    resources = summary.get("resources", {})
    mode = summary.get("mode", "soak")
    duration_secs = summary.get("duration_seconds", 0)

    if enabled_patterns is None:
        enabled_patterns = set(patterns.keys())

    checks: dict[str, dict[str, Any]] = {}
    all_non_advisory_passed = True
    any_advisory_failed = False
    warnings: list[str] = []

    # --- Startup check (error-from-startup only) ---
    if from_startup_error:
        checks["startup"] = {
            "passed": False,
            "threshold": "success",
            "actual": from_startup_error,
            "advisory": False,
        }
        return {
            "result": RESULT_FAILED,
            "warnings": [],
            "checks": checks,
        }

    # --- Per-pattern checks ---
    for pname in sorted(enabled_patterns):
        ps = patterns.get(pname, {})
        if not ps.get("enabled", True):
            continue

        sent = ps.get("sent", 0)
        received = ps.get("received", 0)
        lost = ps.get("lost", 0)
        duplicated = ps.get("duplicated", 0)
        errors = ps.get("errors", 0)
        channels = ps.get("channels", 1)

        # Per-pattern threshold values
        pt = None
        if pattern_thresholds and pname in pattern_thresholds:
            pt = pattern_thresholds[pname]

        # Get channel workers for per-channel checks
        channel_workers = ps.get("_channel_workers", [])

        # -- message_loss (pub/sub + queue only, not RPC) --
        if pname in PUBSUB_QUEUE_PATTERNS:
            if pt is not None:
                loss_thresh = pt.max_loss_pct
            elif pname == "events":
                loss_thresh = getattr(thresholds, "max_events_loss_pct", 5.0)
            else:
                loss_thresh = getattr(thresholds, "max_loss_pct", 0.0)

            # Per-channel fail-on-any check
            if channel_workers:
                worst_ch_idx = 0
                worst_loss_pct = 0.0
                worst_loss_detail = ""
                any_failed = False

                for w in channel_workers:
                    ch_sent = w.sent_count
                    ch_lost = w.tracker.total_lost()
                    ch_loss_pct = (ch_lost / ch_sent * 100) if ch_sent > 0 else 0.0
                    if ch_loss_pct > worst_loss_pct:
                        worst_loss_pct = ch_loss_pct
                        worst_ch_idx = w.channel_index
                        worst_loss_detail = f"ch_{worst_ch_idx:04d}: {ch_loss_pct:.5g}% ({ch_lost}/{ch_sent})"
                    if ch_loss_pct > loss_thresh:
                        any_failed = True

                passed = not any_failed
                actual = worst_loss_detail if worst_loss_detail else f"{worst_loss_pct:.5g}%"
            else:
                # Fallback: aggregate check
                loss_pct = ps.get("loss_pct", 0.0)
                passed = loss_pct <= loss_thresh
                actual = f"{loss_pct:.5g}%"

            checks[f"message_loss:{pname}"] = {
                "passed": passed,
                "threshold": f"{loss_thresh}%",
                "actual": actual,
                "advisory": False,
            }
            if not passed:
                all_non_advisory_passed = False

        # -- duplication / broadcast (pub/sub + queue only, not RPC) --
        if pname in PUBSUB_QUEUE_PATTERNS:
            is_event_pattern = pname in ("events", "events_store")
            consumer_group = ps.get("consumer_group", False)
            num_consumers = ps.get("consumers_per_channel", ps.get("num_consumers", 1))

            if is_event_pattern and not consumer_group and num_consumers > 1:
                # Broadcast mode: per-channel arithmetic check
                if channel_workers:
                    any_failed = False
                    worst_actual = ""
                    worst_deficit = 0  # track the largest deficit (expected - received)

                    for w in channel_workers:
                        ch_sent = w.sent_count
                        ch_received = w.received_count
                        expected = ch_sent * num_consumers
                        if ch_received != expected:
                            any_failed = True
                            deficit = expected - ch_received
                            if deficit > worst_deficit:
                                worst_deficit = deficit
                                worst_actual = f"ch_{w.channel_index:04d}: {ch_received}/{expected}"

                    if not worst_actual:
                        worst_actual = f"{received}"

                    broadcast_ok = not any_failed
                else:
                    expected_total = sent * num_consumers
                    broadcast_ok = (received == expected_total)
                    worst_actual = str(received)

                checks[f"broadcast:{pname}"] = {
                    "passed": broadcast_ok,
                    "threshold": f"sent x {num_consumers} per channel",
                    "actual": worst_actual,
                    "advisory": False,
                }
                if not broadcast_ok:
                    all_non_advisory_passed = False

            elif is_event_pattern and consumer_group:
                # Consumer group mode: strict 0% duplication per channel
                if channel_workers:
                    any_failed = False
                    worst_actual = "0.0%"

                    for w in channel_workers:
                        ch_sent = w.sent_count
                        ch_dup = w.tracker.total_duplicates()
                        ch_dup_pct = (ch_dup / ch_sent * 100) if ch_sent > 0 else 0.0
                        if ch_dup_pct > 0:
                            any_failed = True
                            worst_actual = f"ch_{w.channel_index:04d}: {ch_dup_pct:.5g}%"

                    passed = not any_failed
                else:
                    dup_pct = (duplicated / sent * 100) if sent > 0 else 0.0
                    passed = dup_pct == 0
                    worst_actual = f"{dup_pct:.5g}%"

                checks[f"duplication:{pname}"] = {
                    "passed": passed,
                    "threshold": "0.0%",
                    "actual": worst_actual,
                    "advisory": False,
                }
                if not passed:
                    all_non_advisory_passed = False
            else:
                # Queue patterns or single-consumer events: per-channel threshold check
                dup_thresh = thresholds.max_duplication_pct
                if channel_workers:
                    any_failed = False
                    worst_actual = "0.0%"

                    for w in channel_workers:
                        ch_sent = w.sent_count
                        ch_dup = w.tracker.total_duplicates()
                        ch_dup_pct = (ch_dup / ch_sent * 100) if ch_sent > 0 else 0.0
                        if ch_dup_pct > dup_thresh:
                            any_failed = True
                            worst_actual = f"ch_{w.channel_index:04d}: {ch_dup_pct:.5g}%"

                    passed = not any_failed
                else:
                    dup_pct = (duplicated / sent * 100) if sent > 0 else 0.0
                    passed = dup_pct <= dup_thresh
                    worst_actual = f"{dup_pct:.5g}%"

                checks[f"duplication:{pname}"] = {
                    "passed": passed,
                    "threshold": f"{dup_thresh}%",
                    "actual": worst_actual,
                    "advisory": False,
                }
                if not passed:
                    all_non_advisory_passed = False

        # -- p99_latency (all patterns) -- from pattern-level accumulator
        if pname in RPC_PATTERNS:
            p99 = ps.get("rpc_p99_ms", ps.get("latency_p99_ms", 0.0))
        else:
            p99 = ps.get("latency_p99_ms", 0.0)
        p99_thresh = pt.max_p99_latency_ms if pt else getattr(thresholds, "max_p99_latency_ms", 1000.0)
        passed = p99 <= p99_thresh
        checks[f"p99_latency:{pname}"] = {
            "passed": passed,
            "threshold": f"{p99_thresh:.0f}ms",
            "actual": f"{p99:.1f}ms",
            "advisory": False,
        }
        if not passed:
            all_non_advisory_passed = False

        # -- p999_latency (all patterns) --
        if pname in RPC_PATTERNS:
            p999 = ps.get("rpc_p999_ms", ps.get("latency_p999_ms", 0.0))
        else:
            p999 = ps.get("latency_p999_ms", 0.0)
        p999_thresh = pt.max_p999_latency_ms if pt else getattr(thresholds, "max_p999_latency_ms", 5000.0)
        passed = p999 <= p999_thresh
        checks[f"p999_latency:{pname}"] = {
            "passed": passed,
            "threshold": f"{p999_thresh:.0f}ms",
            "actual": f"{p999:.1f}ms",
            "advisory": False,
        }
        if not passed:
            all_non_advisory_passed = False

        # -- error_rate (all patterns) -- aggregated across channels
        if pname in RPC_PATTERNS:
            rpc_success = ps.get("responses_success", 0)
            total_ops = sent + rpc_success
        else:
            total_ops = sent + received
        err_pct = (errors / total_ops * 100) if total_ops > 0 else 0.0
        err_thresh = thresholds.max_error_rate_pct
        passed = err_pct <= err_thresh
        checks[f"error_rate:{pname}"] = {
            "passed": passed,
            "threshold": f"{err_thresh}%",
            "actual": f"{err_pct:.4f}%",
            "advisory": False,
        }
        if not passed:
            all_non_advisory_passed = False

    # --- Global checks ---

    # corruption (per-channel fail-on-any)
    total_corrupted = 0
    any_channel_corrupted = False
    for pname in enabled_patterns:
        ps = patterns.get(pname, {})
        channel_workers = ps.get("_channel_workers", [])
        if channel_workers:
            for w in channel_workers:
                if w.corrupted_count > 0:
                    any_channel_corrupted = True
                total_corrupted += w.corrupted_count
        else:
            total_corrupted += ps.get("corrupted", 0)
    passed = total_corrupted == 0 and not any_channel_corrupted
    checks["corruption"] = {
        "passed": passed,
        "threshold": "0",
        "actual": str(total_corrupted),
        "advisory": False,
    }
    if not passed:
        all_non_advisory_passed = False

    # throughput (soak mode only, global min across all patterns)
    if mode == "soak" and duration_secs > 0:
        min_tp_pct = 100.0
        for pname in enabled_patterns:
            ps = patterns.get(pname, {})
            target = ps.get("target_rate", 0)
            avg = ps.get("avg_throughput_msgs_sec", ps.get("avg_rate", 0.0))
            if target > 0:
                pct = (avg / target) * 100
                min_tp_pct = min(min_tp_pct, pct)
        tp_thresh = thresholds.min_throughput_pct
        passed = min_tp_pct >= tp_thresh
        checks["throughput"] = {
            "passed": passed,
            "threshold": f"{tp_thresh}%",
            "actual": f"{min_tp_pct:.1f}%",
            "advisory": False,
        }
        if not passed:
            all_non_advisory_passed = False
    else:
        checks["throughput"] = {
            "passed": True,
            "threshold": "N/A (benchmark mode)",
            "actual": "N/A",
            "advisory": False,
        }

    # memory_stability
    growth = resources.get("memory_growth_factor", 1.0)
    max_growth = thresholds.max_memory_growth_factor
    passed = growth <= max_growth
    advisory_mem = duration_secs < 300
    checks["memory_stability"] = {
        "passed": passed,
        "threshold": f"{max_growth}x",
        "actual": f"{growth:.2f}x",
        "advisory": advisory_mem,
    }
    if not passed:
        if advisory_mem:
            any_advisory_failed = True
        else:
            all_non_advisory_passed = False

    # memory_trend (advisory: 1.0 + (max_factor-1.0)*0.5)
    trend_thresh = 1.0 + (max_growth - 1.0) * 0.5
    trend_passed = growth <= trend_thresh
    checks["memory_trend"] = {
        "passed": trend_passed,
        "threshold": f"{trend_thresh:.1f}x",
        "actual": f"{growth:.2f}x",
        "advisory": True,
    }
    if not trend_passed:
        any_advisory_failed = True
        warnings.append(f"Memory growth trend: {growth:.2f}x (advisory threshold: {trend_thresh:.1f}x)")

    # downtime: max across all patterns (not sum)
    max_dt_pct = 0.0
    if duration_secs > 0:
        for pname in enabled_patterns:
            ps = patterns.get(pname, {})
            dt = ps.get("downtime_seconds", 0.0)
            pct = (dt / duration_secs) * 100
            max_dt_pct = max(max_dt_pct, pct)
    dt_thresh = thresholds.max_downtime_pct
    passed = max_dt_pct <= dt_thresh
    checks["downtime"] = {
        "passed": passed,
        "threshold": f"{dt_thresh}%",
        "actual": f"{max_dt_pct:.4f}%",
        "advisory": False,
    }
    if not passed:
        all_non_advisory_passed = False

    # --- Warnings ---
    if not all_patterns_enabled:
        warnings.append("Not all patterns enabled -- not valid for production certification")

    # --- Determine overall result ---
    if not all_non_advisory_passed:
        result = RESULT_FAILED
    elif any_advisory_failed:
        result = RESULT_PASSED_WITH_WARNINGS
    else:
        result = RESULT_PASSED

    return {
        "result": result,
        "passed": all_non_advisory_passed,
        "warnings": warnings,
        "checks": checks,
    }


def print_console_report(summary: dict[str, Any]) -> None:
    """Print formatted console report with v2 multi-channel info."""
    verdict = summary.get("verdict", {})
    patterns = summary.get("patterns", {})
    res = summary.get("resources", {})
    version = summary.get("version", summary.get("sdk_version", ""))

    dur = summary.get("duration_seconds", 0)
    h, m, s = int(dur) // 3600, (int(dur) % 3600) // 60, int(dur) % 60
    dur_str = f"{h}h {m}m {s}s" if h else f"{m}m {s}s"

    print("\n" + "=" * 72)
    print(f"  KUBEMQ BURN-IN TEST REPORT -- Python SDK v{version}")
    print("=" * 72)
    print(f"  Mode:     {summary.get('mode', 'N/A')}")
    print(f"  Broker:   {summary.get('broker_address', 'N/A')}")
    print(f"  Duration: {dur_str}")
    print(f"  Started:  {summary.get('started_at', 'N/A')}")
    print(f"  Ended:    {summary.get('ended_at', 'N/A')}")
    print("-" * 72)

    hdr = f"  {'PATTERN':<22}{'SENT':>10}{'RECV':>10}{'LOST':>6}{'DUP':>6}{'ERR':>6}{'P99(ms)':>9}{'P999(ms)':>10}"
    print(hdr)
    total_sent = total_recv = total_lost = total_dup = total_err = 0
    for name, ps in patterns.items():
        if not ps.get("enabled", True):
            continue
        sent = ps.get("sent", 0)
        recv = ps.get("received", ps.get("responses_success", 0))
        lost = ps.get("lost", 0)
        dup = ps.get("duplicated", 0)
        err = ps.get("errors", 0)
        total_sent += sent
        total_recv += recv
        total_lost += lost
        total_dup += dup
        total_err += err

        # v2: show channel count
        channels = ps.get("channels", 1)
        ch_label = f" ({channels}ch)" if channels > 1 else ""
        display_name = f"{name}{ch_label}"

        latency = ps.get("latency", {})
        if name in ("commands", "queries"):
            p99 = latency.get("p99_ms", ps.get("rpc_p99_ms", 0))
            p999 = latency.get("p999_ms", ps.get("rpc_p999_ms", 0))
        else:
            p99 = latency.get("p99_ms", ps.get("latency_p99_ms", 0))
            p999 = latency.get("p999_ms", ps.get("latency_p999_ms", 0))
        print(f"  {display_name:<22}{sent:>10}{recv:>10}{lost:>6}{dup:>6}{err:>6}{p99:>8.1f}{p999:>10.1f}")

    print("-" * 72)
    print(f"  {'TOTALS':<22}{total_sent:>10}{total_recv:>10}{total_lost:>6}{total_dup:>6}{total_err:>6}")
    print(f"  RESOURCES       RSS: {res.get('baseline_rss_mb', 0):.0f}MB -> "
          f"{res.get('peak_rss_mb', 0):.0f}MB "
          f"({res.get('memory_growth_factor', 0):.2f}x)  "
          f"Workers: {res.get('peak_workers', res.get('peak_workers', 0))}")
    print("-" * 72)

    result = verdict.get("result", "UNKNOWN")
    print(f"  VERDICT: {result}")
    warnings = verdict.get("warnings", [])
    for w in warnings:
        print(f"    WARNING: {w}")
    checks = verdict.get("checks", {})
    for name, check in checks.items():
        marker = "+" if check.get("passed") else "!"
        advisory = " (advisory)" if check.get("advisory") else ""
        print(f"    {marker} {name:<35s}{check.get('actual', 'N/A'):<18s}"
              f"(threshold: {check.get('threshold', 'N/A')}){advisory}")
    print("=" * 72 + "\n")


def write_json_report(summary: dict[str, Any], path: str) -> None:
    """Write summary as JSON file, stripping internal _channel_workers references."""
    # Deep copy and strip internal keys
    clean = _strip_internal_keys(summary)
    with open(path, "w") as f:
        json.dump(clean, f, indent=2, default=str)
    logger.info("report written to %s", path)


def _strip_internal_keys(obj: Any) -> Any:
    """Recursively strip keys starting with '_' from dicts."""
    if isinstance(obj, dict):
        return {k: _strip_internal_keys(v) for k, v in obj.items() if not k.startswith("_")}
    elif isinstance(obj, list):
        return [_strip_internal_keys(item) for item in obj]
    return obj
