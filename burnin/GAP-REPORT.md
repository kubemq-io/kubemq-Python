# Python SDK Burn-In — REST API Spec Implementation Gap Report

> **Spec Version**: 2.2 (2026-03-17)
> **SDK**: Python
> **Date**: 2026-03-17

---

## Summary Statistics

| Metric | Count |
|--------|-------|
| Total Items | 90 |
| Done | 90 |
| Partial / In Progress | 0 |
| Remaining | 0 |
| % Complete | 100% |

---

## 13.1 Boot & Lifecycle

| # | Requirement | Spec Ref | Python | Notes |
|---|------------|----------|:------:|-------|
| L1 | Boot into `idle` state (no auto-start) | §2 | [x] | `main.py` starts HTTP server then enters idle state via `exit_event.wait()` |
| L2 | HTTP server starts on boot before broker connection | §2 | [x] | HTTP server created and started before Engine connects to broker |
| L3 | `/health` returns `{"status":"alive"}` with 200 from boot | §3.1 | [x] | Always returns 200 with `{"status": "alive"}` |
| L4 | `/ready` per-state response: 200 for idle/running/stopped/error, 503 for starting/stopping | §3.1 | [x] | State-aware response in `http_server.py` |
| L5 | Pre-initialize all Prometheus metrics to 0 on startup | §2, §8.3 | [x] | `mc.pre_initialize()` called before HTTP server starts |
| L6 | Run state machine: `idle`→`starting`→`running`→`stopping`→`stopped`/`error` | §4.1 | [x] | `StateMachine` in `run_state.py` with all transitions |
| L7 | Atomic state transitions (Python: Lock) | §4.2 | [x] | `threading.Lock` in `StateMachine`, compare-and-swap via `try_start()`/`try_stop()` |
| L8 | `starting_timeout_seconds` (default 60s) — timer starts at `starting` transition, exceeds → `error` | §4.1, §4.2 | [x] | Watchdog thread `_starting_timeout_watchdog()` in engine |
| L9 | Per-pattern states: `starting`, `running`, `recovering`, `error`, `stopped` | §4.3 | [x] | `PatternState` enum defined; all states wired. `recovering` set on disconnect/reconnect events via worker callback + engine handler. |
| L10 | Stop during `starting` — cancel startup, cleanup partial channels, minimal report, → `stopped` | §4.4 | [x] | `_handle_stop_during_starting()` cleans up and generates minimal report |
| L11 | SIGTERM/SIGINT: stop active run gracefully, generate report, cleanup, exit | §9 | [x] | Signal handler in `main.py` calls `handle_run_stop()`, waits, prints report |
| L12 | Exit codes: 0=PASSED/PASSED_WITH_WARNINGS, 1=FAILED, 2=config error, 0 if idle | §9 | [x] | Exit code logic in signal handler based on verdict result |

## 13.2 Endpoints

| # | Endpoint | Spec Ref | Python | Notes |
|---|----------|----------|:------:|-------|
| E1 | `GET /info` — sdk, version, runtime, os, arch, cpus, memory, pid, uptime, state, broker_address | §5.1 | [x] | Full implementation in `Engine.get_info()` |
| E2 | `GET /broker/status` — gRPC Ping() with 3s timeout, connected/error/latency | §5.2 | [x] | Explicit 3s timeout via `concurrent.futures.ThreadPoolExecutor` wrapping the ping call |
| E3 | `POST /run/start` — full config body, validate all fields, return 202 with run_id | §5.3 | [x] | `handle_run_start()` validates, translates, starts background thread |
| E4 | `POST /run/stop` — graceful stop, return 202. 409 for wrong states | §5.4 | [x] | `handle_run_stop()` with state-aware 409 responses |
| E5 | `GET /run` — full state with pattern+worker metrics | §5.5 | [x] | `get_run()` returns hierarchical metrics with producers/consumers/senders/responders |
| E6 | `GET /run/status` — lightweight: state, totals, pattern_states | §5.6 | [x] | `get_run_status()` with totals aggregation |
| E7 | `GET /run/config` — resolved config with channel names, 404 when no run | §5.7 | [x] | `get_run_config()` returns full resolved config |
| E8 | `GET /run/report` — final report with verdict checks map, 404 when no completed run | §5.8 | [x] | `get_run_report()` returns last completed report |
| E9 | `POST /cleanup` — delete all `python_burnin_*` channels, 409 during active run | §5.9 | [x] | `handle_cleanup()` with state guards |
| E10 | Legacy alias: `/status` → `/run/status` with deprecation warning | §3 | [x] | Logged once per boot via `_log_deprecation()` |
| E11 | Legacy alias: `/summary` → `/run/report` with deprecation warning | §3 | [x] | Logged once per boot via `_log_deprecation()` |

## 13.3 HTTP & Error Handling

| # | Requirement | Spec Ref | Python | Notes |
|---|------------|----------|:------:|-------|
| H1 | CORS headers on all responses with configurable `BURNIN_CORS_ORIGINS` | §7 | [x] | `_cors_headers()` on every response, `CORSConfig` in config |
| H2 | `OPTIONS` preflight → 204 No Content with CORS headers | §7 | [x] | `do_OPTIONS()` handler |
| H3 | Error response format: `{"message": "...", "errors": [...]}` | §6 | [x] | Validation errors include `errors` array |
| H4 | `400` for invalid JSON body with parse error in message | §5.3.4, §6 | [x] | `_read_json_body()` returns 400 on parse error |
| H5 | `400` for validation errors — collect ALL errors, return together | §5.3.4 | [x] | `translate_api_config()` collects all errors |
| H6 | `409` for state conflicts — include current `run_id` and `state` | §5.3, §5.4, §5.9 | [x] | All 409 responses include run_id and state |
| H7 | `Content-Type: application/json` header on all JSON responses | §3 | [x] | Set in `_json_ok()` |
| H8 | Silently ignore unknown JSON fields in POST body | §1, §5.3.4 | [x] | `translate_api_config()` uses `dict.get()` — unknown fields ignored naturally |

## 13.4 Config Handling

| # | Requirement | Spec Ref | Python | Notes |
|---|------------|----------|:------:|-------|
| C1 | Parse nested per-pattern API config schema — no `broker.address` in body | §5.3.1 | [x] | `translate_api_config()` uses startup broker |
| C2 | Translate API nested config → internal flat config per normative mapping | §5.3.3 | [x] | Full mapping in `translate_api_config()` |
| C3 | Per-pattern `enabled` flag — skip disabled patterns, `{"enabled":false}` in responses | §5.3.2, §5.5 | [x] | Workers only created for enabled patterns |
| C4 | Per-pattern threshold overrides: loss_pct, p99, p999 override global defaults | §5.3.3 | [x] | `PatternThresholds` per pattern passed to verdict |
| C5 | Default rate values when omitted: events=100, events_store=100, queues=50, rpc=20 | §5.3.2 | [x] | `_DEFAULT_RATES` dict |
| C6 | Default loss thresholds: events=5.0%, events_store/queues=0.0% | §5.3.2 | [x] | `_DEFAULT_LOSS_PCT` dict |
| C7 | `warmup_duration` mode-dependent default (60s benchmark, 0s soak) | §5.3.2 | [x] | Applied in `translate_api_config()` |
| C8 | `run_id` auto-generation (8-char UUID prefix) | §5.3.2 | [x] | `secrets.token_hex(4)` = 8 hex chars |
| C9 | Full validation: mode, duration, rate>0, concurrency>=1, pct 0-100, size>=64, reorder>=100 | §5.3.4 | [x] | All validation rules in `translate_api_config()` |
| C10 | `visibility_seconds` omitted from API queue config — silently ignore in YAML | §5.3.2, §2.1 | [x] | Not in API translation; YAML still loads it (harmless) |
| C11 | Java: `visibility_seconds` as Java-specific extension | §5.3.2 | N/A | |
| C12 | `poll_wait_timeout_seconds` → ms for Queue Stream, seconds for Queue Simple | §5.3.2 | [x] | Python SDK gRPC proto uses `WaitTimeSeconds` (seconds) for both queue types — no ms conversion needed. SDK handles correctly. |
| C13 | `max_duration` safety cap in thresholds (default 168h) | §5.3.2 | [x] | Used as effective timeout in `_execute_run()` |

## 13.5 Run Data & Metrics (REST API)

| # | Requirement | Spec Ref | Python | Notes |
|---|------------|----------|:------:|-------|
| M1 | Per-run REST counters (reset on new run) | §8.2 | [x] | All worker counters reset on new `start_run()`, Prometheus counters untouched |
| M2 | Pattern-level aggregates: sent, received, lost, duplicated, corrupted, out_of_order, errors, reconnections, loss_pct, latency{} | §5.5 | [x] | Full pattern metrics in `_build_run_response()` |
| M3 | Per-producer metrics: id, sent, errors, actual_rate, latency{} | §5.5 | [x] | `_build_producer_metrics()` from `_producer_stats` |
| M4 | Per-consumer metrics: id, received, lost, duplicated, corrupted, errors, latency{} | §5.5 | [x] | `_build_consumer_metrics()` from `_consumer_stats` |
| M5 | Per-sender RPC metrics: id, sent, responses_success/timeout/error, actual_rate, latency{} | §5.5 | [x] | `_build_sender_metrics()` from `_sender_stats` |
| M6 | Per-responder RPC metrics: id, responded, errors | §5.5 | [x] | `inc_responder_responded/error` wired into commands and queries worker responder callbacks |
| M7 | `actual_rate` = 30-second sliding average | §5.5.1 | [x] | `SlidingRateTracker` (30×1s buckets) in `peak_rate.py`, wired into BaseWorker + engine metrics |
| M8 | `peak_rate` = highest 10-second window rate | §5.5.1 | [x] | `PeakRateTracker` with 10x1s buckets |
| M9 | `bytes_sent` / `bytes_received` per pattern — message body bytes only | §5.5.1 | [x] | Tracked in `BaseWorker._bytes_sent/received` |
| M10 | `unconfirmed` count: Events Store only | §5.5.1 | [x] | `_unconfirmed` incremented in EventsStoreWorker when `result.sent` is False |
| M11 | Live resource metrics: rss_mb, baseline_rss_mb, memory_growth_factor, active_workers | §5.5 | [x] | In `_build_run_response()` resources block |
| M12 | Totals aggregation: RPC success→received, timeout+error→lost | §5.6 | [x] | `_compute_totals()` method |
| M13 | `out_of_order` included in `/run/status` totals | §5.6 | [x] | Included in `_compute_totals()` |
| M14 | `resources` naming: live=rss_mb/active_workers, report=peak_rss_mb/peak_workers | §5.5 | [x] | Different naming in `_build_run_response()` vs `_build_report_from_verdict()` |

## 13.6 Report & Verdict

| # | Requirement | Spec Ref | Python | Notes |
|---|------------|----------|:------:|-------|
| R1 | Report available via `GET /run/report` after stopped/error, until next run starts | §5.8 | [x] | `_last_report` preserved, cleared on new run |
| R2 | Error-from-startup report: verdict=FAILED, `startup` check | §5.8.3 | [x] | `from_startup_error` parameter in `generate_verdict()` |
| R3 | `all_patterns_enabled` boolean flag in report | §5.8.2 | [x] | Set from `RunContext.all_patterns_enabled` |
| R4 | `warnings` array: "Not all patterns enabled" | §5.8.1 | [x] | Added in verdict generation |
| R5 | `peak_rate` per pattern in report | §5.8.2 | [x] | From `PeakRateTracker.peak()` |
| R6 | `avg_rate` per pattern in report (total_sent/elapsed) | §5.8.2 | [x] | Computed in `_build_report_from_verdict()` |
| R7 | Worker-level breakdown in report: producers[], consumers[], senders[], responders[] | §5.8.2 | [x] | Included in report patterns |
| R8 | Verdict checks as map: keys `"name:pattern"` for per-pattern | §5.8.1 | [x] | e.g. `"message_loss:events"`, `"p99_latency:commands"` |
| R9 | Check result fields: `passed`, `threshold`, `actual`, `advisory` | §5.8.1 | [x] | All fields present in every check |
| R10 | Normative check names: message_loss, duplication, corruption, p99_latency, p999_latency, throughput, error_rate, memory_stability, memory_trend, downtime, startup | §5.8.1 | [x] | All 11 check names implemented |
| R11 | `duplication` checks: per enabled pub/sub+queue only (not RPC) | §5.8.1 | [x] | Only for `PUBSUB_QUEUE_PATTERNS` |
| R12 | `error_rate` checks: per enabled pattern (errors/(sent+received)*100) | §5.8.1 | [x] | RPC uses `errors/(sent+responses_success)*100` |
| R13 | `throughput` check: global min across patterns, uses avg_rate. Soak only. | §5.8.1 | [x] | Single global check |
| R14 | `memory_trend` advisory: `1.0 + (max_factor-1.0)*0.5`, advisory=true | §5.8.1 | [x] | Correct formula applied |
| R15 | `PASSED_WITH_WARNINGS`: all non-advisory pass + any advisory fail | §5.8.1 | [x] | Logic in `generate_verdict()` |
| R16 | Memory baseline: 5min after running start, advisory for <5min runs | §5.8.1 | [x] | `MEMORY_BASELINE_SECONDS=300`, advisory flag on `memory_stability` for short runs |
| R17 | Per-pattern loss checks using pattern-specific `max_loss_pct` | §5.8, §5.3.3 | [x] | Per-pattern thresholds from `PatternThresholds` |
| R18 | Per-pattern latency checks (p99, p999) using pattern-specific thresholds | §5.8, §5.3.3 | [x] | Per-pattern p99/p999 thresholds |
| R19 | Verdict result: PASSED / PASSED_WITH_WARNINGS / FAILED | §5.8.1 | [x] | All three values supported |

## 13.7 Startup Config & CLI

| # | Requirement | Spec Ref | Python | Notes |
|---|------------|----------|:------:|-------|
| S1 | `BURNIN_METRICS_PORT` / `metrics.port` (default 8888) | §2.1 | [x] | In startup config and env overrides |
| S2 | `BURNIN_LOG_FORMAT` / `logging.format` | §2.1 | [x] | text/json support |
| S3 | `BURNIN_LOG_LEVEL` / `logging.level` | §2.1 | [x] | debug/info/warn/error |
| S4 | `BURNIN_CORS_ORIGINS` / `cors.origins` (default `*`) | §2.1, §7 | [x] | `CORSConfig` added to Config, env override registered |
| S5 | `BURNIN_BROKER_ADDRESS` / `broker.address` (startup-only) | §2.1 | [x] | Not in API body, only startup config |
| S6 | `BURNIN_CLIENT_ID_PREFIX` / `broker.client_id_prefix` | §2.1 | [x] | Default `burnin-python` |
| S7 | `BURNIN_RECONNECT_INTERVAL` with 0-25% jitter | §2.1 | [x] | Jitter via SDK's `JitterType.FULL` |
| S8 | `BURNIN_RECONNECT_MAX_INTERVAL` (30s) | §2.1 | [x] | In recovery config |
| S9 | `BURNIN_RECONNECT_MULTIPLIER` (2.0) | §2.1 | [x] | In recovery config |
| S10 | `BURNIN_REPORT_OUTPUT_FILE` (SIGTERM flow) | §2.1 | [x] | Written in SIGTERM handler |
| S11 | `BURNIN_SDK_VERSION` / auto-detect | §2.1 | [x] | Auto-detect from `kubemq.__version__` |
| S12 | Java: `BURNIN_QUEUE_VISIBILITY_SECONDS` | §2.1 | N/A | |
| S13 | `--cleanup-only` CLI mode | §2.2 | [x] | Preserved, no HTTP server started |
| S14 | `--validate-config` CLI mode | §2.2 | [x] | Preserved, no HTTP server started |

---

## Key Deviations / Partial Items

All items resolved. No remaining deviations.

---

## Architecture Changes Made

1. **New file `run_state.py`**: `RunState` and `PatternState` enums, `StateMachine` class with `threading.Lock` for atomic transitions.
2. **Config additions**: `CORSConfig`, `PatternThresholds`, `RunContext` dataclasses; `translate_api_config()` function for API body → internal config translation.
3. **HTTP server rewrite**: `ThreadingHTTPServer` for concurrent request handling. All 12 endpoints implemented including POST handlers, CORS, OPTIONS preflight.
4. **Engine refactored**: Boots into idle, supports `handle_run_start()`/`handle_run_stop()` API. Runs engine logic in background thread. Starting timeout watchdog. Report preservation between runs.
5. **Main.py simplified**: HTTP server starts first, main thread blocks on signal, SIGTERM gracefully stops active run.
6. **Report rewrite**: Per-pattern verdict checks with `"name:pattern"` keys, advisory checks, `PASSED_WITH_WARNINGS` logic, `startup` error check.
7. **Worker tracking**: Per-producer/consumer/sender/responder stat dictionaries, `bytes_sent`/`bytes_received` counters on BaseWorker.
8. **Prometheus pre-init**: `mc.pre_initialize()` zeroes all metric series at boot.
9. **`SlidingRateTracker`**: 30×1s ring buffer in `peak_rate.py` for spec-compliant `actual_rate` (M7). Wired into `BaseWorker.record_send()`, advanced in engine's 1s tick alongside `PeakRateTracker`.
10. **Unconfirmed tracking**: `EventsStoreWorker` increments `_unconfirmed` on non-confirmed sends (M10).
11. **Recovering state**: Worker `inc_reconnection()` fires callback to engine, which transitions pattern state to `recovering` and auto-restores to `running` after 5s. `close_clients()` (disconnect manager) also uses `PatternState.RECOVERING` (L9).
12. **Responder metrics**: `inc_responder_responded/error` wired into commands/queries worker responder callbacks (M6).
13. **Broker ping timeout**: `get_broker_status()` wraps `client.ping()` in `concurrent.futures.ThreadPoolExecutor` with 3s timeout (E2).
