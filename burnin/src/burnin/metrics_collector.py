"""Prometheus metrics collection — all 26 metrics + helper functions."""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

SDK_LABEL = "python"

# --- Counters ---

messages_sent_total = Counter(
    "burnin_messages_sent_total",
    "Total messages sent",
    ["sdk", "pattern", "producer_id"],
)

messages_received_total = Counter(
    "burnin_messages_received_total",
    "Total messages received",
    ["sdk", "pattern", "consumer_id"],
)

messages_lost_total = Counter(
    "burnin_messages_lost_total",
    "Total messages confirmed lost",
    ["sdk", "pattern"],
)

messages_duplicated_total = Counter(
    "burnin_messages_duplicated_total",
    "Total duplicate messages received",
    ["sdk", "pattern"],
)

messages_corrupted_total = Counter(
    "burnin_messages_corrupted_total",
    "Total corrupted messages received",
    ["sdk", "pattern"],
)

messages_out_of_order_total = Counter(
    "burnin_messages_out_of_order_total",
    "Total out-of-order messages received",
    ["sdk", "pattern"],
)

messages_unconfirmed_total = Counter(
    "burnin_messages_unconfirmed_total",
    "Total unconfirmed messages",
    ["sdk", "pattern"],
)

reconnection_duplicates_total = Counter(
    "burnin_reconnection_duplicates_total",
    "Duplicates received after reconnection",
    ["sdk", "pattern"],
)

errors_total = Counter(
    "burnin_errors_total",
    "Total errors",
    ["sdk", "pattern", "error_type"],
)

reconnections_total = Counter(
    "burnin_reconnections_total",
    "Total reconnections",
    ["sdk", "pattern"],
)

bytes_sent_total = Counter(
    "burnin_bytes_sent_total",
    "Total bytes sent",
    ["sdk", "pattern"],
)

bytes_received_total = Counter(
    "burnin_bytes_received_total",
    "Total bytes received",
    ["sdk", "pattern"],
)

rpc_responses_total = Counter(
    "burnin_rpc_responses_total",
    "Total RPC responses by status",
    ["sdk", "pattern", "status"],
)

downtime_seconds_total = Counter(
    "burnin_downtime_seconds_total",
    "Total downtime in seconds",
    ["sdk", "pattern"],
)

forced_disconnects_total = Counter(
    "burnin_forced_disconnects_total",
    "Total forced disconnects",
    ["sdk"],
)

# --- Histograms ---

_LATENCY_BUCKETS = (
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025,
    0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
)

_RPC_BUCKETS = (
    0.001, 0.005, 0.01, 0.025, 0.05,
    0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
)

message_latency_seconds = Histogram(
    "burnin_message_latency_seconds",
    "End-to-end message latency",
    ["sdk", "pattern"],
    buckets=_LATENCY_BUCKETS,
)

send_duration_seconds = Histogram(
    "burnin_send_duration_seconds",
    "Duration of send operations",
    ["sdk", "pattern"],
    buckets=_LATENCY_BUCKETS,
)

rpc_duration_seconds = Histogram(
    "burnin_rpc_duration_seconds",
    "Duration of RPC operations",
    ["sdk", "pattern"],
    buckets=_RPC_BUCKETS,
)

# --- Gauges ---

active_connections = Gauge(
    "burnin_active_connections",
    "Number of active connections",
    ["sdk", "pattern"],
)

uptime_seconds = Gauge(
    "burnin_uptime_seconds",
    "Uptime in seconds",
    ["sdk"],
)

target_rate = Gauge(
    "burnin_target_rate",
    "Target message rate",
    ["sdk", "pattern"],
)

actual_rate = Gauge(
    "burnin_actual_rate",
    "Actual message rate",
    ["sdk", "pattern"],
)

consumer_lag_messages = Gauge(
    "burnin_consumer_lag_messages",
    "Consumer lag in messages",
    ["sdk", "pattern"],
)

consumer_group_balance_ratio = Gauge(
    "burnin_consumer_group_balance_ratio",
    "Consumer group balance ratio",
    ["sdk", "pattern"],
)

warmup_active = Gauge(
    "burnin_warmup_active",
    "Whether warmup is active",
    ["sdk"],
)

active_workers = Gauge(
    "burnin_active_workers",
    "Number of active worker threads",
    ["sdk"],
)


# --- Helper functions ---

def inc_sent(pattern: str, producer_id: str, byte_count: int = 0) -> None:
    messages_sent_total.labels(sdk=SDK_LABEL, pattern=pattern, producer_id=producer_id).inc()
    if byte_count > 0:
        bytes_sent_total.labels(sdk=SDK_LABEL, pattern=pattern).inc(byte_count)


def inc_received(pattern: str, consumer_id: str, byte_count: int = 0) -> None:
    messages_received_total.labels(sdk=SDK_LABEL, pattern=pattern, consumer_id=consumer_id).inc()
    if byte_count > 0:
        bytes_received_total.labels(sdk=SDK_LABEL, pattern=pattern).inc(byte_count)


def inc_lost(pattern: str, count: int = 1) -> None:
    messages_lost_total.labels(sdk=SDK_LABEL, pattern=pattern).inc(count)


def inc_duplicated(pattern: str) -> None:
    messages_duplicated_total.labels(sdk=SDK_LABEL, pattern=pattern).inc()


def inc_corrupted(pattern: str) -> None:
    messages_corrupted_total.labels(sdk=SDK_LABEL, pattern=pattern).inc()


def inc_out_of_order(pattern: str) -> None:
    messages_out_of_order_total.labels(sdk=SDK_LABEL, pattern=pattern).inc()


def inc_unconfirmed(pattern: str) -> None:
    messages_unconfirmed_total.labels(sdk=SDK_LABEL, pattern=pattern).inc()


def inc_reconnection_duplicates(pattern: str) -> None:
    reconnection_duplicates_total.labels(sdk=SDK_LABEL, pattern=pattern).inc()


def inc_error(pattern: str, error_type: str) -> None:
    errors_total.labels(sdk=SDK_LABEL, pattern=pattern, error_type=error_type).inc()


def inc_reconnections(pattern: str) -> None:
    reconnections_total.labels(sdk=SDK_LABEL, pattern=pattern).inc()


def inc_rpc_response(pattern: str, status: str) -> None:
    rpc_responses_total.labels(sdk=SDK_LABEL, pattern=pattern, status=status).inc()


def add_downtime(pattern: str, seconds: float) -> None:
    downtime_seconds_total.labels(sdk=SDK_LABEL, pattern=pattern).inc(seconds)


def inc_forced_disconnects() -> None:
    forced_disconnects_total.labels(sdk=SDK_LABEL).inc()


def observe_latency(pattern: str, seconds: float) -> None:
    message_latency_seconds.labels(sdk=SDK_LABEL, pattern=pattern).observe(seconds)


def observe_send_duration(pattern: str, seconds: float) -> None:
    send_duration_seconds.labels(sdk=SDK_LABEL, pattern=pattern).observe(seconds)


def observe_rpc_duration(pattern: str, seconds: float) -> None:
    rpc_duration_seconds.labels(sdk=SDK_LABEL, pattern=pattern).observe(seconds)


def set_active_connections(pattern: str, count: float) -> None:
    active_connections.labels(sdk=SDK_LABEL, pattern=pattern).set(count)


def set_uptime(seconds: float) -> None:
    uptime_seconds.labels(sdk=SDK_LABEL).set(seconds)


def set_target_rate(pattern: str, rate: float) -> None:
    target_rate.labels(sdk=SDK_LABEL, pattern=pattern).set(rate)


def set_actual_rate(pattern: str, rate: float) -> None:
    actual_rate.labels(sdk=SDK_LABEL, pattern=pattern).set(rate)


def set_consumer_lag(pattern: str, lag: float) -> None:
    consumer_lag_messages.labels(sdk=SDK_LABEL, pattern=pattern).set(lag)


def set_warmup_active(val: float) -> None:
    warmup_active.labels(sdk=SDK_LABEL).set(val)


def set_active_workers(count: float) -> None:
    active_workers.labels(sdk=SDK_LABEL).set(count)


_ALL_PATTERNS = ["events", "events_store", "queue_stream", "queue_simple", "commands", "queries"]


def pre_initialize() -> None:
    """Pre-initialize all Prometheus metrics to 0 with baseline label values.

    Prevents absent() alerts in dashboards and ensures all time series
    exist from process boot (spec §2, §8.3).
    """
    for p in _ALL_PATTERNS:
        messages_sent_total.labels(sdk=SDK_LABEL, pattern=p, producer_id="p-000")
        messages_received_total.labels(sdk=SDK_LABEL, pattern=p, consumer_id="c-000")
        messages_lost_total.labels(sdk=SDK_LABEL, pattern=p)
        messages_duplicated_total.labels(sdk=SDK_LABEL, pattern=p)
        messages_corrupted_total.labels(sdk=SDK_LABEL, pattern=p)
        messages_out_of_order_total.labels(sdk=SDK_LABEL, pattern=p)
        messages_unconfirmed_total.labels(sdk=SDK_LABEL, pattern=p)
        reconnection_duplicates_total.labels(sdk=SDK_LABEL, pattern=p)
        errors_total.labels(sdk=SDK_LABEL, pattern=p, error_type="send_failure")
        reconnections_total.labels(sdk=SDK_LABEL, pattern=p)
        bytes_sent_total.labels(sdk=SDK_LABEL, pattern=p)
        bytes_received_total.labels(sdk=SDK_LABEL, pattern=p)
        downtime_seconds_total.labels(sdk=SDK_LABEL, pattern=p)
        message_latency_seconds.labels(sdk=SDK_LABEL, pattern=p)
        send_duration_seconds.labels(sdk=SDK_LABEL, pattern=p)
        active_connections.labels(sdk=SDK_LABEL, pattern=p).set(0)
        target_rate.labels(sdk=SDK_LABEL, pattern=p).set(0)
        actual_rate.labels(sdk=SDK_LABEL, pattern=p).set(0)
        consumer_lag_messages.labels(sdk=SDK_LABEL, pattern=p).set(0)

    for p in ["commands", "queries"]:
        rpc_responses_total.labels(sdk=SDK_LABEL, pattern=p, status="success")
        rpc_responses_total.labels(sdk=SDK_LABEL, pattern=p, status="timeout")
        rpc_responses_total.labels(sdk=SDK_LABEL, pattern=p, status="error")
        rpc_duration_seconds.labels(sdk=SDK_LABEL, pattern=p)

    uptime_seconds.labels(sdk=SDK_LABEL).set(0)
    warmup_active.labels(sdk=SDK_LABEL).set(0)
    active_workers.labels(sdk=SDK_LABEL).set(0)
    forced_disconnects_total.labels(sdk=SDK_LABEL)
