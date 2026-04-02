"""Tests for OBS-1, OBS-2, OBS-3: Tracing, Propagation, Metrics.

All tests run WITHOUT opentelemetry-api installed (no-op path).
OTel-present tests use unittest.mock to simulate the import.
"""

from __future__ import annotations

import threading
from typing import Any
from unittest.mock import MagicMock

# ═══════════════════════════════════════════════════════════════════
# semconv constants
# ═══════════════════════════════════════════════════════════════════


class TestSemconv:
    """Verify all semantic convention constants are defined and consistent."""

    def test_messaging_system_value(self) -> None:
        from kubemq._internal.semconv import MESSAGING_SYSTEM_VALUE

        assert MESSAGING_SYSTEM_VALUE == "kubemq"

    def test_all_metric_names_present(self) -> None:
        from kubemq._internal import semconv

        expected = [
            "METRIC_OPERATION_DURATION",
            "METRIC_SENT_MESSAGES",
            "METRIC_CONSUMED_MESSAGES",
            "METRIC_CONNECTION_COUNT",
            "METRIC_RECONNECTIONS",
            "METRIC_RETRY_ATTEMPTS",
            "METRIC_RETRY_EXHAUSTED",
            "METRIC_SEND_QUEUE_UTILIZATION",
        ]
        for name in expected:
            assert hasattr(semconv, name), f"Missing constant: {name}"

    def test_all_operation_names_present(self) -> None:
        from kubemq._internal import semconv

        expected = ["OP_PUBLISH", "OP_PROCESS", "OP_RECEIVE", "OP_SETTLE", "OP_SEND"]
        for name in expected:
            assert hasattr(semconv, name), f"Missing constant: {name}"

    def test_duration_histogram_buckets_sorted(self) -> None:
        from kubemq._internal.semconv import DURATION_HISTOGRAM_BUCKETS

        assert tuple(sorted(DURATION_HISTOGRAM_BUCKETS)) == DURATION_HISTOGRAM_BUCKETS
        assert len(DURATION_HISTOGRAM_BUCKETS) == 18

    def test_error_type_constants(self) -> None:
        from kubemq._internal import semconv

        expected = [
            "ERROR_TYPE_TRANSIENT",
            "ERROR_TYPE_TIMEOUT",
            "ERROR_TYPE_THROTTLING",
            "ERROR_TYPE_AUTHENTICATION",
            "ERROR_TYPE_AUTHORIZATION",
            "ERROR_TYPE_VALIDATION",
            "ERROR_TYPE_NOT_FOUND",
            "ERROR_TYPE_FATAL",
            "ERROR_TYPE_CANCELLATION",
            "ERROR_TYPE_BACKPRESSURE",
        ]
        for name in expected:
            assert hasattr(semconv, name), f"Missing constant: {name}"


# ═══════════════════════════════════════════════════════════════════
# OBS-1: KubeMQInstrumentor (no-op path)
# ═══════════════════════════════════════════════════════════════════


class TestKubeMQInstrumentorNoOp:
    """Test KubeMQInstrumentor when HAS_OTEL is False (no-op path)."""

    def _make_instrumentor(self) -> Any:
        from kubemq._internal.telemetry import KubeMQInstrumentor

        return KubeMQInstrumentor(
            client_id="test-client",
            address="localhost:50000",
        )

    def test_start_span_returns_noop(self) -> None:
        from kubemq._internal.telemetry import _NoOpSpan

        inst = self._make_instrumentor()
        span = inst.start_span("publish", "my-channel")
        assert isinstance(span, _NoOpSpan)

    def test_start_span_context_manager(self) -> None:
        inst = self._make_instrumentor()
        with inst.start_span("publish", "my-channel") as span:
            span.set_attribute("test", "value")
            assert span.is_recording() is False

    def test_record_error_noop(self) -> None:
        inst = self._make_instrumentor()
        span = inst.start_span("publish", "my-channel")
        inst.record_error(span, ValueError("test"), error_type="validation")

    def test_add_retry_event_noop(self) -> None:
        inst = self._make_instrumentor()
        span = inst.start_span("publish", "my-channel")
        inst.add_retry_event(span, attempt=1, delay_seconds=0.5, error_type="transient")

    def test_default_span_kind_mapping(self) -> None:
        from kubemq._internal.telemetry import KubeMQInstrumentor

        assert KubeMQInstrumentor._default_span_kind("publish") == "PRODUCER"
        assert KubeMQInstrumentor._default_span_kind("process") == "CONSUMER"
        assert KubeMQInstrumentor._default_span_kind("receive") == "CONSUMER"
        assert KubeMQInstrumentor._default_span_kind("settle") == "CONSUMER"
        assert KubeMQInstrumentor._default_span_kind("send") == "CLIENT"
        assert KubeMQInstrumentor._default_span_kind("unknown") == "INTERNAL"

    def test_address_parsing(self) -> None:
        from kubemq._internal.telemetry import KubeMQInstrumentor

        inst = KubeMQInstrumentor(client_id="c", address="myhost:9090")
        assert inst._host == "myhost"
        assert inst._port == 9090

    def test_address_parsing_default_port(self) -> None:
        from kubemq._internal.telemetry import KubeMQInstrumentor

        inst = KubeMQInstrumentor(client_id="c", address="myhost")
        assert inst._host == "myhost"
        assert inst._port == 50000

    def test_address_parsing_ipv6(self) -> None:
        from kubemq._internal.telemetry import KubeMQInstrumentor

        inst = KubeMQInstrumentor(client_id="c", address="[::1]:50000")
        assert inst._host == "[::1]"
        assert inst._port == 50000

    def test_start_span_with_custom_attributes(self) -> None:
        inst = self._make_instrumentor()
        with inst.start_span("publish", "ch", attributes={"custom.key": "val"}) as span:
            assert span.is_recording() is False

    def test_start_span_with_links(self) -> None:
        inst = self._make_instrumentor()
        with inst.start_span("receive", "ch", links=[]) as span:
            assert span.is_recording() is False


# ═══════════════════════════════════════════════════════════════════
# OBS-2: KubeMQTagsCarrier (no-op path)
# ═══════════════════════════════════════════════════════════════════


class TestKubeMQTagsCarrierNoOp:
    """Test KubeMQTagsCarrier when HAS_OTEL is False (no-op path)."""

    def test_inject_noop(self) -> None:
        from kubemq._internal.telemetry import KubeMQTagsCarrier

        tags: dict[str, str] = {"existing": "tag"}
        carrier = KubeMQTagsCarrier(tags)
        carrier.inject()
        assert tags == {"existing": "tag"}

    def test_extract_returns_none(self) -> None:
        from kubemq._internal.telemetry import KubeMQTagsCarrier

        carrier = KubeMQTagsCarrier({})
        assert carrier.extract() is None

    def test_get_set_keys(self) -> None:
        from kubemq._internal.telemetry import KubeMQTagsCarrier

        carrier = KubeMQTagsCarrier({"a": "1"})
        assert carrier.get("a") == "1"
        assert carrier.get("b") is None
        carrier.set("b", "2")
        assert carrier.get("b") == "2"
        assert set(carrier.keys()) == {"a", "b"}

    def test_tags_property(self) -> None:
        from kubemq._internal.telemetry import KubeMQTagsCarrier

        tags = {"k": "v"}
        carrier = KubeMQTagsCarrier(tags)
        assert carrier.tags is tags


class TestCreateLinkFromContextNoOp:
    """Test create_link_from_context when HAS_OTEL is False."""

    def test_returns_none_when_no_otel(self) -> None:
        from kubemq._internal.telemetry import create_link_from_context

        assert create_link_from_context(None) is None

    def test_returns_none_for_none_ctx(self) -> None:
        from kubemq._internal.telemetry import create_link_from_context

        assert create_link_from_context(None) is None


# ═══════════════════════════════════════════════════════════════════
# OBS-3: KubeMQMetrics (no-op path)
# ═══════════════════════════════════════════════════════════════════


class TestKubeMQMetricsNoOp:
    """Test KubeMQMetrics with _NoOpMeter (no-op path)."""

    def _make_metrics(self, **kwargs: Any) -> Any:
        from kubemq._internal.telemetry import _NOOP_METER, KubeMQMetrics

        return KubeMQMetrics(meter=_NOOP_METER, **kwargs)

    def test_all_instruments_created(self) -> None:
        m = self._make_metrics()
        assert m._operation_duration is not None
        assert m._sent_messages is not None
        assert m._consumed_messages is not None
        assert m._connection_count is not None
        assert m._reconnections is not None
        assert m._retry_attempts is not None
        assert m._retry_exhausted is not None

    def test_record_operation_duration(self) -> None:
        m = self._make_metrics()
        m.record_operation_duration(0.5, "publish", "ch1")

    def test_record_operation_duration_with_error(self) -> None:
        m = self._make_metrics()
        m.record_operation_duration(0.5, "publish", "ch1", error_type="transient")

    def test_record_sent_message(self) -> None:
        m = self._make_metrics()
        m.record_sent_message("publish", "ch1")

    def test_record_consumed_message(self) -> None:
        m = self._make_metrics()
        m.record_consumed_message("process", "ch1")

    def test_record_connection_opened_closed(self) -> None:
        m = self._make_metrics()
        m.record_connection_opened()
        m.record_connection_closed()

    def test_record_reconnection_attempt(self) -> None:
        m = self._make_metrics()
        m.record_reconnection_attempt()

    def test_record_retry_attempt(self) -> None:
        m = self._make_metrics()
        m.record_retry_attempt("publish", "transient")

    def test_record_retry_exhausted(self) -> None:
        m = self._make_metrics()
        m.record_retry_exhausted("publish", "transient")


class TestCardinalityManagement:
    """Test metric cardinality management."""

    def _make_metrics(self, **kwargs: Any) -> Any:
        from kubemq._internal.telemetry import _NOOP_METER, KubeMQMetrics

        return KubeMQMetrics(meter=_NOOP_METER, **kwargs)

    def test_channel_within_threshold_included(self) -> None:
        m = self._make_metrics(max_channel_cardinality=5)
        assert m._should_include_channel("ch1") is True
        assert m._should_include_channel("ch2") is True

    def test_channel_exceeding_threshold_omitted(self) -> None:
        m = self._make_metrics(max_channel_cardinality=2)
        assert m._should_include_channel("ch1") is True
        assert m._should_include_channel("ch2") is True
        assert m._should_include_channel("ch3") is False

    def test_already_tracked_channel_always_included(self) -> None:
        m = self._make_metrics(max_channel_cardinality=2)
        assert m._should_include_channel("ch1") is True
        assert m._should_include_channel("ch2") is True
        assert m._should_include_channel("ch1") is True

    def test_allowlisted_channel_always_included(self) -> None:
        m = self._make_metrics(
            max_channel_cardinality=1,
            channel_allowlist={"vip-channel"},
        )
        assert m._should_include_channel("ch1") is True
        assert m._should_include_channel("ch2") is False
        assert m._should_include_channel("vip-channel") is True

    def test_warning_logged_once(self) -> None:
        mock_logger = MagicMock()
        m = self._make_metrics(max_channel_cardinality=1, logger=mock_logger)
        m._should_include_channel("ch1")
        m._should_include_channel("ch2")
        m._should_include_channel("ch3")
        assert mock_logger.warning.call_count == 1

    def test_thread_safety(self) -> None:
        m = self._make_metrics(max_channel_cardinality=50)
        results: list[bool] = []
        errors: list[Exception] = []

        def worker(channel: str) -> None:
            try:
                results.append(m._should_include_channel(channel))
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(f"ch-{i}",)) for i in range(100)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert len(results) == 100
        included = sum(1 for r in results if r)
        assert included == 50


# ═══════════════════════════════════════════════════════════════════
# error_code_to_error_type
# ═══════════════════════════════════════════════════════════════════


class TestErrorCodeToErrorType:
    """Test error_code_to_error_type mapping."""

    def test_all_error_codes_mapped(self) -> None:
        from kubemq._internal.telemetry import error_code_to_error_type
        from kubemq.core.exceptions import ErrorCode

        for code in ErrorCode:
            result = error_code_to_error_type(code)
            assert isinstance(result, str), f"ErrorCode.{code.name} not mapped"
            assert result != "", f"ErrorCode.{code.name} mapped to empty string"

    def test_specific_mappings(self) -> None:
        from kubemq._internal.telemetry import error_code_to_error_type
        from kubemq.core.exceptions import ErrorCode

        assert error_code_to_error_type(ErrorCode.CONNECTION_TIMEOUT) == "timeout"
        assert error_code_to_error_type(ErrorCode.UNAVAILABLE) == "transient"
        assert error_code_to_error_type(ErrorCode.AUTH_FAILED) == "authentication"
        assert error_code_to_error_type(ErrorCode.PERMISSION_DENIED) == "authorization"
        assert error_code_to_error_type(ErrorCode.VALIDATION_ERROR) == "validation"
        assert error_code_to_error_type(ErrorCode.NOT_FOUND) == "not_found"
        assert error_code_to_error_type(ErrorCode.RESOURCE_EXHAUSTED) == "throttling"
        assert error_code_to_error_type(ErrorCode.INTERNAL) == "fatal"
        assert error_code_to_error_type(ErrorCode.CANCELLED) == "cancellation"
        assert error_code_to_error_type(ErrorCode.BUFFER_FULL) == "backpressure"

    def test_non_error_code_returns_fatal(self) -> None:
        from kubemq._internal.telemetry import error_code_to_error_type

        assert error_code_to_error_type("not_an_enum") == "fatal"
        assert error_code_to_error_type(None) == "fatal"
        assert error_code_to_error_type(42) == "fatal"


# ═══════════════════════════════════════════════════════════════════
# ClientConfig integration
# ═══════════════════════════════════════════════════════════════════


class TestClientConfigObservability:
    """Test observability fields on ClientConfig."""

    def test_default_max_channel_cardinality(self) -> None:
        from kubemq.core.config import ClientConfig

        cfg = ClientConfig(address="localhost:50000")
        assert cfg.max_channel_cardinality == 100

    def test_default_channel_allowlist_empty(self) -> None:
        from kubemq.core.config import ClientConfig

        cfg = ClientConfig(address="localhost:50000")
        assert cfg.channel_allowlist == []

    def test_custom_cardinality(self) -> None:
        from kubemq.core.config import ClientConfig

        cfg = ClientConfig(
            address="localhost:50000",
            max_channel_cardinality=50,
            channel_allowlist=["vip"],
        )
        assert cfg.max_channel_cardinality == 50
        assert cfg.channel_allowlist == ["vip"]

    def test_mutable_default_not_shared(self) -> None:
        from kubemq.core.config import ClientConfig

        cfg1 = ClientConfig(address="localhost:50000")
        cfg2 = ClientConfig(address="localhost:50000")
        cfg1.channel_allowlist.append("test")
        assert "test" not in cfg2.channel_allowlist
