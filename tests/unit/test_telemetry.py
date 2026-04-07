"""Tests for kubemq._internal.telemetry module."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from kubemq._internal.telemetry import (
    _NOOP_COUNTER,
    _NOOP_HISTOGRAM,
    _NOOP_METER,
    _NOOP_SPAN,
    _NOOP_SPAN_CONTEXT,
    _NOOP_TRACER,
    _NOOP_UP_DOWN_COUNTER,
    HAS_OTEL,
    KubeMQInstrumentor,
    KubeMQMetrics,
    KubeMQTagsCarrier,
    create_link_from_context,
    error_code_to_error_type,
    get_meter,
    get_tracer,
)

# ==============================================================================
# _NoOpSpan tests
# ==============================================================================


class TestNoOpSpan:
    """Tests for _NoOpSpan — all methods are no-ops."""

    def test_set_attribute(self):
        _NOOP_SPAN.set_attribute("key", "value")

    def test_set_status(self):
        _NOOP_SPAN.set_status("OK", description="all good")

    def test_add_event(self):
        _NOOP_SPAN.add_event("my-event", attributes={"k": "v"}, timestamp=12345)

    def test_record_exception(self):
        _NOOP_SPAN.record_exception(
            RuntimeError("boom"),
            attributes={"k": "v"},
            timestamp=12345,
            escaped=True,
        )

    def test_is_recording_returns_false(self):
        assert _NOOP_SPAN.is_recording() is False

    def test_end(self):
        _NOOP_SPAN.end(end_time=99999)

    def test_get_span_context_returns_noop_context(self):
        ctx = _NOOP_SPAN.get_span_context()
        assert ctx is _NOOP_SPAN_CONTEXT

    def test_context_manager(self):
        with _NOOP_SPAN as span:
            assert span is _NOOP_SPAN


# ==============================================================================
# _NoOpSpanContext tests
# ==============================================================================


class TestNoOpSpanContext:
    """Tests for _NoOpSpanContext defaults."""

    def test_trace_id_zero(self):
        assert _NOOP_SPAN_CONTEXT.trace_id == 0

    def test_span_id_zero(self):
        assert _NOOP_SPAN_CONTEXT.span_id == 0

    def test_is_valid_false(self):
        assert _NOOP_SPAN_CONTEXT.is_valid is False

    def test_trace_flags_zero(self):
        assert _NOOP_SPAN_CONTEXT.trace_flags == 0

    def test_trace_state_none(self):
        assert _NOOP_SPAN_CONTEXT.trace_state is None


# ==============================================================================
# _NoOpTracer tests
# ==============================================================================


class TestNoOpTracer:
    """Tests for _NoOpTracer."""

    def test_start_span_returns_noop_span(self):
        span = _NOOP_TRACER.start_span("test-span")
        assert span is _NOOP_SPAN

    def test_start_as_current_span_returns_noop_span(self):
        span = _NOOP_TRACER.start_as_current_span("test-span")
        assert span is _NOOP_SPAN

    def test_start_as_current_span_with_all_args(self):
        span = _NOOP_TRACER.start_as_current_span(
            "test",
            context=None,
            kind=None,
            attributes={"k": "v"},
            links=None,
            start_time=0,
            record_exception=True,
            set_status_on_exception=True,
            end_on_exit=True,
        )
        assert span is _NOOP_SPAN


# ==============================================================================
# _NoOpCounter / _NoOpHistogram / _NoOpUpDownCounter tests
# ==============================================================================


class TestNoOpMetricInstruments:
    """Tests for no-op metric instruments."""

    def test_counter_add(self):
        _NOOP_COUNTER.add(1, attributes={"k": "v"})

    def test_histogram_record(self):
        _NOOP_HISTOGRAM.record(0.5, attributes={"k": "v"})

    def test_up_down_counter_add(self):
        _NOOP_UP_DOWN_COUNTER.add(-1, attributes={"k": "v"})


# ==============================================================================
# _NoOpMeter tests
# ==============================================================================


class TestNoOpMeter:
    """Tests for _NoOpMeter returning no-op instruments."""

    def test_create_counter(self):
        counter = _NOOP_METER.create_counter("test", unit="1", description="desc")
        assert counter is _NOOP_COUNTER

    def test_create_histogram(self):
        hist = _NOOP_METER.create_histogram("test", unit="s", description="desc")
        assert hist is _NOOP_HISTOGRAM

    def test_create_up_down_counter(self):
        udc = _NOOP_METER.create_up_down_counter("test", unit="1", description="desc")
        assert udc is _NOOP_UP_DOWN_COUNTER


# ==============================================================================
# get_tracer / get_meter tests (HAS_OTEL=False path)
# ==============================================================================


class TestGetTracerMeterNoOtel:
    """Tests for get_tracer/get_meter when HAS_OTEL is False."""

    def test_get_tracer_returns_noop_when_no_otel(self):
        with patch("kubemq._internal.telemetry.HAS_OTEL", False):
            tracer = get_tracer()
            assert tracer is _NOOP_TRACER

    def test_get_meter_returns_noop_when_no_otel(self):
        with patch("kubemq._internal.telemetry.HAS_OTEL", False):
            meter = get_meter()
            assert meter is _NOOP_METER


# ==============================================================================
# get_tracer / get_meter tests (HAS_OTEL=True path, mocked)
# ==============================================================================


class TestGetTracerMeterWithOtel:
    """Tests for get_tracer/get_meter when HAS_OTEL is True."""

    def test_get_tracer_with_otel_calls_provider(self):
        mock_provider = MagicMock()
        mock_tracer = MagicMock()
        mock_provider.get_tracer.return_value = mock_tracer

        with (
            patch("kubemq._internal.telemetry.HAS_OTEL", True),
            patch("kubemq.__version__", "1.2.3"),
        ):
            result = get_tracer(tracer_provider=mock_provider)

        mock_provider.get_tracer.assert_called_once_with("kubemq", "1.2.3")
        assert result is mock_tracer

    def test_get_tracer_with_otel_falls_back_to_global(self):
        mock_global_provider = MagicMock()
        mock_tracer = MagicMock()
        mock_global_provider.get_tracer.return_value = mock_tracer

        with (
            patch("kubemq._internal.telemetry.HAS_OTEL", True),
            patch("kubemq._internal.telemetry.otel_trace", create=True) as mock_otel_trace,
            patch("kubemq.__version__", "1.2.3"),
        ):
            mock_otel_trace.get_tracer_provider.return_value = mock_global_provider
            result = get_tracer(tracer_provider=None)

        mock_global_provider.get_tracer.assert_called_once_with("kubemq", "1.2.3")
        assert result is mock_tracer

    def test_get_meter_with_otel_calls_provider(self):
        mock_provider = MagicMock()
        mock_meter = MagicMock()
        mock_provider.get_meter.return_value = mock_meter

        with (
            patch("kubemq._internal.telemetry.HAS_OTEL", True),
            patch("kubemq.__version__", "1.2.3"),
        ):
            result = get_meter(meter_provider=mock_provider)

        mock_provider.get_meter.assert_called_once_with("kubemq", "1.2.3")
        assert result is mock_meter

    def test_get_meter_with_otel_falls_back_to_global(self):
        mock_global_provider = MagicMock()
        mock_meter = MagicMock()
        mock_global_provider.get_meter.return_value = mock_meter

        with (
            patch("kubemq._internal.telemetry.HAS_OTEL", True),
            patch("kubemq._internal.telemetry.otel_metrics", create=True) as mock_otel_metrics,
            patch("kubemq.__version__", "1.2.3"),
        ):
            mock_otel_metrics.get_meter_provider.return_value = mock_global_provider
            result = get_meter(meter_provider=None)

        mock_global_provider.get_meter.assert_called_once_with("kubemq", "1.2.3")
        assert result is mock_meter


# ==============================================================================
# KubeMQInstrumentor.record_error tests
# ==============================================================================


class TestKubeMQInstrumentorRecordError:
    """Tests for record_error with mocked span."""

    def test_record_error_noop_when_has_otel_false(self):
        instrumentor = KubeMQInstrumentor("client-1", "host:50000")
        mock_span = MagicMock()
        with patch("kubemq._internal.telemetry.HAS_OTEL", False):
            instrumentor.record_error(mock_span, RuntimeError("err"), "timeout")
        mock_span.set_status.assert_not_called()

    def test_record_error_with_recording_span(self):
        instrumentor = KubeMQInstrumentor("client-1", "host:50000")
        mock_span = MagicMock()
        mock_span.is_recording.return_value = True
        err = RuntimeError("test error")

        mock_status_code = MagicMock()
        with (
            patch("kubemq._internal.telemetry.HAS_OTEL", True),
            patch.dict(
                "sys.modules",
                {
                    "opentelemetry.trace": MagicMock(StatusCode=MagicMock(ERROR=mock_status_code)),
                },
            ),
        ):
            instrumentor.record_error(mock_span, err, "timeout")

        mock_span.set_status.assert_called_once()
        mock_span.record_exception.assert_called_once_with(err)
        mock_span.set_attribute.assert_called_once_with("error.type", "timeout")

    def test_record_error_without_error_type(self):
        instrumentor = KubeMQInstrumentor("client-1", "host:50000")
        mock_span = MagicMock()
        mock_span.is_recording.return_value = True
        err = RuntimeError("test error")

        with (
            patch("kubemq._internal.telemetry.HAS_OTEL", True),
            patch.dict(
                "sys.modules",
                {
                    "opentelemetry.trace": MagicMock(StatusCode=MagicMock(ERROR=MagicMock())),
                },
            ),
        ):
            instrumentor.record_error(mock_span, err, "")

        mock_span.set_status.assert_called_once()
        mock_span.record_exception.assert_called_once_with(err)
        mock_span.set_attribute.assert_not_called()

    def test_record_error_span_not_recording(self):
        instrumentor = KubeMQInstrumentor("client-1", "host:50000")
        mock_span = MagicMock()
        mock_span.is_recording.return_value = False

        with (
            patch("kubemq._internal.telemetry.HAS_OTEL", True),
            patch.dict(
                "sys.modules",
                {
                    "opentelemetry.trace": MagicMock(StatusCode=MagicMock(ERROR=MagicMock())),
                },
            ),
        ):
            instrumentor.record_error(mock_span, RuntimeError("err"), "timeout")

        mock_span.set_status.assert_not_called()
        mock_span.record_exception.assert_not_called()


# ==============================================================================
# KubeMQInstrumentor._resolve_span_kind tests
# ==============================================================================


class TestResolveSpanKind:
    """Tests for _resolve_span_kind static method."""

    def test_returns_none_when_no_otel(self):
        with patch("kubemq._internal.telemetry.HAS_OTEL", False):
            result = KubeMQInstrumentor._resolve_span_kind("PRODUCER")
            assert result is None

    @pytest.mark.skipif(not HAS_OTEL, reason="OTel not installed")
    def test_producer_string(self):
        from opentelemetry.trace import SpanKind

        result = KubeMQInstrumentor._resolve_span_kind("PRODUCER")
        assert result == SpanKind.PRODUCER

    @pytest.mark.skipif(not HAS_OTEL, reason="OTel not installed")
    def test_consumer_string(self):
        from opentelemetry.trace import SpanKind

        result = KubeMQInstrumentor._resolve_span_kind("CONSUMER")
        assert result == SpanKind.CONSUMER

    @pytest.mark.skipif(not HAS_OTEL, reason="OTel not installed")
    def test_client_string(self):
        from opentelemetry.trace import SpanKind

        result = KubeMQInstrumentor._resolve_span_kind("CLIENT")
        assert result == SpanKind.CLIENT

    @pytest.mark.skipif(not HAS_OTEL, reason="OTel not installed")
    def test_server_string(self):
        from opentelemetry.trace import SpanKind

        result = KubeMQInstrumentor._resolve_span_kind("SERVER")
        assert result == SpanKind.SERVER

    @pytest.mark.skipif(not HAS_OTEL, reason="OTel not installed")
    def test_internal_string(self):
        from opentelemetry.trace import SpanKind

        result = KubeMQInstrumentor._resolve_span_kind("INTERNAL")
        assert result == SpanKind.INTERNAL

    @pytest.mark.skipif(not HAS_OTEL, reason="OTel not installed")
    def test_unknown_string_defaults_to_internal(self):
        from opentelemetry.trace import SpanKind

        result = KubeMQInstrumentor._resolve_span_kind("UNKNOWN")
        assert result == SpanKind.INTERNAL

    @pytest.mark.skipif(not HAS_OTEL, reason="OTel not installed")
    def test_span_kind_enum_passthrough(self):
        from opentelemetry.trace import SpanKind

        result = KubeMQInstrumentor._resolve_span_kind(SpanKind.PRODUCER)
        assert result == SpanKind.PRODUCER

    @pytest.mark.skipif(not HAS_OTEL, reason="OTel not installed")
    def test_case_insensitive(self):
        from opentelemetry.trace import SpanKind

        result = KubeMQInstrumentor._resolve_span_kind("producer")
        assert result == SpanKind.PRODUCER


# ==============================================================================
# KubeMQTagsCarrier tests
# ==============================================================================


class TestKubeMQTagsCarrier:
    """Tests for KubeMQTagsCarrier inject/extract."""

    def test_get_returns_value(self):
        carrier = KubeMQTagsCarrier({"traceparent": "00-abc-def-01"})
        assert carrier.get("traceparent") == "00-abc-def-01"

    def test_get_returns_none_for_missing(self):
        carrier = KubeMQTagsCarrier({})
        assert carrier.get("traceparent") is None

    def test_get_accepts_default_parameter(self):
        """Regression: OTel TextMapGetter calls carrier.get(key, default)."""
        carrier = KubeMQTagsCarrier({"traceparent": "00-abc-def-01"})
        # Must accept 3 positional args without TypeError
        assert carrier.get("traceparent", None) == "00-abc-def-01"
        assert carrier.get("missing", "fallback") == "fallback"
        assert carrier.get("missing", None) is None

    def test_get_default_not_used_when_key_exists(self):
        carrier = KubeMQTagsCarrier({"key": "value"})
        assert carrier.get("key", "default") == "value"

    def test_set_writes_value(self):
        carrier = KubeMQTagsCarrier({})
        carrier.set("traceparent", "00-abc-def-01")
        assert carrier.tags["traceparent"] == "00-abc-def-01"

    def test_keys_returns_all_keys(self):
        carrier = KubeMQTagsCarrier({"a": "1", "b": "2"})
        assert sorted(carrier.keys()) == ["a", "b"]

    def test_inject_noop_when_no_otel(self):
        with patch("kubemq._internal.telemetry.HAS_OTEL", False):
            carrier = KubeMQTagsCarrier({})
            carrier.inject()
            assert carrier.tags == {}

    def test_extract_returns_none_when_no_otel(self):
        with patch("kubemq._internal.telemetry.HAS_OTEL", False):
            carrier = KubeMQTagsCarrier({"traceparent": "value"})
            result = carrier.extract()
            assert result is None

    @pytest.mark.skipif(not HAS_OTEL, reason="OTel not installed")
    def test_inject_calls_propagate_inject(self):
        with patch("kubemq._internal.telemetry.HAS_OTEL", True):
            carrier = KubeMQTagsCarrier({})
            with patch("opentelemetry.propagate.inject") as mock_inject:
                carrier.inject()
                mock_inject.assert_called_once_with(carrier=carrier, context=None)

    @pytest.mark.skipif(not HAS_OTEL, reason="OTel not installed")
    def test_extract_calls_propagate_extract(self):
        with patch("kubemq._internal.telemetry.HAS_OTEL", True):
            carrier = KubeMQTagsCarrier({"traceparent": "val"})
            with patch("opentelemetry.propagate.extract") as mock_extract:
                mock_extract.return_value = MagicMock()
                result = carrier.extract()
                mock_extract.assert_called_once_with(carrier=carrier)
                assert result is mock_extract.return_value

    @pytest.mark.skipif(not HAS_OTEL, reason="OTel not installed")
    def test_inject_with_context(self):
        with patch("kubemq._internal.telemetry.HAS_OTEL", True):
            carrier = KubeMQTagsCarrier({})
            mock_ctx = MagicMock()
            with patch("opentelemetry.propagate.inject") as mock_inject:
                carrier.inject(context=mock_ctx)
                mock_inject.assert_called_once_with(carrier=carrier, context=mock_ctx)


# ==============================================================================
# create_link_from_context tests
# ==============================================================================


class TestCreateLinkFromContext:
    """Tests for create_link_from_context."""

    def test_returns_none_when_no_otel(self):
        with patch("kubemq._internal.telemetry.HAS_OTEL", False):
            result = create_link_from_context(MagicMock())
            assert result is None

    def test_returns_none_when_ctx_is_none(self):
        result = create_link_from_context(None)
        assert result is None

    @pytest.mark.skipif(not HAS_OTEL, reason="OTel not installed")
    def test_returns_link_with_valid_span_context(self):
        from opentelemetry.trace import Link, SpanContext, TraceFlags

        mock_span_ctx = SpanContext(
            trace_id=1,
            span_id=2,
            is_remote=True,
            trace_flags=TraceFlags(1),
        )
        mock_span = MagicMock()
        mock_span.get_span_context.return_value = mock_span_ctx

        with patch("kubemq._internal.telemetry.HAS_OTEL", True):
            with patch("opentelemetry.trace.get_current_span", return_value=mock_span):
                result = create_link_from_context(MagicMock())

        assert isinstance(result, Link)

    @pytest.mark.skipif(not HAS_OTEL, reason="OTel not installed")
    def test_returns_none_with_invalid_span_context(self):
        mock_span_ctx = MagicMock()
        mock_span_ctx.is_valid = False
        mock_span = MagicMock()
        mock_span.get_span_context.return_value = mock_span_ctx

        with patch("kubemq._internal.telemetry.HAS_OTEL", True):
            with patch("opentelemetry.trace.get_current_span", return_value=mock_span):
                result = create_link_from_context(MagicMock())

        assert result is None


# ==============================================================================
# error_code_to_error_type tests
# ==============================================================================


class TestErrorCodeToErrorType:
    """Tests for error_code_to_error_type mapping."""

    def test_known_error_codes(self):
        from kubemq.core.exceptions import ErrorCode

        assert error_code_to_error_type(ErrorCode.CONNECTION_TIMEOUT) == "timeout"
        assert error_code_to_error_type(ErrorCode.UNAVAILABLE) == "transient"
        assert error_code_to_error_type(ErrorCode.CONNECTION_NOT_READY) == "transient"
        assert error_code_to_error_type(ErrorCode.AUTH_FAILED) == "authentication"
        assert error_code_to_error_type(ErrorCode.PERMISSION_DENIED) == "authorization"
        assert error_code_to_error_type(ErrorCode.VALIDATION_ERROR) == "validation"
        assert error_code_to_error_type(ErrorCode.ALREADY_EXISTS) == "validation"
        assert error_code_to_error_type(ErrorCode.OUT_OF_RANGE) == "validation"
        assert error_code_to_error_type(ErrorCode.NOT_FOUND) == "not_found"
        assert error_code_to_error_type(ErrorCode.RESOURCE_EXHAUSTED) == "throttling"
        assert error_code_to_error_type(ErrorCode.ABORTED) == "transient"
        assert error_code_to_error_type(ErrorCode.INTERNAL) == "fatal"
        assert error_code_to_error_type(ErrorCode.UNKNOWN) == "fatal"
        assert error_code_to_error_type(ErrorCode.UNIMPLEMENTED) == "fatal"
        assert error_code_to_error_type(ErrorCode.DATA_LOSS) == "fatal"
        assert error_code_to_error_type(ErrorCode.CANCELLED) == "cancellation"
        assert error_code_to_error_type(ErrorCode.BUFFER_FULL) == "backpressure"
        assert error_code_to_error_type(ErrorCode.STREAM_BROKEN) == "transient"
        assert error_code_to_error_type(ErrorCode.CLIENT_CLOSED) == "cancellation"
        assert error_code_to_error_type(ErrorCode.CONFIGURATION_ERROR) == "validation"

    def test_unknown_value_returns_fatal(self):
        result = error_code_to_error_type("not-an-error-code")
        assert result == "fatal"

    def test_import_error_path(self):
        import sys

        real_module = sys.modules.get("kubemq.core.exceptions")
        sys.modules["kubemq.core.exceptions"] = None  # type: ignore[assignment]
        try:
            result = error_code_to_error_type("anything")
            assert result == "unknown"
        finally:
            if real_module is not None:
                sys.modules["kubemq.core.exceptions"] = real_module
            else:
                sys.modules.pop("kubemq.core.exceptions", None)


# ==============================================================================
# KubeMQMetrics._should_include_channel cardinality tests
# ==============================================================================


class TestKubeMQMetricsShouldIncludeChannel:
    """Tests for cardinality management in _should_include_channel."""

    def test_below_threshold_includes_channel(self):
        metrics = KubeMQMetrics(meter=_NOOP_METER, max_channel_cardinality=3)
        assert metrics._should_include_channel("ch-1") is True
        assert metrics._should_include_channel("ch-2") is True
        assert metrics._should_include_channel("ch-3") is True

    def test_already_tracked_channel_always_included(self):
        metrics = KubeMQMetrics(meter=_NOOP_METER, max_channel_cardinality=1)
        assert metrics._should_include_channel("ch-1") is True
        assert metrics._should_include_channel("ch-1") is True

    def test_exceeds_threshold_excludes_new_channel(self):
        metrics = KubeMQMetrics(meter=_NOOP_METER, max_channel_cardinality=1)
        assert metrics._should_include_channel("ch-1") is True
        assert metrics._should_include_channel("ch-2") is False

    def test_allowlist_always_included(self):
        metrics = KubeMQMetrics(
            meter=_NOOP_METER,
            max_channel_cardinality=1,
            channel_allowlist={"vip-channel"},
        )
        assert metrics._should_include_channel("ch-1") is True
        assert metrics._should_include_channel("vip-channel") is True
        assert metrics._should_include_channel("ch-2") is False

    def test_cardinality_warning_logged_once(self):
        mock_logger = MagicMock()
        metrics = KubeMQMetrics(
            meter=_NOOP_METER,
            max_channel_cardinality=1,
            logger=mock_logger,
        )
        metrics._should_include_channel("ch-1")
        metrics._should_include_channel("ch-2")
        metrics._should_include_channel("ch-3")

        assert mock_logger.warning.call_count == 1
        assert "cardinality threshold exceeded" in str(mock_logger.warning.call_args)

    def test_no_warning_without_logger(self):
        metrics = KubeMQMetrics(
            meter=_NOOP_METER,
            max_channel_cardinality=1,
            logger=None,
        )
        metrics._should_include_channel("ch-1")
        result = metrics._should_include_channel("ch-2")
        assert result is False
        assert metrics._cardinality_warned is True


# ==============================================================================
# KubeMQInstrumentor misc tests
# ==============================================================================


class TestKubeMQInstrumentorMisc:
    """Misc tests for KubeMQInstrumentor."""

    def test_default_span_kind_mapping(self):
        assert KubeMQInstrumentor._default_span_kind("publish") == "PRODUCER"
        assert KubeMQInstrumentor._default_span_kind("process") == "CONSUMER"
        assert KubeMQInstrumentor._default_span_kind("receive") == "CONSUMER"
        assert KubeMQInstrumentor._default_span_kind("settle") == "CONSUMER"
        assert KubeMQInstrumentor._default_span_kind("send") == "CLIENT"
        assert KubeMQInstrumentor._default_span_kind("unknown-op") == "INTERNAL"

    def test_address_parsing_with_port(self):
        instrumentor = KubeMQInstrumentor("c1", "myhost:9090")
        assert instrumentor._host == "myhost"
        assert instrumentor._port == 9090

    def test_address_parsing_without_port(self):
        instrumentor = KubeMQInstrumentor("c1", "myhost")
        assert instrumentor._host == "myhost"
        assert instrumentor._port == 50000

    def test_add_retry_event_noop_when_no_otel(self):
        instrumentor = KubeMQInstrumentor("c1", "host:50000")
        mock_span = MagicMock()
        with patch("kubemq._internal.telemetry.HAS_OTEL", False):
            instrumentor.add_retry_event(mock_span, 1, 0.5, "timeout")
        mock_span.add_event.assert_not_called()

    def test_add_retry_event_with_otel(self):
        instrumentor = KubeMQInstrumentor("c1", "host:50000")
        mock_span = MagicMock()
        mock_span.is_recording.return_value = True
        with patch("kubemq._internal.telemetry.HAS_OTEL", True):
            instrumentor.add_retry_event(mock_span, 2, 1.5, "transient")
        mock_span.add_event.assert_called_once()


# ==============================================================================
# KubeMQMetrics recording methods (smoke tests with no-op meter)
# ==============================================================================


class TestKubeMQMetricsRecording:
    """Smoke tests for KubeMQMetrics recording methods with no-op meter."""

    def test_record_operation_duration(self):
        metrics = KubeMQMetrics(meter=_NOOP_METER)
        metrics.record_operation_duration(0.5, "publish", "ch-1")

    def test_record_operation_duration_with_error(self):
        metrics = KubeMQMetrics(meter=_NOOP_METER)
        metrics.record_operation_duration(0.5, "publish", "ch-1", error_type="timeout")

    def test_record_sent_message(self):
        metrics = KubeMQMetrics(meter=_NOOP_METER)
        metrics.record_sent_message("publish", "ch-1")

    def test_record_consumed_message(self):
        metrics = KubeMQMetrics(meter=_NOOP_METER)
        metrics.record_consumed_message("process", "ch-1")

    def test_record_connection_opened(self):
        metrics = KubeMQMetrics(meter=_NOOP_METER)
        metrics.record_connection_opened()

    def test_record_connection_closed(self):
        metrics = KubeMQMetrics(meter=_NOOP_METER)
        metrics.record_connection_closed()

    def test_record_reconnection_attempt(self):
        metrics = KubeMQMetrics(meter=_NOOP_METER)
        metrics.record_reconnection_attempt()

    def test_record_retry_attempt(self):
        metrics = KubeMQMetrics(meter=_NOOP_METER)
        metrics.record_retry_attempt("publish", "timeout")

    def test_record_retry_exhausted(self):
        metrics = KubeMQMetrics(meter=_NOOP_METER)
        metrics.record_retry_exhausted("publish", "timeout")


# ==============================================================================
# Additional coverage tests
# ==============================================================================


class TestErrorCodeToErrorTypeNonEnum:
    """Test error_code_to_error_type when code is not an ErrorCode enum instance."""

    def test_non_enum_int_returns_fatal(self):
        assert error_code_to_error_type(42) == "fatal"

    def test_none_returns_fatal(self):
        assert error_code_to_error_type(None) == "fatal"

    def test_random_object_returns_fatal(self):
        assert error_code_to_error_type(object()) == "fatal"


class TestSerializeSpanToBytes:
    """Tests for serialize_span_to_bytes."""

    def test_returns_empty_when_no_otel(self):
        from kubemq._internal.telemetry import serialize_span_to_bytes

        with patch("kubemq._internal.telemetry.HAS_OTEL", False):
            result = serialize_span_to_bytes()
            assert result == b""

    @pytest.mark.skipif(not HAS_OTEL, reason="OTel not installed")
    def test_returns_empty_when_carrier_empty_after_inject(self):
        from kubemq._internal.telemetry import serialize_span_to_bytes

        with patch("kubemq._internal.telemetry.HAS_OTEL", True):
            with patch("opentelemetry.propagate.inject") as mock_inject:
                mock_inject.side_effect = lambda carrier, context=None: None
                result = serialize_span_to_bytes()
                assert result == b""

    @pytest.mark.skipif(not HAS_OTEL, reason="OTel not installed")
    def test_returns_json_when_carrier_has_data(self):
        import json

        from kubemq._internal.telemetry import serialize_span_to_bytes

        def fake_inject(carrier, context=None):
            carrier["traceparent"] = "00-abc-def-01"

        with patch("kubemq._internal.telemetry.HAS_OTEL", True):
            with patch("opentelemetry.propagate.inject", side_effect=fake_inject):
                result = serialize_span_to_bytes()
                parsed = json.loads(result)
                assert parsed["traceparent"] == "00-abc-def-01"


class TestDeserializeSpanFromBytes:
    """Tests for deserialize_span_from_bytes."""

    def test_returns_none_when_no_otel(self):
        from kubemq._internal.telemetry import deserialize_span_from_bytes

        with patch("kubemq._internal.telemetry.HAS_OTEL", False):
            result = deserialize_span_from_bytes(b'{"traceparent": "value"}')
            assert result is None

    def test_returns_none_when_data_empty(self):
        from kubemq._internal.telemetry import deserialize_span_from_bytes

        result = deserialize_span_from_bytes(b"")
        assert result is None

    @pytest.mark.skipif(not HAS_OTEL, reason="OTel not installed")
    def test_returns_none_on_bad_json(self):
        from kubemq._internal.telemetry import deserialize_span_from_bytes

        with patch("kubemq._internal.telemetry.HAS_OTEL", True):
            result = deserialize_span_from_bytes(b"not-json")
            assert result is None

    @pytest.mark.skipif(not HAS_OTEL, reason="OTel not installed")
    def test_returns_context_on_valid_json(self):
        from kubemq._internal.telemetry import deserialize_span_from_bytes

        with patch("kubemq._internal.telemetry.HAS_OTEL", True):
            with patch("opentelemetry.propagate.extract") as mock_extract:
                mock_ctx = MagicMock()
                mock_extract.return_value = mock_ctx
                result = deserialize_span_from_bytes(b'{"traceparent": "val"}')
                assert result is mock_ctx
                mock_extract.assert_called_once()


class TestKubeMQInstrumentorStartSpan:
    """Test KubeMQInstrumentor.start_span with default kind."""

    def test_start_span_returns_noop_without_otel(self):
        instrumentor = KubeMQInstrumentor("c1", "host:50000")
        with patch("kubemq._internal.telemetry.HAS_OTEL", False):
            span = instrumentor.start_span("publish", "my-channel")
            assert span is _NOOP_SPAN

    def test_start_span_with_explicit_kind(self):
        instrumentor = KubeMQInstrumentor("c1", "host:50000")
        with patch("kubemq._internal.telemetry.HAS_OTEL", False):
            span = instrumentor.start_span("publish", "my-channel", kind="PRODUCER")
            assert span is _NOOP_SPAN


# ==============================================================================
# Mock-based OTel tests (don't require OTel installed)
# ==============================================================================


class TestResolveSpanKindMocked:
    """Tests for _resolve_span_kind using mocked OTel modules."""

    def test_string_resolves_to_mocked_kind(self):
        import enum

        class FakeSpanKind(enum.Enum):
            PRODUCER = "PRODUCER_ENUM"
            CONSUMER = "CONSUMER_ENUM"
            CLIENT = "CLIENT_ENUM"
            SERVER = "SERVER_ENUM"
            INTERNAL = "INTERNAL_ENUM"

        mock_trace = MagicMock()
        mock_trace.SpanKind = FakeSpanKind

        with (
            patch("kubemq._internal.telemetry.HAS_OTEL", True),
            patch.dict("sys.modules", {"opentelemetry.trace": mock_trace}),
        ):
            result = KubeMQInstrumentor._resolve_span_kind("PRODUCER")
            assert result == FakeSpanKind.PRODUCER

    def test_enum_passthrough_mocked(self):
        import enum

        class FakeSpanKind(enum.Enum):
            PRODUCER = "P"

        mock_trace = MagicMock()
        mock_trace.SpanKind = FakeSpanKind

        with (
            patch("kubemq._internal.telemetry.HAS_OTEL", True),
            patch.dict("sys.modules", {"opentelemetry.trace": mock_trace}),
        ):
            result = KubeMQInstrumentor._resolve_span_kind(FakeSpanKind.PRODUCER)
            assert result is FakeSpanKind.PRODUCER

    def test_unknown_defaults_to_internal_mocked(self):
        import enum

        class FakeSpanKind(enum.Enum):
            PRODUCER = "PRODUCER_ENUM"
            CONSUMER = "CONSUMER_ENUM"
            CLIENT = "CLIENT_ENUM"
            SERVER = "SERVER_ENUM"
            INTERNAL = "INTERNAL_ENUM"

        mock_trace = MagicMock()
        mock_trace.SpanKind = FakeSpanKind

        with (
            patch("kubemq._internal.telemetry.HAS_OTEL", True),
            patch.dict("sys.modules", {"opentelemetry.trace": mock_trace}),
        ):
            result = KubeMQInstrumentor._resolve_span_kind("UNKNOWN_KIND")
            assert result == FakeSpanKind.INTERNAL


class TestKubeMQTagsCarrierMocked:
    """Tests for inject/extract using mocked OTel propagators."""

    def test_inject_calls_otel_inject(self):
        mock_inject = MagicMock()
        mock_propagate = MagicMock()
        mock_propagate.inject = mock_inject

        with (
            patch("kubemq._internal.telemetry.HAS_OTEL", True),
            patch.dict("sys.modules", {"opentelemetry.propagate": mock_propagate}),
        ):
            carrier = KubeMQTagsCarrier({"key": "val"})
            carrier.inject()
            mock_inject.assert_called_once()

    def test_extract_calls_otel_extract(self):
        mock_ctx = MagicMock()
        mock_extract = MagicMock(return_value=mock_ctx)
        mock_propagate = MagicMock()
        mock_propagate.extract = mock_extract

        with (
            patch("kubemq._internal.telemetry.HAS_OTEL", True),
            patch.dict("sys.modules", {"opentelemetry.propagate": mock_propagate}),
        ):
            carrier = KubeMQTagsCarrier({"traceparent": "00-abc"})
            result = carrier.extract()
            assert result is mock_ctx
            mock_extract.assert_called_once()


class TestSerializeSpanToBytesMocked:
    """Tests for serialize_span_to_bytes with mocked OTel."""

    def test_returns_empty_when_carrier_empty(self):
        mock_inject = MagicMock()
        mock_propagate = MagicMock()
        mock_propagate.inject = mock_inject

        with (
            patch("kubemq._internal.telemetry.HAS_OTEL", True),
            patch.dict("sys.modules", {"opentelemetry.propagate": mock_propagate}),
        ):
            from kubemq._internal.telemetry import serialize_span_to_bytes

            result = serialize_span_to_bytes()
            assert result == b""

    def test_returns_json_when_carrier_populated(self):
        import json

        def fake_inject(carrier, context=None):
            carrier["traceparent"] = "00-abc-def-01"

        mock_propagate = MagicMock()
        mock_propagate.inject = fake_inject

        with (
            patch("kubemq._internal.telemetry.HAS_OTEL", True),
            patch.dict("sys.modules", {"opentelemetry.propagate": mock_propagate}),
        ):
            from kubemq._internal.telemetry import serialize_span_to_bytes

            result = serialize_span_to_bytes()
            parsed = json.loads(result)
            assert parsed["traceparent"] == "00-abc-def-01"


class TestDeserializeSpanFromBytesMocked:
    """Tests for deserialize_span_from_bytes with mocked OTel."""

    def test_returns_none_on_bad_json(self):
        mock_propagate = MagicMock()

        with (
            patch("kubemq._internal.telemetry.HAS_OTEL", True),
            patch.dict("sys.modules", {"opentelemetry.propagate": mock_propagate}),
        ):
            from kubemq._internal.telemetry import deserialize_span_from_bytes

            result = deserialize_span_from_bytes(b"not-json{{{")
            assert result is None

    def test_returns_context_on_valid_json(self):
        mock_ctx = MagicMock()
        mock_extract = MagicMock(return_value=mock_ctx)
        mock_propagate = MagicMock()
        mock_propagate.extract = mock_extract

        with (
            patch("kubemq._internal.telemetry.HAS_OTEL", True),
            patch.dict("sys.modules", {"opentelemetry.propagate": mock_propagate}),
        ):
            from kubemq._internal.telemetry import deserialize_span_from_bytes

            result = deserialize_span_from_bytes(b'{"traceparent": "val"}')
            assert result is mock_ctx


class TestCreateLinkFromContextMocked:
    """Tests for create_link_from_context with mocked OTel."""

    def test_returns_link_when_valid_span(self):
        mock_span_ctx = MagicMock()
        mock_span_ctx.is_valid = True
        mock_span = MagicMock()
        mock_span.get_span_context.return_value = mock_span_ctx
        mock_link = MagicMock()

        mock_otel_trace = MagicMock()
        mock_otel_trace.get_current_span.return_value = mock_span
        mock_otel_trace.Link = MagicMock(return_value=mock_link)

        with (
            patch("kubemq._internal.telemetry.HAS_OTEL", True),
            patch.dict(
                "sys.modules",
                {
                    "opentelemetry": MagicMock(trace=mock_otel_trace),
                    "opentelemetry.trace": mock_otel_trace,
                },
            ),
        ):
            from kubemq._internal.telemetry import create_link_from_context

            ctx = MagicMock()
            result = create_link_from_context(ctx)
            assert result is mock_link

    def test_returns_none_when_span_invalid(self):
        mock_span_ctx = MagicMock()
        mock_span_ctx.is_valid = False
        mock_span = MagicMock()
        mock_span.get_span_context.return_value = mock_span_ctx

        mock_otel_trace = MagicMock()
        mock_otel_trace.get_current_span.return_value = mock_span

        with (
            patch("kubemq._internal.telemetry.HAS_OTEL", True),
            patch.dict(
                "sys.modules",
                {
                    "opentelemetry": MagicMock(trace=mock_otel_trace),
                    "opentelemetry.trace": mock_otel_trace,
                },
            ),
        ):
            from kubemq._internal.telemetry import create_link_from_context

            result = create_link_from_context(MagicMock())
            assert result is None


class TestAddRetryEventMocked:
    """Tests for add_retry_event when span is recording."""

    def test_adds_event_when_recording(self):
        instrumentor = KubeMQInstrumentor("c1", "host:50000")
        mock_span = MagicMock()
        mock_span.is_recording.return_value = True

        with patch("kubemq._internal.telemetry.HAS_OTEL", True):
            instrumentor.add_retry_event(
                mock_span, attempt=2, delay_seconds=1.5, error_type="timeout"
            )

        mock_span.add_event.assert_called_once()

    def test_noop_when_not_recording(self):
        instrumentor = KubeMQInstrumentor("c1", "host:50000")
        mock_span = MagicMock()
        mock_span.is_recording.return_value = False

        with patch("kubemq._internal.telemetry.HAS_OTEL", True):
            instrumentor.add_retry_event(
                mock_span, attempt=1, delay_seconds=0.5, error_type="timeout"
            )

        mock_span.add_event.assert_not_called()
