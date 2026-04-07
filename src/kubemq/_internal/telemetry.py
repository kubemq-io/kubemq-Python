"""OpenTelemetry integration with graceful degradation.

When opentelemetry-api is not installed, all telemetry operations
become no-ops with near-zero overhead. The HAS_OTEL flag controls
runtime behavior; TYPE_CHECKING guards allow type annotations to
work without the package installed.
"""

from __future__ import annotations

import threading
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

# ── Runtime feature detection ──────────────────────────────────────
try:
    from opentelemetry import (
        context as otel_context,  # noqa: F401
        metrics as otel_metrics,
        trace as otel_trace,
    )
    from opentelemetry.propagators import textmap as otel_textmap  # noqa: F401

    HAS_OTEL = True
except ImportError:
    HAS_OTEL = False

# ── Type-only imports (never executed at runtime) ──────────────────
if TYPE_CHECKING:
    from opentelemetry.context import Context  # noqa: F401
    from opentelemetry.metrics import (  # noqa: F401
        Counter,
        Histogram,
        Meter,
        MeterProvider,
        UpDownCounter,
    )
    from opentelemetry.trace import (  # noqa: F401
        Span,
        SpanKind,
        StatusCode,
        Tracer,
        TracerProvider,
    )


# ── No-op stubs (used when HAS_OTEL is False) ─────────────────────
# These implement the same call interface as OTel classes so that
# instrumentation code can call span.set_attribute() etc. without
# guards. The stubs do nothing and return instantly.


class _NoOpSpan:
    """No-op span that silently ignores all operations."""

    __slots__ = ()

    def set_attribute(self, key: str, value: Any) -> None:
        pass

    def set_status(self, status: Any, description: str | None = None) -> None:
        pass

    def add_event(
        self,
        name: str,
        attributes: dict[str, Any] | None = None,
        timestamp: int | None = None,
    ) -> None:
        pass

    def record_exception(
        self,
        exception: BaseException,
        attributes: dict[str, Any] | None = None,
        timestamp: int | None = None,
        escaped: bool = False,
    ) -> None:
        pass

    def is_recording(self) -> bool:
        return False

    def end(self, end_time: int | None = None) -> None:
        pass

    def get_span_context(self) -> _NoOpSpanContext:
        return _NOOP_SPAN_CONTEXT

    def __enter__(self) -> _NoOpSpan:
        return self

    def __exit__(self, *args: Any) -> None:
        pass


class _NoOpSpanContext:
    """No-op span context with invalid trace/span IDs.

    Class-level attributes with empty __slots__: these are read-only
    constants shared across all instances. __slots__ = () prevents
    accidental instance attribute creation.
    """

    __slots__ = ()

    trace_id: int = 0
    span_id: int = 0
    is_valid: bool = False
    trace_flags: int = 0
    trace_state: Any = None


class _NoOpTracer:
    """No-op tracer that returns _NoOpSpan for all operations."""

    __slots__ = ()

    def start_span(
        self,
        name: str,
        context: Any = None,
        kind: Any = None,
        attributes: dict[str, Any] | None = None,
        links: Sequence[Any] | None = None,
        start_time: int | None = None,
        record_exception: bool = True,
        set_status_on_exception: bool = True,
    ) -> _NoOpSpan:
        return _NOOP_SPAN

    def start_as_current_span(
        self,
        name: str,
        context: Any = None,
        kind: Any = None,
        attributes: dict[str, Any] | None = None,
        links: Sequence[Any] | None = None,
        start_time: int | None = None,
        record_exception: bool = True,
        set_status_on_exception: bool = True,
        end_on_exit: bool = True,
    ) -> _NoOpSpan:
        return _NOOP_SPAN


class _NoOpCounter:
    """No-op counter metric."""

    __slots__ = ()

    def add(self, amount: int | float, attributes: dict[str, Any] | None = None) -> None:
        pass


class _NoOpHistogram:
    """No-op histogram metric."""

    __slots__ = ()

    def record(self, amount: int | float, attributes: dict[str, Any] | None = None) -> None:
        pass


class _NoOpUpDownCounter:
    """No-op up-down counter metric."""

    __slots__ = ()

    def add(self, amount: int | float, attributes: dict[str, Any] | None = None) -> None:
        pass


class _NoOpMeter:
    """No-op meter that returns no-op instruments."""

    __slots__ = ()

    def create_counter(self, name: str, unit: str = "", description: str = "") -> _NoOpCounter:
        return _NOOP_COUNTER

    def create_histogram(self, name: str, unit: str = "", description: str = "") -> _NoOpHistogram:
        return _NOOP_HISTOGRAM

    def create_up_down_counter(
        self, name: str, unit: str = "", description: str = ""
    ) -> _NoOpUpDownCounter:
        return _NOOP_UP_DOWN_COUNTER


# Singleton no-op instances (avoid allocation on every call)
_NOOP_SPAN_CONTEXT = _NoOpSpanContext()
_NOOP_SPAN = _NoOpSpan()
_NOOP_TRACER = _NoOpTracer()
_NOOP_COUNTER = _NoOpCounter()
_NOOP_HISTOGRAM = _NoOpHistogram()
_NOOP_UP_DOWN_COUNTER = _NoOpUpDownCounter()
_NOOP_METER = _NoOpMeter()


def get_tracer(
    tracer_provider: Any | None = None,
) -> Any:
    """Get a tracer instance, returning no-op if OTel is not available.

    Args:
        tracer_provider: Optional TracerProvider. Falls back to global if None.

    Returns:
        OTel Tracer when available, _NoOpTracer otherwise.
    """
    if not HAS_OTEL:
        return _NOOP_TRACER

    from kubemq import __version__

    provider = tracer_provider or otel_trace.get_tracer_provider()
    return provider.get_tracer("kubemq", __version__)


def get_meter(
    meter_provider: Any | None = None,
) -> Any:
    """Get a meter instance, returning no-op if OTel is not available.

    Args:
        meter_provider: Optional MeterProvider. Falls back to global if None.

    Returns:
        OTel Meter when available, _NoOpMeter otherwise.
    """
    if not HAS_OTEL:
        return _NOOP_METER

    from kubemq import __version__

    provider = meter_provider or otel_metrics.get_meter_provider()
    return provider.get_meter("kubemq", __version__)


# ── OBS-1: KubeMQInstrumentor ─────────────────────────────────────


class KubeMQInstrumentor:
    """Creates OTel spans and records metrics for KubeMQ operations.

    Handles both real OTel and no-op paths transparently.
    Instantiated once per client and shared across all operations.

    Args:
        client_id: The KubeMQ client ID.
        address: Server address (host:port).
        tracer_provider: Optional TracerProvider override.
        meter_provider: Optional MeterProvider override.
        logger: Logger Protocol instance for internal diagnostics.
    """

    __slots__ = (
        "_client_id",
        "_host",
        "_port",
        "_tracer",
        "_meter",
        "_logger",
        "_metrics",
    )

    def __init__(
        self,
        client_id: str,
        address: str,
        tracer_provider: Any = None,
        meter_provider: Any = None,
        logger: Any = None,
    ) -> None:
        self._client_id = client_id

        # Parse host:port — IPv6 bracketed notation per RFC 2732
        parts = address.rsplit(":", 1)
        self._host = parts[0]
        self._port = int(parts[1]) if len(parts) > 1 else 50000

        self._tracer = get_tracer(tracer_provider)
        self._meter = get_meter(meter_provider)
        self._logger = logger
        self._metrics: Any = None

    def start_span(
        self,
        operation: str,
        channel: str,
        kind: Any = None,
        attributes: dict[str, Any] | None = None,
        links: Any = None,
    ) -> Any:
        """Start a new span for a messaging operation.

        Args:
            operation: Operation name (publish, process, receive, settle, send).
            channel: Target channel/queue name.
            kind: OTel SpanKind. Defaults based on operation if None.
            attributes: Additional span attributes.
            links: Span links for producer-consumer correlation.

        Returns:
            Span (real or no-op) — usable as context manager.
        """
        from kubemq._internal.semconv import (
            MESSAGING_CLIENT_ID,
            MESSAGING_DESTINATION_NAME,
            MESSAGING_OPERATION_NAME,
            MESSAGING_OPERATION_TYPE,
            MESSAGING_SYSTEM,
            MESSAGING_SYSTEM_VALUE,
            SERVER_ADDRESS,
            SERVER_PORT,
        )

        span_name = f"{operation} {channel}"

        if kind is None:
            kind = self._default_span_kind(operation)

        base_attributes = {
            MESSAGING_SYSTEM: MESSAGING_SYSTEM_VALUE,
            MESSAGING_OPERATION_NAME: operation,
            MESSAGING_OPERATION_TYPE: operation,
            MESSAGING_DESTINATION_NAME: channel,
            MESSAGING_CLIENT_ID: self._client_id,
            SERVER_ADDRESS: self._host,
            SERVER_PORT: self._port,
        }
        if attributes:
            base_attributes.update(attributes)

        span_kind = self._resolve_span_kind(kind) if HAS_OTEL else kind
        return self._tracer.start_as_current_span(
            name=span_name,
            kind=span_kind,
            attributes=base_attributes,
            links=links,
        )

    def record_error(self, span: Any, error: Exception, error_type: str = "") -> None:
        """Record an error on a span.

        Args:
            span: The span to record the error on.
            error: The exception that occurred.
            error_type: The error.type attribute value.
        """
        if not HAS_OTEL:
            return

        from opentelemetry.trace import StatusCode

        if span.is_recording():
            span.set_status(StatusCode.ERROR, str(error))
            span.record_exception(error)
            if error_type:
                span.set_attribute("error.type", error_type)

    def add_retry_event(
        self,
        span: Any,
        attempt: int,
        delay_seconds: float,
        error_type: str,
    ) -> None:
        """Add a retry event to a span.

        Args:
            span: The span to add the event to.
            attempt: Current retry attempt number (1-based).
            delay_seconds: Delay before this retry attempt.
            error_type: The error that triggered the retry.
        """
        if not HAS_OTEL:
            return

        from kubemq._internal.semconv import (
            ERROR_TYPE,
            RETRY_ATTEMPT,
            RETRY_DELAY_SECONDS,
            RETRY_EVENT_NAME,
        )

        if span.is_recording():
            span.add_event(
                RETRY_EVENT_NAME,
                attributes={
                    RETRY_ATTEMPT: attempt,
                    RETRY_DELAY_SECONDS: delay_seconds,
                    ERROR_TYPE: error_type,
                },
            )

    @staticmethod
    def _default_span_kind(operation: str) -> str:
        """Map operation to default SpanKind string."""
        _KIND_MAP = {
            "publish": "PRODUCER",
            "process": "CONSUMER",
            "receive": "CONSUMER",
            "settle": "CONSUMER",
            "send": "CLIENT",
        }
        return _KIND_MAP.get(operation, "INTERNAL")

    @staticmethod
    def _resolve_span_kind(kind: Any) -> Any:
        """Resolve a SpanKind value — handles string or enum input."""
        if not HAS_OTEL:
            return None

        from opentelemetry.trace import SpanKind

        if isinstance(kind, SpanKind):
            return kind

        _STR_TO_KIND = {
            "PRODUCER": SpanKind.PRODUCER,
            "CONSUMER": SpanKind.CONSUMER,
            "CLIENT": SpanKind.CLIENT,
            "SERVER": SpanKind.SERVER,
            "INTERNAL": SpanKind.INTERNAL,
        }
        return _STR_TO_KIND.get(str(kind).upper(), SpanKind.INTERNAL)


# ── OBS-2: W3C Trace Context Propagation ──────────────────────────


class KubeMQTagsCarrier:
    """OTel TextMapCarrier adapter over KubeMQ message tags.

    Wraps a dict[str, str] (message tags) to support OTel
    propagator inject/extract operations for W3C Trace Context.

    When HAS_OTEL is False, all operations are no-ops.

    Usage::

        # Inject (producer side)
        carrier = KubeMQTagsCarrier(message.tags)
        carrier.inject()

        # Extract (consumer side)
        carrier = KubeMQTagsCarrier(message.tags)
        ctx = carrier.extract()
    """

    __slots__ = ("_tags",)

    def __init__(self, tags: dict[str, str]) -> None:
        self._tags = tags

    @property
    def tags(self) -> dict[str, str]:
        """The underlying tags dict (may be mutated by inject)."""
        return self._tags

    def get(self, key: str, default: str | None = None) -> str | None:
        """Get a tag value by key. Used by OTel Getter protocol."""
        val = self._tags.get(key)
        return val if val is not None else default

    def set(self, key: str, value: str) -> None:
        """Set a tag value. Used by OTel Setter protocol."""
        self._tags[key] = value

    def keys(self) -> list[str]:
        """Return all tag keys. Used by OTel Getter protocol."""
        return list(self._tags.keys())

    def inject(self, context: Any = None) -> None:
        """Inject current trace context into tags.

        Injects traceparent and tracestate headers into the tags dict.
        No-op when OTel is not available.

        Args:
            context: Optional OTel Context. Uses current context if None.
        """
        if not HAS_OTEL:
            return
        from opentelemetry.propagate import inject

        inject(carrier=self, context=context)

    def extract(self) -> Any:
        """Extract trace context from tags.

        Returns an OTel Context with the extracted trace info,
        or None when OTel is not available.

        Returns:
            OTel Context or None.
        """
        if not HAS_OTEL:
            return None
        from opentelemetry.propagate import extract

        return extract(carrier=self)


def serialize_span_to_bytes(context: Any = None) -> bytes:
    """Serialize the current OTel span context into bytes for the protobuf Span field.

    Uses W3C Trace Context propagation format (JSON-encoded traceparent/tracestate).
    Returns empty bytes when OTel is not available or no active span exists.
    """
    if not HAS_OTEL:
        return b""
    from opentelemetry.propagate import inject

    carrier: dict[str, str] = {}
    inject(carrier=carrier, context=context)
    if not carrier:
        return b""
    import json

    return json.dumps(carrier).encode("utf-8")


def deserialize_span_from_bytes(data: bytes) -> Any:
    """Deserialize span context from protobuf Span bytes field.

    Returns an OTel Context, or None when OTel is not available or data is empty.
    """
    if not HAS_OTEL or not data:
        return None
    import json

    from opentelemetry.propagate import extract

    try:
        carrier = json.loads(data.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    return extract(carrier=carrier)


def create_link_from_context(ctx: Any) -> Any:
    """Create a span Link from an extracted OTel Context.

    Returns None if OTel is not available or context has no valid span.

    Args:
        ctx: OTel Context (from KubeMQTagsCarrier.extract()).

    Returns:
        OTel Link or None.
    """
    if not HAS_OTEL or ctx is None:
        return None

    from opentelemetry import trace as _otel_trace
    from opentelemetry.trace import Link

    span_ctx = _otel_trace.get_current_span(ctx).get_span_context()
    if span_ctx and span_ctx.is_valid:
        return Link(span_ctx)
    return None


# ── OBS-3: KubeMQMetrics ──────────────────────────────────────────


class KubeMQMetrics:
    """OTel metrics instruments for KubeMQ operations.

    Creates and manages all required metric instruments. Provides
    cardinality management for messaging.destination.name attribute.

    Args:
        meter: OTel Meter or _NoOpMeter.
        max_channel_cardinality: Maximum unique channel names tracked.
            When exceeded, messaging.destination.name is omitted from
            new metric series and a WARNING log is emitted.
        channel_allowlist: Explicit set of channel names always included
            regardless of cardinality threshold.
        logger: Logger Protocol instance for cardinality warnings.
    """

    __slots__ = (
        "_meter",
        "_max_cardinality",
        "_channel_allowlist",
        "_channel_names",
        "_cardinality_warned",
        "_lock",
        "_logger",
        "_operation_duration",
        "_sent_messages",
        "_consumed_messages",
        "_connection_count",
        "_reconnections",
        "_retry_attempts",
        "_retry_exhausted",
    )

    def __init__(
        self,
        meter: Any,
        max_channel_cardinality: int = 100,
        channel_allowlist: set[str] | None = None,
        logger: Any = None,
    ) -> None:
        self._meter = meter
        self._max_cardinality = max_channel_cardinality
        self._channel_allowlist = channel_allowlist or set()
        self._channel_names: set[str] = set()
        self._cardinality_warned = False
        self._lock = threading.Lock()
        self._logger = logger

        from kubemq._internal.semconv import (
            METRIC_CONNECTION_COUNT,
            METRIC_CONSUMED_MESSAGES,
            METRIC_OPERATION_DURATION,
            METRIC_RECONNECTIONS,
            METRIC_RETRY_ATTEMPTS,
            METRIC_RETRY_EXHAUSTED,
            METRIC_SENT_MESSAGES,
        )

        self._operation_duration = meter.create_histogram(
            name=METRIC_OPERATION_DURATION,
            unit="s",
            description="Duration of messaging operations",
        )
        self._sent_messages = meter.create_counter(
            name=METRIC_SENT_MESSAGES,
            unit="{message}",
            description="Total messages sent",
        )
        self._consumed_messages = meter.create_counter(
            name=METRIC_CONSUMED_MESSAGES,
            unit="{message}",
            description="Total messages consumed",
        )
        self._connection_count = meter.create_up_down_counter(
            name=METRIC_CONNECTION_COUNT,
            unit="{connection}",
            description="Active connections",
        )
        self._reconnections = meter.create_counter(
            name=METRIC_RECONNECTIONS,
            unit="{attempt}",
            description="Reconnection attempts",
        )
        self._retry_attempts = meter.create_counter(
            name=METRIC_RETRY_ATTEMPTS,
            unit="{attempt}",
            description="Retry attempts",
        )
        self._retry_exhausted = meter.create_counter(
            name=METRIC_RETRY_EXHAUSTED,
            unit="{attempt}",
            description="Retries exhausted",
        )

    def record_operation_duration(
        self,
        duration_seconds: float,
        operation: str,
        channel: str,
        error_type: str | None = None,
    ) -> None:
        """Record the duration of a messaging operation.

        Args:
            duration_seconds: Operation duration in seconds.
            operation: Operation name (publish, process, receive, etc.).
            channel: Target channel/queue name.
            error_type: Error type if operation failed, None on success.
        """
        attrs = self._base_attributes(operation, channel)
        if error_type:
            from kubemq._internal.semconv import ERROR_TYPE

            attrs[ERROR_TYPE] = error_type
        self._operation_duration.record(duration_seconds, attributes=attrs)

    def record_sent_message(self, operation: str, channel: str) -> None:
        """Increment the sent messages counter."""
        self._sent_messages.add(1, attributes=self._base_attributes(operation, channel))

    def record_consumed_message(self, operation: str, channel: str) -> None:
        """Increment the consumed messages counter."""
        self._consumed_messages.add(1, attributes=self._base_attributes(operation, channel))

    def record_connection_opened(self) -> None:
        """Increment the active connection count."""
        from kubemq._internal.semconv import MESSAGING_SYSTEM, MESSAGING_SYSTEM_VALUE

        self._connection_count.add(1, attributes={MESSAGING_SYSTEM: MESSAGING_SYSTEM_VALUE})

    def record_connection_closed(self) -> None:
        """Decrement the active connection count."""
        from kubemq._internal.semconv import MESSAGING_SYSTEM, MESSAGING_SYSTEM_VALUE

        self._connection_count.add(-1, attributes={MESSAGING_SYSTEM: MESSAGING_SYSTEM_VALUE})

    def record_reconnection_attempt(self) -> None:
        """Increment the reconnection counter.

        Increments on each entry to RECONNECTING state — not on
        successful reconnection.
        """
        from kubemq._internal.semconv import MESSAGING_SYSTEM, MESSAGING_SYSTEM_VALUE

        self._reconnections.add(1, attributes={MESSAGING_SYSTEM: MESSAGING_SYSTEM_VALUE})

    def record_retry_attempt(self, operation: str, error_type: str) -> None:
        """Increment the retry attempts counter.

        Args:
            operation: The operation being retried.
            error_type: The error.type that triggered the retry.
        """
        from kubemq._internal.semconv import (
            ERROR_TYPE,
            MESSAGING_OPERATION_NAME,
            MESSAGING_SYSTEM,
            MESSAGING_SYSTEM_VALUE,
        )

        self._retry_attempts.add(
            1,
            attributes={
                MESSAGING_SYSTEM: MESSAGING_SYSTEM_VALUE,
                MESSAGING_OPERATION_NAME: operation,
                ERROR_TYPE: error_type,
            },
        )

    def record_retry_exhausted(self, operation: str, error_type: str) -> None:
        """Increment the retry exhausted counter.

        Args:
            operation: The operation that exhausted retries.
            error_type: The error.type that exhausted retries.
        """
        from kubemq._internal.semconv import (
            ERROR_TYPE,
            MESSAGING_OPERATION_NAME,
            MESSAGING_SYSTEM,
            MESSAGING_SYSTEM_VALUE,
        )

        self._retry_exhausted.add(
            1,
            attributes={
                MESSAGING_SYSTEM: MESSAGING_SYSTEM_VALUE,
                MESSAGING_OPERATION_NAME: operation,
                ERROR_TYPE: error_type,
            },
        )

    def _base_attributes(self, operation: str, channel: str) -> dict[str, Any]:
        """Build base metric attributes with cardinality management."""
        from kubemq._internal.semconv import (
            MESSAGING_DESTINATION_NAME,
            MESSAGING_OPERATION_NAME,
            MESSAGING_SYSTEM,
            MESSAGING_SYSTEM_VALUE,
        )

        attrs: dict[str, Any] = {
            MESSAGING_SYSTEM: MESSAGING_SYSTEM_VALUE,
            MESSAGING_OPERATION_NAME: operation,
        }

        if self._should_include_channel(channel):
            attrs[MESSAGING_DESTINATION_NAME] = channel

        return attrs

    def _should_include_channel(self, channel: str) -> bool:
        """Check if channel name should be included in metric attributes.

        Implements cardinality management:
        1. Always include if in allowlist
        2. Always include if already tracked
        3. Include if below threshold, adding to tracked set
        4. Omit and warn if threshold exceeded

        Uses ``threading.Lock`` for thread safety (not ``asyncio.Lock``)
        because this method is called from both sync and async client
        operations.
        """
        if channel in self._channel_allowlist:
            return True

        with self._lock:
            if channel in self._channel_names:
                return True
            if len(self._channel_names) < self._max_cardinality:
                self._channel_names.add(channel)
                return True

            if not self._cardinality_warned:
                self._cardinality_warned = True
                if self._logger:
                    self._logger.warning(
                        "metric cardinality threshold exceeded, "
                        "omitting messaging.destination.name for new channels",
                        max_cardinality=self._max_cardinality,
                        tracked_channels=len(self._channel_names),
                    )
            return False


def error_code_to_error_type(code: Any) -> str:
    """Map ErrorCode enum to error.type metric attribute value."""
    try:
        from kubemq.core.exceptions import ErrorCode
    except ImportError:
        return "unknown"

    _MAPPING = {
        ErrorCode.CONNECTION_TIMEOUT: "timeout",
        ErrorCode.UNAVAILABLE: "transient",
        ErrorCode.CONNECTION_NOT_READY: "transient",
        ErrorCode.AUTH_FAILED: "authentication",
        ErrorCode.PERMISSION_DENIED: "authorization",
        ErrorCode.VALIDATION_ERROR: "validation",
        ErrorCode.ALREADY_EXISTS: "validation",
        ErrorCode.OUT_OF_RANGE: "validation",
        ErrorCode.NOT_FOUND: "not_found",
        ErrorCode.RESOURCE_EXHAUSTED: "throttling",
        ErrorCode.ABORTED: "transient",
        ErrorCode.INTERNAL: "fatal",
        ErrorCode.UNKNOWN: "fatal",
        ErrorCode.UNIMPLEMENTED: "fatal",
        ErrorCode.DATA_LOSS: "fatal",
        ErrorCode.CANCELLED: "cancellation",
        ErrorCode.BUFFER_FULL: "backpressure",
        ErrorCode.STREAM_BROKEN: "transient",
        ErrorCode.CLIENT_CLOSED: "cancellation",
        ErrorCode.CONFIGURATION_ERROR: "validation",
    }
    if isinstance(code, ErrorCode):
        return _MAPPING.get(code, "fatal")
    return "fatal"
