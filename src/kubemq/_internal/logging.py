"""Logging implementations for KubeMQ SDK.

Provides NoOpLogger (default) and StdLibLoggerAdapter (bridge to stdlib logging).
"""

from __future__ import annotations

import logging as stdlib_logging
from typing import Any


class NoOpLogger:
    """Default logger that discards all messages.

    Used when no logger is configured — ensures zero logging overhead.
    Satisfies the Logger Protocol defined in core/types.py.
    """

    __slots__ = ()

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        pass

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        pass

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        pass

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        pass


class StdLibLoggerAdapter:
    """Bridge from the Logger Protocol to Python stdlib logging.

    Formats structured kwargs as key=value pairs appended to the message.
    When OTel is active (HAS_OTEL), injects trace_id and span_id from
    the current span context.

    Example output::

        INFO kubemq.PubSubClient connection established client_id=abc address=host:50000

    Args:
        name: Logger name passed to logging.getLogger(). Defaults to "kubemq".
        level: Minimum log level. Defaults to logging.DEBUG (filter at handler level).
    """

    __slots__ = ("_logger", "_has_otel")

    def __init__(self, name: str = "kubemq", level: int = stdlib_logging.DEBUG) -> None:
        self._logger = stdlib_logging.getLogger(name)
        self._logger.setLevel(level)
        from kubemq._internal.telemetry import HAS_OTEL

        self._has_otel = HAS_OTEL

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log(stdlib_logging.DEBUG, msg, args, kwargs)

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log(stdlib_logging.INFO, msg, args, kwargs)

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log(stdlib_logging.WARNING, msg, args, kwargs)

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log(stdlib_logging.ERROR, msg, args, kwargs)

    def _log(self, level: int, msg: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> None:
        if self._logger.isEnabledFor(level):
            formatted_msg = msg % args if args else msg
            self._logger.log(level, self._format(formatted_msg, kwargs))

    def _format(self, msg: str, kwargs: dict[str, Any]) -> str:
        """Format message with structured key=value pairs and OTel context."""
        enriched = self._inject_trace_context(kwargs)
        if not enriched:
            return msg
        pairs = " ".join(f"{k}={v}" for k, v in enriched.items())
        return f"{msg} {pairs}"

    def _inject_trace_context(self, kwargs: dict[str, Any]) -> dict[str, Any]:
        """Inject trace_id and span_id from current OTel span, if active."""
        if not self._has_otel:
            return kwargs

        from opentelemetry import trace as otel_trace

        span = otel_trace.get_current_span()
        ctx = span.get_span_context()
        if ctx and ctx.is_valid:
            kwargs = {
                "trace_id": format(ctx.trace_id, "032x"),
                "span_id": format(ctx.span_id, "016x"),
                **kwargs,
            }
        return kwargs


# Singleton no-op logger instance
NOOP_LOGGER = NoOpLogger()
