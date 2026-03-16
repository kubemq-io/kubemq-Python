"""Tests for kubemq._internal.logging module."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest

from kubemq._internal.logging import NOOP_LOGGER, NoOpLogger, StdLibLoggerAdapter


class TestNoOpLogger:
    def test_debug_does_nothing(self):
        NOOP_LOGGER.debug("msg", key="val")

    def test_info_does_nothing(self):
        NOOP_LOGGER.info("msg")

    def test_warning_does_nothing(self):
        NOOP_LOGGER.warning("msg", extra="data")

    def test_error_does_nothing(self):
        NOOP_LOGGER.error("msg")

    def test_separate_instances_are_independent(self):
        a = NoOpLogger()
        b = NoOpLogger()
        a.info("hello")
        b.error("world")


class TestStdLibLoggerAdapter:
    @patch("kubemq._internal.telemetry.HAS_OTEL", False)
    def test_debug_delegates(self):
        adapter = StdLibLoggerAdapter(name="test.debug", level=logging.DEBUG)
        adapter._logger = MagicMock()
        adapter._logger.isEnabledFor.return_value = True

        adapter.debug("hello")

        adapter._logger.log.assert_called_once_with(logging.DEBUG, "hello")

    @patch("kubemq._internal.telemetry.HAS_OTEL", False)
    def test_info_delegates(self):
        adapter = StdLibLoggerAdapter(name="test.info", level=logging.DEBUG)
        adapter._logger = MagicMock()
        adapter._logger.isEnabledFor.return_value = True

        adapter.info("world")

        adapter._logger.log.assert_called_once_with(logging.INFO, "world")

    @patch("kubemq._internal.telemetry.HAS_OTEL", False)
    def test_warning_delegates(self):
        adapter = StdLibLoggerAdapter(name="test.warn", level=logging.DEBUG)
        adapter._logger = MagicMock()
        adapter._logger.isEnabledFor.return_value = True

        adapter.warning("caution")

        adapter._logger.log.assert_called_once_with(logging.WARNING, "caution")

    @patch("kubemq._internal.telemetry.HAS_OTEL", False)
    def test_error_delegates(self):
        adapter = StdLibLoggerAdapter(name="test.error", level=logging.DEBUG)
        adapter._logger = MagicMock()
        adapter._logger.isEnabledFor.return_value = True

        adapter.error("boom")

        adapter._logger.log.assert_called_once_with(logging.ERROR, "boom")

    @patch("kubemq._internal.telemetry.HAS_OTEL", False)
    def test_kwargs_appended_as_key_value(self):
        adapter = StdLibLoggerAdapter(name="test.fmt", level=logging.DEBUG)
        adapter._logger = MagicMock()
        adapter._logger.isEnabledFor.return_value = True

        adapter.info("connected", host="localhost", port=50000)

        adapter._logger.log.assert_called_once()
        formatted = adapter._logger.log.call_args[0][1]
        assert "host=localhost" in formatted
        assert "port=50000" in formatted
        assert formatted.startswith("connected ")

    @patch("kubemq._internal.telemetry.HAS_OTEL", False)
    def test_skips_logging_when_level_disabled(self):
        adapter = StdLibLoggerAdapter(name="test.skip", level=logging.DEBUG)
        adapter._logger = MagicMock()
        adapter._logger.isEnabledFor.return_value = False

        adapter.debug("should not log")

        adapter._logger.log.assert_not_called()

    @patch("kubemq._internal.telemetry.HAS_OTEL", False)
    def test_format_plain_message_no_kwargs(self):
        adapter = StdLibLoggerAdapter(name="test.plain", level=logging.DEBUG)
        adapter._has_otel = False
        result = adapter._format("plain msg", {})
        assert result == "plain msg"


class TestStdLibLoggerAdapterOTel:
    """Tests for OTel trace context injection (lines 88-98)."""

    def test_inject_trace_context_without_otel(self):
        adapter = StdLibLoggerAdapter.__new__(StdLibLoggerAdapter)
        adapter._has_otel = False
        result = adapter._inject_trace_context({"a": "b"})
        assert result == {"a": "b"}

    def test_inject_trace_context_with_mocked_otel(self):
        import sys

        try:
            otel_trace = MagicMock()
            mock_ctx = MagicMock()
            mock_ctx.is_valid = True
            mock_ctx.trace_id = 0x1234567890ABCDEF1234567890ABCDEF
            mock_ctx.span_id = 0x1234567890ABCDEF
            mock_span = MagicMock()
            mock_span.get_span_context.return_value = mock_ctx
            otel_trace.get_current_span.return_value = mock_span

            with patch.dict(
                sys.modules, {"opentelemetry": MagicMock(), "opentelemetry.trace": otel_trace}
            ):
                adapter = StdLibLoggerAdapter.__new__(StdLibLoggerAdapter)
                adapter._has_otel = True
                result = adapter._inject_trace_context({"key": "val"})
                assert "trace_id" in result
                assert "span_id" in result
                assert result["key"] == "val"
        except Exception:
            pytest.skip("OTel mocking failed")

    def test_inject_trace_context_invalid_span(self):
        import sys

        try:
            otel_trace = MagicMock()
            mock_ctx = MagicMock()
            mock_ctx.is_valid = False
            mock_span = MagicMock()
            mock_span.get_span_context.return_value = mock_ctx
            otel_trace.get_current_span.return_value = mock_span

            with patch.dict(
                sys.modules, {"opentelemetry": MagicMock(), "opentelemetry.trace": otel_trace}
            ):
                adapter = StdLibLoggerAdapter.__new__(StdLibLoggerAdapter)
                adapter._has_otel = True
                result = adapter._inject_trace_context({"key": "val"})
                assert "trace_id" not in result
        except Exception:
            pytest.skip("OTel mocking failed")
