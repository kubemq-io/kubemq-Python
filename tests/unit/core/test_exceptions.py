"""Tests for kubemq.core.exceptions module.

Covers REQ-ERR-1, REQ-ERR-2, REQ-ERR-5, REQ-ERR-6, REQ-ERR-9.
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import grpc
import pytest

from kubemq.core.exceptions import (
    ERROR_CLASSIFICATION,
    ErrorCategory,
    ErrorCode,
    KubeMQAuthenticationError,
    KubeMQBufferFullError,
    KubeMQCancellationError,
    KubeMQChannelError,
    KubeMQCircuitOpenError,
    KubeMQConnectionError,
    KubeMQError,
    KubeMQHandlerError,
    KubeMQMessageError,
    KubeMQTimeoutError,
    KubeMQTransactionError,
    KubeMQTransportError,
    KubeMQValidationError,
    _parse_code,
    classify_error,
    from_grpc_error,
)


# -----------------------------------------------------------------------
# REQ-ERR-1: Typed Error Hierarchy
# -----------------------------------------------------------------------


class TestErrorCode:
    """Tests for the ErrorCode enum (REQ-ERR-1)."""

    def test_all_20_members_present(self):
        required = {
            "CONNECTION_TIMEOUT", "UNAVAILABLE", "CONNECTION_NOT_READY",
            "AUTH_FAILED", "PERMISSION_DENIED",
            "VALIDATION_ERROR", "ALREADY_EXISTS", "OUT_OF_RANGE",
            "NOT_FOUND", "RESOURCE_EXHAUSTED",
            "ABORTED",
            "INTERNAL", "UNKNOWN", "UNIMPLEMENTED", "DATA_LOSS",
            "CANCELLED",
            "BUFFER_FULL", "STREAM_BROKEN", "CLIENT_CLOSED", "CONFIGURATION_ERROR",
        }
        assert required.issubset({e.name for e in ErrorCode})
        assert len(ErrorCode) == 20

    def test_values_are_strings(self):
        for code in ErrorCode:
            assert isinstance(code.value, str)
            assert code.value == code.name


class TestParseCode:
    """Tests for _parse_code helper."""

    def test_parse_by_value(self):
        assert _parse_code("UNAVAILABLE") == ErrorCode.UNAVAILABLE

    def test_parse_by_name(self):
        assert _parse_code("AUTH_FAILED") == ErrorCode.AUTH_FAILED

    def test_unknown_string_returns_none(self):
        assert _parse_code("BOGUS") is None

    def test_none_returns_none(self):
        assert _parse_code(None) is None


class TestKubeMQError:
    """Tests for the base KubeMQError exception (REQ-ERR-1)."""

    def test_basic_creation(self):
        exc = KubeMQError("Test error")
        assert exc.message == "Test error"
        assert exc.code is None
        assert exc.details == {}
        assert exc.cause is None
        assert exc.operation == ""
        assert exc.channel is None
        assert exc.is_retryable is False
        assert exc.request_id == ""

    def test_all_fields(self):
        cause = RuntimeError("Original")
        exc = KubeMQError(
            "Complete error",
            code=ErrorCode.AUTH_FAILED,
            details={"field": "test"},
            cause=cause,
            operation="SendEvent",
            channel="orders",
            is_retryable=False,
            request_id="abc-123",
        )
        assert exc.message == "Complete error"
        assert exc.code == ErrorCode.AUTH_FAILED
        assert exc.details == {"field": "test"}
        assert exc.cause is cause
        assert exc.operation == "SendEvent"
        assert exc.channel == "orders"
        assert exc.is_retryable is False
        assert exc.request_id == "abc-123"

    def test_code_as_string_parsed(self):
        exc = KubeMQError("test", code="UNAVAILABLE")
        assert exc.code == ErrorCode.UNAVAILABLE

    def test_code_as_enum(self):
        exc = KubeMQError("test", code=ErrorCode.AUTH_FAILED)
        assert exc.code == ErrorCode.AUTH_FAILED

    def test_unknown_string_code(self):
        exc = KubeMQError("test", code="BOGUS")
        assert exc.code is None

    def test_cause_chaining(self):
        original = ValueError("original")
        try:
            raise KubeMQError("wrapped") from original
        except KubeMQError as e:
            assert e.__cause__ is original

    def test_with_details(self):
        details = {"key": "value", "count": 42}
        exc = KubeMQError("Test error", details=details)
        assert exc.details == details

    def test_with_cause(self):
        cause = ValueError("Original error")
        exc = KubeMQError("Test error", cause=cause)
        assert exc.cause is cause


class TestKubeMQConnectionError:
    """Tests for KubeMQConnectionError."""

    def test_inheritance(self):
        exc = KubeMQConnectionError("Connection failed")
        assert isinstance(exc, KubeMQError)

    def test_with_host_port(self):
        exc = KubeMQConnectionError(
            "Connection refused",
            details={"host": "localhost", "port": 50000},
        )
        assert exc.details["host"] == "localhost"
        assert exc.details["port"] == 50000

    def test_kwargs_forwarding(self):
        exc = KubeMQConnectionError(
            "fail",
            code=ErrorCode.UNAVAILABLE,
            operation="Connect",
            channel="test",
            is_retryable=True,
        )
        assert exc.code == ErrorCode.UNAVAILABLE
        assert exc.operation == "Connect"
        assert exc.is_retryable is True


class TestKubeMQAuthenticationError:
    """Tests for KubeMQAuthenticationError."""

    def test_inheritance(self):
        exc = KubeMQAuthenticationError("Invalid token")
        assert isinstance(exc, KubeMQConnectionError)
        assert isinstance(exc, KubeMQError)

    def test_default_not_retryable(self):
        exc = KubeMQAuthenticationError("auth failed")
        assert exc.is_retryable is False


class TestKubeMQTimeoutError:
    """Tests for KubeMQTimeoutError."""

    def test_inheritance(self):
        exc = KubeMQTimeoutError("Operation timed out")
        assert isinstance(exc, KubeMQError)
        assert not isinstance(exc, KubeMQConnectionError)

    def test_default_retryable(self):
        exc = KubeMQTimeoutError("timeout")
        assert exc.is_retryable is True

    def test_default_code(self):
        exc = KubeMQTimeoutError("timeout")
        assert exc.code == ErrorCode.CONNECTION_TIMEOUT


class TestKubeMQValidationError:
    """Tests for KubeMQValidationError."""

    def test_inheritance(self):
        exc = KubeMQValidationError("Invalid parameter")
        assert isinstance(exc, KubeMQError)

    def test_default_not_retryable(self):
        exc = KubeMQValidationError("bad input")
        assert exc.is_retryable is False

    def test_field_validation(self):
        exc = KubeMQValidationError(
            "Invalid channel name",
            details={"field": "channel", "value": ""},
        )
        assert exc.details["field"] == "channel"


class TestKubeMQChannelError:
    def test_inheritance(self):
        exc = KubeMQChannelError("Channel not found")
        assert isinstance(exc, KubeMQError)


class TestKubeMQMessageError:
    def test_inheritance(self):
        exc = KubeMQMessageError("Message send failed")
        assert isinstance(exc, KubeMQError)


class TestKubeMQTransactionError:
    def test_inheritance(self):
        exc = KubeMQTransactionError("Transaction failed")
        assert isinstance(exc, KubeMQError)


class TestKubeMQCircuitOpenError:
    def test_inheritance(self):
        exc = KubeMQCircuitOpenError("Circuit breaker open")
        assert isinstance(exc, KubeMQError)
        assert not isinstance(exc, KubeMQConnectionError)

    def test_kwargs_forwarding(self):
        exc = KubeMQCircuitOpenError("open", operation="SendEvent")
        assert exc.operation == "SendEvent"


class TestSubclassFieldInheritance:
    """Subclass inherits all fields via **kwargs (REQ-ERR-1 T7)."""

    def test_timeout_inherits_operation(self):
        exc = KubeMQTimeoutError("timeout", operation="SendEvent")
        assert exc.operation == "SendEvent"

    def test_all_subclasses_support_new_fields(self):
        subclasses = [
            KubeMQConnectionError, KubeMQAuthenticationError,
            KubeMQTimeoutError, KubeMQValidationError,
            KubeMQChannelError, KubeMQMessageError,
            KubeMQTransactionError, KubeMQCircuitOpenError,
        ]
        for cls in subclasses:
            exc = cls(
                "test",
                operation="Op",
                channel="ch",
                request_id="rid",
            )
            assert exc.operation == "Op"
            assert exc.channel == "ch"
            assert exc.request_id == "rid"


# -----------------------------------------------------------------------
# REQ-ERR-2: Error Classification
# -----------------------------------------------------------------------


class TestErrorCategory:
    def test_all_categories_present(self):
        expected = {
            "TRANSIENT", "TIMEOUT", "THROTTLING", "AUTHENTICATION",
            "AUTHORIZATION", "VALIDATION", "NOT_FOUND", "FATAL",
            "CANCELLATION", "BACKPRESSURE",
        }
        assert expected == {c.name for c in ErrorCategory}


class TestErrorClassification:
    def test_all_error_codes_classified(self):
        assert set(ErrorCode) == set(ERROR_CLASSIFICATION.keys())

    def test_unavailable_is_transient_retryable(self):
        cat, retryable = ERROR_CLASSIFICATION[ErrorCode.UNAVAILABLE]
        assert cat == ErrorCategory.TRANSIENT
        assert retryable is True

    def test_auth_failed_not_retryable(self):
        cat, retryable = ERROR_CLASSIFICATION[ErrorCode.AUTH_FAILED]
        assert cat == ErrorCategory.AUTHENTICATION
        assert retryable is False

    def test_buffer_full_is_backpressure(self):
        cat, retryable = ERROR_CLASSIFICATION[ErrorCode.BUFFER_FULL]
        assert cat == ErrorCategory.BACKPRESSURE
        assert retryable is False


class TestClassifyError:
    def test_classify_connection_error(self):
        e = KubeMQConnectionError("fail", code=ErrorCode.UNAVAILABLE)
        cat, retryable = classify_error(e)
        assert cat == ErrorCategory.TRANSIENT
        assert retryable is True

    def test_classify_auth_error(self):
        e = KubeMQAuthenticationError("fail", code=ErrorCode.AUTH_FAILED)
        cat, retryable = classify_error(e)
        assert cat == ErrorCategory.AUTHENTICATION
        assert retryable is False

    def test_classify_none_code(self):
        e = KubeMQError("fail")
        cat, retryable = classify_error(e)
        assert cat == ErrorCategory.FATAL
        assert retryable is False


class TestKubeMQBufferFullError:
    def test_creation(self):
        exc = KubeMQBufferFullError("full", buffer_size=1024)
        assert exc.code == ErrorCode.BUFFER_FULL
        assert exc.is_retryable is False
        assert exc.buffer_size == 1024
        assert isinstance(exc, KubeMQError)

    def test_default_buffer_size(self):
        exc = KubeMQBufferFullError("full")
        assert exc.buffer_size == 0


class TestKubeMQCancellationError:
    def test_creation(self):
        exc = KubeMQCancellationError("cancelled")
        assert exc.code == ErrorCode.CANCELLED
        assert exc.is_retryable is False
        assert isinstance(exc, KubeMQError)


# -----------------------------------------------------------------------
# REQ-ERR-5: Actionable Error Messages
# -----------------------------------------------------------------------


class TestActionableMessages:
    def test_str_with_operation_and_channel(self):
        exc = KubeMQError(
            "connection refused",
            code=ErrorCode.UNAVAILABLE,
            operation="SendEvent",
            channel="orders",
        )
        s = str(exc)
        assert s.startswith('SendEvent failed on channel "orders":')
        assert "connection refused" in s

    def test_str_with_operation_no_channel(self):
        exc = KubeMQError(
            "connection refused",
            operation="SendEvent",
        )
        s = str(exc)
        assert s.startswith("SendEvent failed:")

    def test_str_no_operation(self):
        exc = KubeMQError("connection refused")
        assert str(exc) == "connection refused"

    def test_str_includes_suggestion(self):
        exc = KubeMQError(
            "unavailable",
            code=ErrorCode.UNAVAILABLE,
            operation="SendEvent",
        )
        s = str(exc)
        assert "Suggestion:" in s
        assert "Check server connectivity" in s

    def test_str_with_retry_exhaustion(self):
        exc = KubeMQError(
            "unavailable",
            code=ErrorCode.UNAVAILABLE,
            details={"retry_attempts": 3, "retry_duration_seconds": 5.2},
        )
        s = str(exc)
        assert "Retries exhausted:" in s
        assert "3/3" in s
        assert "5.2s" in s

    def test_str_unknown_code_no_suggestion(self):
        exc = KubeMQError("fail")
        s = str(exc)
        assert "Suggestion:" not in s

    def test_str_never_includes_traceback(self):
        exc = KubeMQError(
            "fail",
            code=ErrorCode.INTERNAL,
            operation="Op",
            channel="ch",
        )
        assert "Traceback" not in str(exc)

    def test_repr_includes_key_fields(self):
        exc = KubeMQError(
            "fail",
            code=ErrorCode.INTERNAL,
            operation="SendEvent",
            channel="orders",
            is_retryable=False,
        )
        r = repr(exc)
        assert "KubeMQError(" in r
        assert "SendEvent" in r
        assert "orders" in r
        assert "INTERNAL" in r

    def test_all_error_codes_have_suggestions(self):
        from kubemq.core.exceptions import _ERROR_SUGGESTIONS
        for code in ErrorCode:
            assert code in _ERROR_SUGGESTIONS, f"Missing suggestion for {code.name}"


# -----------------------------------------------------------------------
# REQ-ERR-6: gRPC Error Mapping
# -----------------------------------------------------------------------


class TestFromGrpcError:
    """Tests for from_grpc_error function (REQ-ERR-6)."""

    def _create_mock_grpc_error(self, code, details="Test details"):
        mock_error = MagicMock()
        mock_error.code = MagicMock(return_value=code)
        mock_error.details = MagicMock(return_value=details)
        mock_error.__class__ = grpc.RpcError
        return mock_error

    def test_all_17_codes_produce_sdk_errors(self):
        """All 17 gRPC status codes produce KubeMQError subclasses."""
        for code in grpc.StatusCode:
            if code == grpc.StatusCode.OK:
                continue
            grpc_error = self._create_mock_grpc_error(code)
            result = from_grpc_error(grpc_error)
            assert isinstance(result, KubeMQError), f"Code {code} did not produce KubeMQError"

    def test_unavailable_maps_to_connection_error(self):
        err = self._create_mock_grpc_error(grpc.StatusCode.UNAVAILABLE)
        result = from_grpc_error(err)
        assert isinstance(result, KubeMQConnectionError)
        assert result.code == ErrorCode.UNAVAILABLE
        assert result.is_retryable is True

    def test_unauthenticated_maps_to_auth_error(self):
        err = self._create_mock_grpc_error(grpc.StatusCode.UNAUTHENTICATED)
        result = from_grpc_error(err)
        assert isinstance(result, KubeMQAuthenticationError)
        assert result.code == ErrorCode.AUTH_FAILED
        assert result.is_retryable is False

    def test_permission_denied_maps_to_auth_error(self):
        err = self._create_mock_grpc_error(grpc.StatusCode.PERMISSION_DENIED)
        result = from_grpc_error(err)
        assert isinstance(result, KubeMQAuthenticationError)
        assert result.code == ErrorCode.PERMISSION_DENIED

    def test_deadline_exceeded_maps_to_timeout(self):
        err = self._create_mock_grpc_error(grpc.StatusCode.DEADLINE_EXCEEDED)
        result = from_grpc_error(err)
        assert isinstance(result, KubeMQTimeoutError)
        assert result.code == ErrorCode.CONNECTION_TIMEOUT
        assert result.is_retryable is True

    def test_invalid_argument_maps_to_validation(self):
        err = self._create_mock_grpc_error(grpc.StatusCode.INVALID_ARGUMENT)
        result = from_grpc_error(err)
        assert isinstance(result, KubeMQValidationError)
        assert result.code == ErrorCode.VALIDATION_ERROR

    def test_not_found_maps_to_channel_error(self):
        err = self._create_mock_grpc_error(grpc.StatusCode.NOT_FOUND)
        result = from_grpc_error(err)
        assert isinstance(result, KubeMQChannelError)
        assert result.code == ErrorCode.NOT_FOUND

    def test_already_exists_maps_to_validation(self):
        err = self._create_mock_grpc_error(grpc.StatusCode.ALREADY_EXISTS)
        result = from_grpc_error(err)
        assert isinstance(result, KubeMQValidationError)
        assert result.code == ErrorCode.ALREADY_EXISTS

    def test_resource_exhausted_maps_to_message_error(self):
        err = self._create_mock_grpc_error(grpc.StatusCode.RESOURCE_EXHAUSTED)
        result = from_grpc_error(err)
        assert isinstance(result, KubeMQMessageError)
        assert result.code == ErrorCode.RESOURCE_EXHAUSTED
        assert result.is_retryable is True

    def test_failed_precondition_maps_to_validation(self):
        err = self._create_mock_grpc_error(grpc.StatusCode.FAILED_PRECONDITION)
        result = from_grpc_error(err)
        assert isinstance(result, KubeMQValidationError)
        assert result.code == ErrorCode.VALIDATION_ERROR

    def test_out_of_range_maps_to_validation(self):
        err = self._create_mock_grpc_error(grpc.StatusCode.OUT_OF_RANGE)
        result = from_grpc_error(err)
        assert isinstance(result, KubeMQValidationError)
        assert result.code == ErrorCode.OUT_OF_RANGE

    def test_aborted_maps_to_transaction(self):
        err = self._create_mock_grpc_error(grpc.StatusCode.ABORTED)
        result = from_grpc_error(err)
        assert isinstance(result, KubeMQTransactionError)
        assert result.code == ErrorCode.ABORTED
        assert result.is_retryable is True

    def test_unimplemented_not_retryable(self):
        err = self._create_mock_grpc_error(grpc.StatusCode.UNIMPLEMENTED)
        result = from_grpc_error(err)
        assert isinstance(result, KubeMQError)
        assert result.code == ErrorCode.UNIMPLEMENTED
        assert result.is_retryable is False

    def test_internal_not_retryable(self):
        err = self._create_mock_grpc_error(grpc.StatusCode.INTERNAL)
        result = from_grpc_error(err)
        assert isinstance(result, KubeMQError)
        assert result.code == ErrorCode.INTERNAL
        assert result.is_retryable is False

    def test_data_loss_not_retryable(self):
        err = self._create_mock_grpc_error(grpc.StatusCode.DATA_LOSS)
        result = from_grpc_error(err)
        assert isinstance(result, KubeMQError)
        assert result.code == ErrorCode.DATA_LOSS
        assert result.is_retryable is False

    def test_unknown_is_retryable(self):
        err = self._create_mock_grpc_error(grpc.StatusCode.UNKNOWN)
        result = from_grpc_error(err)
        assert isinstance(result, KubeMQError)
        assert result.code == ErrorCode.UNKNOWN
        assert result.is_retryable is True

    def test_cancelled_without_token_server_initiated(self):
        err = self._create_mock_grpc_error(grpc.StatusCode.CANCELLED)
        result = from_grpc_error(err)
        assert isinstance(result, KubeMQConnectionError)
        assert result.is_retryable is True

    def test_cancelled_with_client_token(self):
        err = self._create_mock_grpc_error(grpc.StatusCode.CANCELLED)
        token = MagicMock()
        token.is_cancelled = True
        result = from_grpc_error(err, cancellation_token=token)
        assert isinstance(result, KubeMQCancellationError)
        assert result.is_retryable is False

    def test_operation_and_channel_propagated(self):
        err = self._create_mock_grpc_error(grpc.StatusCode.UNAVAILABLE)
        result = from_grpc_error(err, operation="SendEvent", channel="orders")
        assert result.operation == "SendEvent"
        assert result.channel == "orders"

    def test_original_error_preserved_as_cause(self):
        grpc_error = self._create_mock_grpc_error(grpc.StatusCode.UNAVAILABLE)
        result = from_grpc_error(grpc_error)
        assert result.cause is grpc_error

    def test_non_grpc_error(self):
        regular_error = ValueError("Not a gRPC error")
        result = from_grpc_error(regular_error)
        assert isinstance(result, KubeMQError)
        assert "Not a gRPC error" in result.message

    def test_duck_typed_error(self):
        class FakeGrpcError:
            def code(self):
                return "UNAVAILABLE"
            def details(self):
                return "server down"

        result = from_grpc_error(FakeGrpcError(), operation="TestOp")
        assert isinstance(result, KubeMQConnectionError)
        assert result.code == ErrorCode.UNAVAILABLE
        assert result.operation == "TestOp"

    def test_rich_details_extracted(self):
        err = self._create_mock_grpc_error(grpc.StatusCode.UNAVAILABLE)
        err.trailing_metadata = MagicMock(return_value=[
            ("custom-key", "custom-value"),
            ("grpc-status", "14"),
        ])
        result = from_grpc_error(err)
        assert result.details.get("custom-key") == "custom-value"
        assert "grpc-status" not in result.details


# -----------------------------------------------------------------------
# REQ-ERR-9: Async Error Propagation
# -----------------------------------------------------------------------


class TestKubeMQTransportError:
    def test_creation(self):
        exc = KubeMQTransportError("stream broken", code=ErrorCode.STREAM_BROKEN)
        assert isinstance(exc, KubeMQError)
        assert exc.code == ErrorCode.STREAM_BROKEN

    def test_isinstance_distinction(self):
        transport = KubeMQTransportError("transport")
        handler = KubeMQHandlerError("handler")
        assert isinstance(transport, KubeMQTransportError)
        assert not isinstance(transport, KubeMQHandlerError)
        assert isinstance(handler, KubeMQHandlerError)
        assert not isinstance(handler, KubeMQTransportError)


class TestKubeMQHandlerError:
    def test_creation(self):
        cause = RuntimeError("handler crash")
        exc = KubeMQHandlerError(
            f"Message handler raised RuntimeError: {cause}",
            cause=cause,
            operation="MessageHandler",
            channel="events",
        )
        assert isinstance(exc, KubeMQError)
        assert exc.is_retryable is False
        assert exc.cause is cause
        assert exc.operation == "MessageHandler"
        assert exc.channel == "events"

    def test_cause_chaining(self):
        original = ValueError("bad handler")
        try:
            raise KubeMQHandlerError("handler error") from original
        except KubeMQHandlerError as e:
            assert e.__cause__ is original


class TestAsyncErrorPropagation:
    """Verify raise ... from e pattern works in async context."""

    async def test_async_cause_chain(self):
        original = RuntimeError("original")

        async def failing():
            try:
                raise original
            except RuntimeError as e:
                raise KubeMQTransportError("wrapped") from e

        with pytest.raises(KubeMQTransportError) as exc_info:
            await failing()

        assert exc_info.value.__cause__ is original

    async def test_handler_error_does_not_crash_stream(self):
        errors_received: list[KubeMQError] = []

        async def on_error(err: KubeMQError) -> None:
            errors_received.append(err)

        handler_exc = ValueError("bad handler code")
        handler_error = KubeMQHandlerError(
            f"Message handler raised ValueError: {handler_exc}",
            cause=handler_exc,
            operation="MessageHandler",
        )
        await on_error(handler_error)

        assert len(errors_received) == 1
        assert isinstance(errors_received[0], KubeMQHandlerError)


# -----------------------------------------------------------------------
# Exception Hierarchy
# -----------------------------------------------------------------------


class TestExceptionHierarchy:
    def test_all_exceptions_inherit_from_base(self):
        exceptions = [
            KubeMQConnectionError("test"),
            KubeMQAuthenticationError("test"),
            KubeMQTimeoutError("test"),
            KubeMQValidationError("test"),
            KubeMQChannelError("test"),
            KubeMQMessageError("test"),
            KubeMQTransactionError("test"),
            KubeMQCircuitOpenError("test"),
            KubeMQBufferFullError("test"),
            KubeMQCancellationError("test"),
            KubeMQTransportError("test"),
            KubeMQHandlerError("test"),
        ]
        for exc in exceptions:
            assert isinstance(exc, KubeMQError)
            assert isinstance(exc, Exception)

    def test_exceptions_are_catchable(self):
        with pytest.raises(KubeMQError):
            raise KubeMQConnectionError("Connection failed")

        with pytest.raises(KubeMQError):
            raise KubeMQValidationError("Invalid input")

        with pytest.raises(KubeMQError):
            raise KubeMQBufferFullError("buffer full")
