"""Tests for kubemq.core.exceptions module."""

from __future__ import annotations

from unittest.mock import MagicMock

import grpc
import pytest

from kubemq.core.exceptions import (
    KubeMQAuthenticationError,
    KubeMQChannelError,
    KubeMQCircuitOpenError,
    KubeMQConnectionError,
    KubeMQError,
    KubeMQMessageError,
    KubeMQTimeoutError,
    KubeMQTransactionError,
    KubeMQValidationError,
    from_grpc_error,
)


class TestKubeMQError:
    """Tests for the base KubeMQError exception."""

    def test_basic_creation(self):
        """Test basic exception creation with just a message."""
        exc = KubeMQError("Test error")
        assert exc.message == "Test error"
        assert exc.code is None
        assert exc.details == {}  # Empty dict, not None
        assert exc.cause is None

    def test_with_code(self):
        """Test exception creation with error code."""
        exc = KubeMQError("Test error", code="ERR001")
        assert exc.message == "Test error"
        assert exc.code == "ERR001"

    def test_with_details(self):
        """Test exception creation with details dictionary."""
        details = {"key": "value", "count": 42}
        exc = KubeMQError("Test error", details=details)
        assert exc.details == details

    def test_with_cause(self):
        """Test exception creation with cause exception."""
        cause = ValueError("Original error")
        exc = KubeMQError("Test error", cause=cause)
        assert exc.cause is cause
        # Note: __cause__ is not automatically set by our implementation

    def test_full_creation(self):
        """Test exception creation with all parameters."""
        cause = RuntimeError("Original")
        details = {"field": "test"}
        exc = KubeMQError(
            "Complete error",
            code="ERR999",
            details=details,
            cause=cause,
        )
        assert exc.message == "Complete error"
        assert exc.code == "ERR999"
        assert exc.details == details
        assert exc.cause is cause

    def test_str_with_code(self):
        """Test string representation includes code."""
        exc = KubeMQError("Test error", code="ERR001")
        assert "[ERR001]" in str(exc)
        assert "Test error" in str(exc)

    def test_repr(self):
        """Test repr output."""
        exc = KubeMQError("Test error", code="ERR001")
        assert "KubeMQError" in repr(exc)
        assert "Test error" in repr(exc)


class TestKubeMQConnectionError:
    """Tests for KubeMQConnectionError."""

    def test_inheritance(self):
        """Test that KubeMQConnectionError inherits from KubeMQError."""
        exc = KubeMQConnectionError("Connection failed")
        assert isinstance(exc, KubeMQError)

    def test_with_host_port(self):
        """Test exception with host and port details."""
        exc = KubeMQConnectionError(
            "Connection refused",
            details={"host": "localhost", "port": 50000},
        )
        assert exc.details["host"] == "localhost"
        assert exc.details["port"] == 50000


class TestKubeMQAuthenticationError:
    """Tests for KubeMQAuthenticationError."""

    def test_inheritance(self):
        """Test inheritance chain."""
        exc = KubeMQAuthenticationError("Invalid token")
        assert isinstance(exc, KubeMQConnectionError)
        assert isinstance(exc, KubeMQError)


class TestKubeMQTimeoutError:
    """Tests for KubeMQTimeoutError."""

    def test_inheritance(self):
        """Test inheritance chain - inherits from KubeMQError, not KubeMQConnectionError."""
        exc = KubeMQTimeoutError("Operation timed out")
        assert isinstance(exc, KubeMQError)
        # KubeMQTimeoutError inherits directly from KubeMQError
        assert not isinstance(exc, KubeMQConnectionError)


class TestKubeMQValidationError:
    """Tests for KubeMQValidationError."""

    def test_inheritance(self):
        """Test inheritance."""
        exc = KubeMQValidationError("Invalid parameter")
        assert isinstance(exc, KubeMQError)

    def test_field_validation(self):
        """Test field-specific validation error."""
        exc = KubeMQValidationError(
            "Invalid channel name",
            details={"field": "channel", "value": ""},
        )
        assert exc.details["field"] == "channel"


class TestKubeMQChannelError:
    """Tests for KubeMQChannelError."""

    def test_inheritance(self):
        """Test inheritance."""
        exc = KubeMQChannelError("Channel not found")
        assert isinstance(exc, KubeMQError)


class TestKubeMQMessageError:
    """Tests for KubeMQMessageError."""

    def test_inheritance(self):
        """Test inheritance."""
        exc = KubeMQMessageError("Message send failed")
        assert isinstance(exc, KubeMQError)


class TestKubeMQTransactionError:
    """Tests for KubeMQTransactionError."""

    def test_inheritance(self):
        """Test inheritance."""
        exc = KubeMQTransactionError("Transaction failed")
        assert isinstance(exc, KubeMQError)


class TestKubeMQCircuitOpenError:
    """Tests for KubeMQCircuitOpenError."""

    def test_inheritance(self):
        """Test inheritance chain - inherits from KubeMQError, not KubeMQConnectionError."""
        exc = KubeMQCircuitOpenError("Circuit breaker open")
        assert isinstance(exc, KubeMQError)
        # KubeMQCircuitOpenError inherits directly from KubeMQError
        assert not isinstance(exc, KubeMQConnectionError)


class TestFromGrpcError:
    """Tests for from_grpc_error function."""

    def _create_mock_grpc_error(self, code, details="Test details"):
        """Helper to create a mock gRPC error."""
        mock_error = MagicMock()
        mock_error.code = MagicMock(return_value=code)
        mock_error.details = MagicMock(return_value=details)
        # Make it look like an RpcError to isinstance check
        mock_error.__class__ = grpc.RpcError
        return mock_error

    def test_unavailable_error(self):
        """Test mapping of UNAVAILABLE to KubeMQConnectionError."""
        grpc_error = self._create_mock_grpc_error(grpc.StatusCode.UNAVAILABLE)
        result = from_grpc_error(grpc_error)
        assert isinstance(result, KubeMQConnectionError)
        assert result.code == "UNAVAILABLE"

    def test_unauthenticated_error(self):
        """Test mapping of UNAUTHENTICATED to KubeMQAuthenticationError."""
        grpc_error = self._create_mock_grpc_error(grpc.StatusCode.UNAUTHENTICATED)
        result = from_grpc_error(grpc_error)
        assert isinstance(result, KubeMQAuthenticationError)
        assert result.code == "UNAUTHENTICATED"

    def test_permission_denied_error(self):
        """Test mapping of PERMISSION_DENIED to KubeMQAuthenticationError."""
        grpc_error = self._create_mock_grpc_error(grpc.StatusCode.PERMISSION_DENIED)
        result = from_grpc_error(grpc_error)
        assert isinstance(result, KubeMQAuthenticationError)
        assert result.code == "PERMISSION_DENIED"

    def test_deadline_exceeded_error(self):
        """Test mapping of DEADLINE_EXCEEDED to KubeMQTimeoutError."""
        grpc_error = self._create_mock_grpc_error(grpc.StatusCode.DEADLINE_EXCEEDED)
        result = from_grpc_error(grpc_error)
        assert isinstance(result, KubeMQTimeoutError)
        assert result.code == "DEADLINE_EXCEEDED"

    def test_invalid_argument_error(self):
        """Test mapping of INVALID_ARGUMENT to KubeMQValidationError."""
        grpc_error = self._create_mock_grpc_error(grpc.StatusCode.INVALID_ARGUMENT)
        result = from_grpc_error(grpc_error)
        assert isinstance(result, KubeMQValidationError)
        assert result.code == "INVALID_ARGUMENT"

    def test_not_found_error(self):
        """Test mapping of NOT_FOUND to KubeMQChannelError."""
        grpc_error = self._create_mock_grpc_error(grpc.StatusCode.NOT_FOUND)
        result = from_grpc_error(grpc_error)
        assert isinstance(result, KubeMQChannelError)
        assert result.code == "NOT_FOUND"

    def test_already_exists_error(self):
        """Test mapping of ALREADY_EXISTS to KubeMQChannelError."""
        grpc_error = self._create_mock_grpc_error(grpc.StatusCode.ALREADY_EXISTS)
        result = from_grpc_error(grpc_error)
        assert isinstance(result, KubeMQChannelError)
        assert result.code == "ALREADY_EXISTS"

    def test_resource_exhausted_error(self):
        """Test mapping of RESOURCE_EXHAUSTED to KubeMQMessageError."""
        grpc_error = self._create_mock_grpc_error(grpc.StatusCode.RESOURCE_EXHAUSTED)
        result = from_grpc_error(grpc_error)
        assert isinstance(result, KubeMQMessageError)
        assert result.code == "RESOURCE_EXHAUSTED"

    def test_unknown_error(self):
        """Test mapping of unknown codes to base KubeMQError."""
        grpc_error = self._create_mock_grpc_error(grpc.StatusCode.INTERNAL)
        result = from_grpc_error(grpc_error)
        assert isinstance(result, KubeMQError)
        assert result.code == "INTERNAL"

    def test_non_grpc_error(self):
        """Test handling of non-gRPC exceptions."""
        regular_error = ValueError("Not a gRPC error")
        result = from_grpc_error(regular_error)
        assert isinstance(result, KubeMQError)
        assert "Not a gRPC error" in result.message

    def test_preserves_cause(self):
        """Test that the original error is preserved as cause."""
        grpc_error = self._create_mock_grpc_error(grpc.StatusCode.UNAVAILABLE)
        result = from_grpc_error(grpc_error)
        assert result.cause is grpc_error


class TestExceptionHierarchy:
    """Tests for the complete exception hierarchy."""

    def test_all_exceptions_inherit_from_base(self):
        """Test that all exceptions inherit from KubeMQError."""
        exceptions = [
            KubeMQConnectionError("test"),
            KubeMQAuthenticationError("test"),
            KubeMQTimeoutError("test"),
            KubeMQValidationError("test"),
            KubeMQChannelError("test"),
            KubeMQMessageError("test"),
            KubeMQTransactionError("test"),
            KubeMQCircuitOpenError("test"),
        ]
        for exc in exceptions:
            assert isinstance(exc, KubeMQError)
            assert isinstance(exc, Exception)

    def test_exceptions_are_catchable(self):
        """Test that exceptions can be caught by base type."""
        with pytest.raises(KubeMQError):
            raise KubeMQConnectionError("Connection failed")

        with pytest.raises(KubeMQError):
            raise KubeMQValidationError("Invalid input")
