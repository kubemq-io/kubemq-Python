"""Unit tests for gRPC error → SDK error classification.

Tests all 17 gRPC status codes per REQ-ERR-6 mapping table.
Validates from_grpc_error correctly maps status codes to SDK exceptions
with appropriate ErrorCode, is_retryable, and exception type.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import grpc
import pytest

from kubemq.core.exceptions import (
    ErrorCode,
    KubeMQAuthenticationError,
    KubeMQCancellationError,
    KubeMQChannelError,
    KubeMQConnectionError,
    KubeMQError,
    KubeMQMessageError,
    KubeMQTimeoutError,
    KubeMQTransactionError,
    KubeMQValidationError,
    from_grpc_error,
)

# Mapping table aligned with _GRPC_MAPPING in src/kubemq/core/exceptions.py.
# CANCELLED without cancellation_token is treated as server-initiated
# and returns KubeMQConnectionError (retryable=True, code=CANCELLED).
GRPC_CODE_EXPECTATIONS = [
    # (grpc_code, expected_error_code, expected_is_retryable, expected_exception_type)
    (grpc.StatusCode.OK, ErrorCode.UNKNOWN, False, KubeMQError),
    (grpc.StatusCode.UNKNOWN, ErrorCode.UNKNOWN, True, KubeMQError),
    (grpc.StatusCode.INVALID_ARGUMENT, ErrorCode.VALIDATION_ERROR, False, KubeMQValidationError),
    (grpc.StatusCode.DEADLINE_EXCEEDED, ErrorCode.CONNECTION_TIMEOUT, True, KubeMQTimeoutError),
    (grpc.StatusCode.NOT_FOUND, ErrorCode.NOT_FOUND, False, KubeMQChannelError),
    (grpc.StatusCode.ALREADY_EXISTS, ErrorCode.ALREADY_EXISTS, False, KubeMQValidationError),
    (grpc.StatusCode.PERMISSION_DENIED, ErrorCode.PERMISSION_DENIED, False, KubeMQAuthenticationError),
    (grpc.StatusCode.RESOURCE_EXHAUSTED, ErrorCode.RESOURCE_EXHAUSTED, True, KubeMQMessageError),
    (grpc.StatusCode.FAILED_PRECONDITION, ErrorCode.VALIDATION_ERROR, False, KubeMQValidationError),
    (grpc.StatusCode.ABORTED, ErrorCode.ABORTED, True, KubeMQTransactionError),
    (grpc.StatusCode.OUT_OF_RANGE, ErrorCode.OUT_OF_RANGE, False, KubeMQValidationError),
    (grpc.StatusCode.UNIMPLEMENTED, ErrorCode.UNIMPLEMENTED, False, KubeMQError),
    (grpc.StatusCode.INTERNAL, ErrorCode.INTERNAL, False, KubeMQError),
    (grpc.StatusCode.UNAVAILABLE, ErrorCode.UNAVAILABLE, True, KubeMQConnectionError),
    (grpc.StatusCode.DATA_LOSS, ErrorCode.DATA_LOSS, False, KubeMQError),
    (grpc.StatusCode.UNAUTHENTICATED, ErrorCode.AUTH_FAILED, False, KubeMQAuthenticationError),
]


class _MockRpcError(grpc.RpcError):
    """Concrete mock of grpc.RpcError with .code() and .details().

    MagicMock(spec=grpc.RpcError) can't be used because RpcError is an ABC
    and .code()/.details() are only defined on concrete subclasses.
    """

    def __init__(self, code: grpc.StatusCode, details: str) -> None:
        super().__init__(details)
        self._code = code
        self._details = details
        self._trailing_metadata: list[tuple[str, str]] = []

    def code(self) -> grpc.StatusCode:
        return self._code

    def details(self) -> str:
        return self._details

    def trailing_metadata(self) -> list[tuple[str, str]]:
        return self._trailing_metadata


def _make_grpc_error(code: grpc.StatusCode, details: str) -> grpc.RpcError:
    return _MockRpcError(code, details)


class TestGrpcErrorClassification:
    """Verify from_grpc_error maps all 17 gRPC status codes correctly."""

    @pytest.mark.parametrize(
        "grpc_code, expected_code, expected_retryable, expected_type",
        GRPC_CODE_EXPECTATIONS,
        ids=[code.name for code, *_ in GRPC_CODE_EXPECTATIONS],
    )
    def test_grpc_code_mapping(
        self,
        grpc_code: grpc.StatusCode,
        expected_code: ErrorCode,
        expected_retryable: bool,
        expected_type: type,
    ) -> None:
        mock_error = _make_grpc_error(grpc_code, "test detail")
        result = from_grpc_error(mock_error, operation="SendEvent", channel="test-ch")

        assert isinstance(result, expected_type)
        assert result.code == expected_code
        assert result.is_retryable == expected_retryable
        assert result.operation == "SendEvent"
        assert result.channel == "test-ch"

    def test_cancelled_without_token_is_server_initiated(self) -> None:
        """CANCELLED without cancellation_token → server-initiated (retryable)."""
        mock_error = _make_grpc_error(grpc.StatusCode.CANCELLED, "cancelled")
        result = from_grpc_error(mock_error, operation="Subscribe", channel="ch")

        assert isinstance(result, KubeMQConnectionError)
        assert result.code == ErrorCode.CANCELLED
        assert result.is_retryable is True

    def test_cancelled_with_token_is_client_initiated(self) -> None:
        """CANCELLED with active cancellation_token → client-initiated."""
        error = _make_grpc_error(grpc.StatusCode.CANCELLED, "cancelled by client")
        token = MagicMock()
        token.is_cancelled = True

        result = from_grpc_error(
            error,
            operation="Subscribe",
            channel="ch",
            cancellation_token=token,
        )

        assert isinstance(result, KubeMQCancellationError)
        assert result.code == ErrorCode.CANCELLED
        assert result.is_retryable is False

    def test_operation_and_channel_propagated(self) -> None:
        mock_error = _make_grpc_error(grpc.StatusCode.UNAVAILABLE, "server down")
        result = from_grpc_error(mock_error, operation="Subscribe", channel="my-channel")

        assert result.operation == "Subscribe"
        assert result.channel == "my-channel"

    def test_cause_chain_preserved(self) -> None:
        mock_error = _make_grpc_error(grpc.StatusCode.INTERNAL, "oops")
        result = from_grpc_error(mock_error, operation="Ping")

        assert result.cause is mock_error

    def test_non_grpc_error_wrapped(self) -> None:
        """Non-gRPC exceptions are wrapped in KubeMQError."""
        error = RuntimeError("unexpected")
        result = from_grpc_error(error, operation="Send", channel="ch")

        assert isinstance(result, KubeMQError)
        assert result.cause is error
        assert result.operation == "Send"
        assert result.channel == "ch"

    def test_duck_typed_grpc_error(self) -> None:
        """Objects with .code() and .details() but not RpcError are handled."""
        error = MagicMock()
        error.code.return_value = "UNAVAILABLE"
        error.details.return_value = "connection refused"

        result = from_grpc_error(error, operation="Ping", channel="ch")

        assert isinstance(result, KubeMQConnectionError)
        assert result.code == ErrorCode.UNAVAILABLE

    def test_error_details_extracted(self) -> None:
        error = _MockRpcError(grpc.StatusCode.UNAVAILABLE, "server down")
        error._trailing_metadata = [("x-request-id", "req-123")]

        result = from_grpc_error(error, operation="Send")

        assert result.details.get("x-request-id") == "req-123"
