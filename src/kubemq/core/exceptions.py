"""Exception hierarchy for KubeMQ Python SDK.

All custom exceptions are prefixed with 'KubeMQ' to avoid shadowing
Python built-in exceptions (e.g., ConnectionError, TimeoutError).
"""

from __future__ import annotations

from typing import Any


class KubeMQError(Exception):
    """Base exception for all KubeMQ errors.

    Attributes:
        message: Human-readable error message
        code: Optional error code for programmatic handling
        details: Optional dictionary with additional error context
        cause: Optional original exception that caused this error
    """

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        details: dict[str, Any] | None = None,
        cause: Exception | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.code = code
        self.details = details or {}
        self.cause = cause

    def __str__(self) -> str:
        parts = [self.message]
        if self.code:
            parts.insert(0, f"[{self.code}]")
        return " ".join(parts)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.message!r}, code={self.code!r})"


# Connection-related exceptions
class KubeMQConnectionError(KubeMQError):
    """Connection-related errors (does not shadow built-in ConnectionError)."""

    pass


class KubeMQAuthenticationError(KubeMQConnectionError):
    """Authentication or authorization failed."""

    pass


class KubeMQTimeoutError(KubeMQError):
    """Operation timed out (does not shadow built-in TimeoutError)."""

    pass


# Validation exceptions
class KubeMQValidationError(KubeMQError):
    """Input validation failed."""

    pass


# Operation exceptions
class KubeMQChannelError(KubeMQError):
    """Channel operation failed (create, delete, list)."""

    pass


class KubeMQMessageError(KubeMQError):
    """Message operation failed (send, receive)."""

    pass


class KubeMQTransactionError(KubeMQError):
    """Transaction operation failed (ack, reject, requeue)."""

    pass


# Advanced feature exceptions (forward declaration for Phase 5)
class KubeMQCircuitOpenError(KubeMQError):
    """Circuit breaker is open, request rejected.

    Note: This exception is defined here for completeness but is
    primarily used in Phase 5 (Advanced Features).
    """

    pass


def from_grpc_error(error: Exception) -> KubeMQError:
    """Convert a gRPC error to the appropriate KubeMQ exception.

    Works with both grpc.RpcError (sync) and grpc.aio.AioRpcError (async).
    Both have compatible .code() and .details() methods.

    Args:
        error: The original exception (may or may not be a gRPC error)

    Returns:
        An appropriate KubeMQError subclass instance
    """
    # Gracefully handle case where grpc is not installed
    try:
        import grpc
        import grpc.aio
    except ImportError:
        return KubeMQError(str(error), cause=error)

    # Handle both sync and async gRPC errors
    # grpc.aio.AioRpcError inherits from grpc.RpcError, so this check covers both
    if not isinstance(error, grpc.RpcError):
        # Check if it looks like a gRPC-like error (has code() and details() methods)
        # This handles mock objects and duck-typed errors
        if hasattr(error, "code") and callable(error.code):
            return _convert_grpc_like_error(error)
        return KubeMQError(str(error), cause=error)

    # Extract gRPC status code and details
    # Both RpcError and AioRpcError have .code() method
    code = None
    details = str(error)

    if hasattr(error, "code") and callable(error.code):
        code = error.code()
    if hasattr(error, "details") and callable(error.details):
        details = error.details() or str(error)

    # Map gRPC status codes to KubeMQ exceptions
    mapping = {
        grpc.StatusCode.UNAVAILABLE: KubeMQConnectionError,
        grpc.StatusCode.DEADLINE_EXCEEDED: KubeMQTimeoutError,
        grpc.StatusCode.UNAUTHENTICATED: KubeMQAuthenticationError,
        grpc.StatusCode.PERMISSION_DENIED: KubeMQAuthenticationError,
        grpc.StatusCode.INVALID_ARGUMENT: KubeMQValidationError,
        grpc.StatusCode.NOT_FOUND: KubeMQChannelError,
        grpc.StatusCode.ALREADY_EXISTS: KubeMQChannelError,
        grpc.StatusCode.RESOURCE_EXHAUSTED: KubeMQMessageError,
        grpc.StatusCode.ABORTED: KubeMQTransactionError,
        grpc.StatusCode.CANCELLED: KubeMQError,
        grpc.StatusCode.INTERNAL: KubeMQError,
        grpc.StatusCode.UNKNOWN: KubeMQError,
    }

    exc_class = mapping.get(code, KubeMQError) if code else KubeMQError

    return exc_class(
        details or "Unknown gRPC error",
        code=code.name if code else None,
        cause=error,
    )


def _convert_grpc_like_error(error: Exception) -> KubeMQError:
    """Convert a gRPC-like error (duck-typed) to KubeMQ exception.

    This handles mock objects and errors that have code() and details() methods
    but aren't actual grpc.RpcError instances.
    """
    code = error.code() if hasattr(error, "code") and callable(error.code) else None
    details_raw = (
        error.details() if hasattr(error, "details") and callable(error.details) else str(error)
    )
    details = str(details_raw) if details_raw is not None else ""

    # Handle string codes (e.g., "UNAVAILABLE")
    code_str = str(code) if code else ""

    # Map string codes to exception classes
    string_mapping = {
        "UNAVAILABLE": KubeMQConnectionError,
        "DEADLINE_EXCEEDED": KubeMQTimeoutError,
        "UNAUTHENTICATED": KubeMQAuthenticationError,
        "PERMISSION_DENIED": KubeMQAuthenticationError,
        "INVALID_ARGUMENT": KubeMQValidationError,
        "NOT_FOUND": KubeMQChannelError,
        "ALREADY_EXISTS": KubeMQChannelError,
        "RESOURCE_EXHAUSTED": KubeMQMessageError,
        "ABORTED": KubeMQTransactionError,
    }

    exc_class = string_mapping.get(code_str, KubeMQError)

    return exc_class(
        details or "Unknown gRPC error",
        code=code_str or None,
        cause=error,
    )
