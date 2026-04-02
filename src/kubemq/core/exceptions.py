"""Exception hierarchy for KubeMQ Python SDK.

All custom exceptions are prefixed with 'KubeMQ' to avoid shadowing
Python built-in exceptions (e.g., ConnectionError, TimeoutError).
"""

from __future__ import annotations

import asyncio
import logging
import re
from enum import Enum, unique
from typing import Any

logger = logging.getLogger("kubemq.errors")


# ---------------------------------------------------------------------------
# REQ-ERR-1: ErrorCode enum
# ---------------------------------------------------------------------------


@unique
class ErrorCode(Enum):
    """Machine-readable error codes for programmatic handling.

    Stable across minor versions per semantic versioning policy.
    New codes may be added in minor versions; existing codes are
    never removed or changed in meaning within a major version.
    """

    # Connection
    CONNECTION_TIMEOUT = "CONNECTION_TIMEOUT"
    UNAVAILABLE = "UNAVAILABLE"
    CONNECTION_NOT_READY = "CONNECTION_NOT_READY"

    # Authentication / Authorization
    AUTH_FAILED = "AUTH_FAILED"
    PERMISSION_DENIED = "PERMISSION_DENIED"

    # Validation
    VALIDATION_ERROR = "VALIDATION_ERROR"
    ALREADY_EXISTS = "ALREADY_EXISTS"
    OUT_OF_RANGE = "OUT_OF_RANGE"

    # Resource
    NOT_FOUND = "NOT_FOUND"
    RESOURCE_EXHAUSTED = "RESOURCE_EXHAUSTED"

    # Transaction / Conflict
    ABORTED = "ABORTED"

    # Fatal
    INTERNAL = "INTERNAL"
    UNKNOWN = "UNKNOWN"
    UNIMPLEMENTED = "UNIMPLEMENTED"
    DATA_LOSS = "DATA_LOSS"

    # Cancellation
    CANCELLED = "CANCELLED"

    # SDK-generated
    BUFFER_FULL = "BUFFER_FULL"
    STREAM_BROKEN = "STREAM_BROKEN"
    CLIENT_CLOSED = "CLIENT_CLOSED"
    CONFIGURATION_ERROR = "CONFIGURATION_ERROR"


def _parse_code(raw: str | None) -> ErrorCode | None:
    """Best-effort parse of a string into an ErrorCode enum member."""
    if raw is None:
        return None
    try:
        return ErrorCode(raw)
    except ValueError:
        try:
            return ErrorCode[raw]
        except KeyError:
            return None


# ---------------------------------------------------------------------------
# REQ-ERR-2: ErrorCategory enum & classification table
# ---------------------------------------------------------------------------


@unique
class ErrorCategory(Enum):
    """Classification categories for error types."""

    TRANSIENT = "TRANSIENT"
    TIMEOUT = "TIMEOUT"
    THROTTLING = "THROTTLING"
    AUTHENTICATION = "AUTHENTICATION"
    AUTHORIZATION = "AUTHORIZATION"
    VALIDATION = "VALIDATION"
    NOT_FOUND = "NOT_FOUND"
    FATAL = "FATAL"
    CANCELLATION = "CANCELLATION"
    BACKPRESSURE = "BACKPRESSURE"


# ---------------------------------------------------------------------------
# REQ-ERR-5: Suggestion map for actionable error messages
# ---------------------------------------------------------------------------

_ERROR_SUGGESTIONS: dict[ErrorCode, str] = {
    ErrorCode.UNAVAILABLE: (
        "Check server connectivity and firewall rules. "
        "Verify the KubeMQ server is running at the configured address."
    ),
    ErrorCode.AUTH_FAILED: (
        "Verify your auth token is valid and not expired. "
        "Check the token matches the server's expected format."
    ),
    ErrorCode.PERMISSION_DENIED: (
        "Verify your credentials have the required permissions for this operation and channel."
    ),
    ErrorCode.NOT_FOUND: ("Verify the channel/queue exists or create it first."),
    ErrorCode.VALIDATION_ERROR: (
        "Check the request parameters. Ensure channel name is not empty "
        "and message body is within size limits."
    ),
    ErrorCode.ALREADY_EXISTS: (
        "The resource already exists. Use a different name or delete the existing resource first."
    ),
    ErrorCode.CONNECTION_TIMEOUT: (
        "The operation timed out. Consider increasing the timeout or checking server load."
    ),
    ErrorCode.RESOURCE_EXHAUSTED: (
        "The server is rate-limiting requests. Reduce send rate or increase server capacity."
    ),
    ErrorCode.BUFFER_FULL: (
        "The reconnection buffer is full. Wait for the connection to recover "
        "or increase buffer_size in ClientConfig."
    ),
    ErrorCode.STREAM_BROKEN: (
        "The stream was interrupted. The SDK will attempt to reconnect "
        "automatically. Check unacknowledged message IDs in the error."
    ),
    ErrorCode.CLIENT_CLOSED: ("The client has been closed. Create a new client instance."),
    ErrorCode.INTERNAL: (
        "An internal server error occurred. If this persists, "
        "check server logs and consider reporting the issue."
    ),
    ErrorCode.UNKNOWN: (
        "An unknown error occurred. This may be transient. Check server logs for details."
    ),
    ErrorCode.CONFIGURATION_ERROR: (
        "Check the client configuration. Verify address format, TLS settings, and credential paths."
    ),
    ErrorCode.CANCELLED: (
        "The operation was cancelled. This is expected if a cancellation token was triggered."
    ),
    ErrorCode.UNIMPLEMENTED: (
        "This operation is not supported by the server. Check server version compatibility."
    ),
    ErrorCode.DATA_LOSS: ("Unrecoverable data loss detected. Check server storage health."),
    ErrorCode.OUT_OF_RANGE: (
        "A value is out of the acceptable range. Check pagination parameters or sequence numbers."
    ),
    ErrorCode.ABORTED: ("The operation was aborted due to a conflict. Retry may succeed."),
    ErrorCode.CONNECTION_NOT_READY: (
        "The connection is not ready. Wait for the client to connect or check server availability."
    ),
}


# ---------------------------------------------------------------------------
# REQ-ERR-1 + ERR-5: KubeMQError base class
# ---------------------------------------------------------------------------


class KubeMQError(Exception):
    """Base exception for all KubeMQ errors.

    Attributes:
        message: Human-readable error message.
        code: Machine-readable ErrorCode enum value.
        details: Additional structured context.
        cause: Original exception that caused this error.
        operation: Name of the failed operation (e.g. "SendEvent").
        channel: Target channel/queue name, if applicable.
        is_retryable: Whether the caller should retry this operation.
        request_id: Client-generated UUID for log correlation.
    """

    def __init__(
        self,
        message: str,
        *,
        code: ErrorCode | str | None = None,
        details: dict[str, Any] | None = None,
        cause: Exception | None = None,
        operation: str = "",
        channel: str | None = None,
        is_retryable: bool = False,
        request_id: str = "",
    ) -> None:
        super().__init__(message)
        self.message = message
        self.code = code if isinstance(code, ErrorCode) else _parse_code(code)
        self.details = details or {}
        self.cause = cause
        self.operation = operation
        self.channel = channel
        self.is_retryable = is_retryable
        self.request_id = request_id

    def __str__(self) -> str:
        parts: list[str] = []

        if self.operation and self.channel:
            parts.append(f'{self.operation} failed on channel "{self.channel}": {self.message}')
        elif self.operation:
            parts.append(f"{self.operation} failed: {self.message}")
        else:
            parts.append(self.message)

        suggestion = _ERROR_SUGGESTIONS.get(self.code) if self.code else None
        if suggestion:
            parts.append(f"  Suggestion: {suggestion}")

        retry_attempts = self.details.get("retry_attempts")
        if retry_attempts is not None:
            duration = self.details.get("retry_duration_seconds", 0)
            parts.append(
                f"  Retries exhausted: {retry_attempts}/{retry_attempts} "
                f"attempts over {duration:.1f}s"
            )

        return "\n".join(parts)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"message={self.message!r}, "
            f"code={self.code!r}, "
            f"operation={self.operation!r}, "
            f"channel={self.channel!r}, "
            f"is_retryable={self.is_retryable})"
        )


# ---------------------------------------------------------------------------
# Connection-related exceptions (ERR-1 subclasses with **kwargs forwarding)
# ---------------------------------------------------------------------------


class KubeMQConnectionError(KubeMQError):
    """Connection-related errors (does not shadow built-in ConnectionError)."""

    def __init__(self, message: str, **kwargs: Any) -> None:
        super().__init__(message, **kwargs)


class KubeMQAuthenticationError(KubeMQConnectionError):
    """Authentication or authorization failed."""

    def __init__(self, message: str, **kwargs: Any) -> None:
        kwargs.setdefault("is_retryable", False)
        super().__init__(message, **kwargs)


class KubeMQTimeoutError(KubeMQError):
    """Operation timed out (does not shadow built-in TimeoutError)."""

    def __init__(self, message: str, **kwargs: Any) -> None:
        kwargs.setdefault("is_retryable", True)
        kwargs.setdefault("code", ErrorCode.CONNECTION_TIMEOUT)
        super().__init__(message, **kwargs)


# Validation exceptions
class KubeMQValidationError(KubeMQError):
    """Input validation failed."""

    def __init__(self, message: str, **kwargs: Any) -> None:
        kwargs.setdefault("is_retryable", False)
        super().__init__(message, **kwargs)


# Operation exceptions
class KubeMQChannelError(KubeMQError):
    """Channel operation failed (create, delete, list)."""

    def __init__(self, message: str, **kwargs: Any) -> None:
        super().__init__(message, **kwargs)


class KubeMQMessageError(KubeMQError):
    """Message operation failed (send, receive)."""

    def __init__(self, message: str, **kwargs: Any) -> None:
        super().__init__(message, **kwargs)


class KubeMQTransactionError(KubeMQError):
    """Transaction operation failed (ack, reject, requeue)."""

    def __init__(self, message: str, **kwargs: Any) -> None:
        super().__init__(message, **kwargs)


class KubeMQConfigurationError(KubeMQError):
    """Invalid SDK configuration detected at construction or connection time.

    Non-retryable: the caller must fix the configuration.
    Examples: invalid TLS version, conflicting cert options, missing required fields.
    """

    def __init__(self, message: str, **kwargs: Any) -> None:
        kwargs.setdefault("code", ErrorCode.CONFIGURATION_ERROR)
        kwargs.setdefault("is_retryable", False)
        super().__init__(message, **kwargs)


class KubeMQCircuitOpenError(KubeMQError):
    """Circuit breaker is open, request rejected."""

    def __init__(self, message: str, **kwargs: Any) -> None:
        super().__init__(message, **kwargs)


# ---------------------------------------------------------------------------
# REQ-ERR-2: New error types (continued)
# ---------------------------------------------------------------------------


class KubeMQBufferFullError(KubeMQError):
    """Reconnection buffer is full; messages are being dropped.

    Classification: Backpressure. is_retryable=False.
    User should wait for reconnection to complete or increase buffer size.
    """

    def __init__(
        self,
        message: str,
        *,
        buffer_size: int = 0,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            message,
            code=ErrorCode.BUFFER_FULL,
            is_retryable=False,
            **kwargs,
        )
        self.buffer_size = buffer_size


class KubeMQCancellationError(KubeMQError):
    """Operation was cancelled by the caller.

    Classification: Cancellation. is_retryable=False.
    """

    def __init__(self, message: str, **kwargs: Any) -> None:
        super().__init__(
            message,
            code=ErrorCode.CANCELLED,
            is_retryable=False,
            **kwargs,
        )


# ---------------------------------------------------------------------------
# REQ-ERR-9: Async error propagation types
# ---------------------------------------------------------------------------


class KubeMQClientClosedError(KubeMQError):
    """Raised when an operation is attempted on a closed client.

    This is a terminal error — the client cannot be reopened.
    Create a new client instance instead.
    """

    def __init__(self, message: str = "Client is closed", **kwargs: Any) -> None:
        kwargs.setdefault("code", ErrorCode.CLIENT_CLOSED)
        kwargs.setdefault("is_retryable", False)
        super().__init__(message, **kwargs)


class KubeMQConnectionNotReadyError(KubeMQError):
    """Raised when an operation requires READY state but connection is not ready.

    Raised when wait_for_ready=False and state is CONNECTING or RECONNECTING.
    """

    def __init__(self, message: str = "Connection is not ready", **kwargs: Any) -> None:
        kwargs.setdefault("code", ErrorCode.CONNECTION_NOT_READY)
        kwargs.setdefault("is_retryable", False)
        super().__init__(message, **kwargs)


class KubeMQTransportError(KubeMQError):
    """Error originating from the transport layer (stream broken, auth expired, etc.).

    Distinct from KubeMQHandlerError to allow callers to differentiate
    infrastructure failures from application-level failures.
    """

    def __init__(self, message: str, **kwargs: Any) -> None:
        super().__init__(message, **kwargs)


class KubeMQHandlerError(KubeMQError):
    """Error from user-provided message handler code.

    The subscription continues processing subsequent messages.
    The original handler exception is available via __cause__.
    """

    def __init__(self, message: str, **kwargs: Any) -> None:
        kwargs.setdefault("is_retryable", False)
        super().__init__(message, **kwargs)


# ---------------------------------------------------------------------------
# REQ-ERR-8: Streaming error handling
# ---------------------------------------------------------------------------


class KubeMQStreamBrokenError(KubeMQError):
    """Stream was interrupted; in-flight messages may not have been acknowledged.

    Attributes:
        unacked_message_ids: List of message IDs sent but not acknowledged
            before the stream broke.
    """

    def __init__(
        self,
        message: str,
        *,
        unacked_message_ids: list[str] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            message,
            code=ErrorCode.STREAM_BROKEN,
            is_retryable=True,
            **kwargs,
        )
        self.unacked_message_ids = unacked_message_ids or []


# ---------------------------------------------------------------------------
# REQ-ERR-2: Classification table (after all error classes are defined)
# ---------------------------------------------------------------------------

ERROR_CLASSIFICATION: dict[ErrorCode, tuple[ErrorCategory, bool]] = {
    ErrorCode.UNAVAILABLE: (ErrorCategory.TRANSIENT, True),
    ErrorCode.ABORTED: (ErrorCategory.TRANSIENT, True),
    ErrorCode.CONNECTION_TIMEOUT: (ErrorCategory.TIMEOUT, True),
    ErrorCode.RESOURCE_EXHAUSTED: (ErrorCategory.THROTTLING, True),
    ErrorCode.AUTH_FAILED: (ErrorCategory.AUTHENTICATION, False),
    ErrorCode.PERMISSION_DENIED: (ErrorCategory.AUTHORIZATION, False),
    ErrorCode.VALIDATION_ERROR: (ErrorCategory.VALIDATION, False),
    ErrorCode.ALREADY_EXISTS: (ErrorCategory.VALIDATION, False),
    ErrorCode.OUT_OF_RANGE: (ErrorCategory.VALIDATION, False),
    ErrorCode.NOT_FOUND: (ErrorCategory.NOT_FOUND, False),
    ErrorCode.INTERNAL: (ErrorCategory.FATAL, False),
    ErrorCode.UNKNOWN: (ErrorCategory.TRANSIENT, True),
    ErrorCode.UNIMPLEMENTED: (ErrorCategory.FATAL, False),
    ErrorCode.DATA_LOSS: (ErrorCategory.FATAL, False),
    ErrorCode.CANCELLED: (ErrorCategory.CANCELLATION, False),
    ErrorCode.BUFFER_FULL: (ErrorCategory.BACKPRESSURE, False),
    ErrorCode.STREAM_BROKEN: (ErrorCategory.TRANSIENT, True),
    ErrorCode.CLIENT_CLOSED: (ErrorCategory.FATAL, False),
    ErrorCode.CONNECTION_NOT_READY: (ErrorCategory.TRANSIENT, True),
    ErrorCode.CONFIGURATION_ERROR: (ErrorCategory.VALIDATION, False),
}


def classify_error(error: KubeMQError) -> tuple[ErrorCategory, bool]:
    """Return (category, is_retryable) for an error.

    Falls back to (FATAL, False) for unknown codes.
    """
    if error.code is None:
        return (ErrorCategory.FATAL, False)
    return ERROR_CLASSIFICATION.get(error.code, (ErrorCategory.FATAL, False))


# ---------------------------------------------------------------------------
# REQ-AUTH-2: TLS handshake failure classification
# ---------------------------------------------------------------------------


def classify_tls_error(error: Exception) -> KubeMQError:
    """Classify a TLS-related error into the appropriate exception type.

    Classification rules per GS REQ-AUTH-2:
    - Certificate validation (expired, untrusted, hostname mismatch)
      -> KubeMQAuthenticationError (non-retryable)
    - Network error during handshake
      -> KubeMQConnectionError (retryable/transient)
    - TLS version/cipher negotiation failure
      -> KubeMQConfigurationError (non-retryable)
    """
    msg = str(error).lower()

    cert_keywords = (
        "certificate verify failed",
        "certificate has expired",
        "ssl: certificate_verify_failed",
        "hostname mismatch",
        "certificate_unknown",
        "bad_certificate",
        "certificate_expired",
        "certificate_revoked",
        "unknown_ca",
    )
    if any(kw in msg for kw in cert_keywords):
        return KubeMQAuthenticationError(
            f"TLS certificate validation failed: {error}",
            code=ErrorCode.AUTH_FAILED,
            cause=error,
            is_retryable=False,
        )

    negotiation_keywords = (
        "no shared cipher",
        "protocol_version",
        "tlsv1 alert",
        "unsupported protocol",
        "no protocols available",
        "handshake_failure",
    )
    if any(kw in msg for kw in negotiation_keywords):
        return KubeMQConfigurationError(
            f"TLS negotiation failed (check min_tls_version and cipher config): {error}",
            code=ErrorCode.CONFIGURATION_ERROR,
            cause=error,
        )

    return KubeMQConnectionError(
        f"TLS handshake failed due to network error: {error}",
        code=ErrorCode.UNAVAILABLE,
        cause=error,
        is_retryable=True,
    )


# ---------------------------------------------------------------------------
# REQ-ERR-6: gRPC error mapping (all 17 status codes)
# ---------------------------------------------------------------------------

try:
    import grpc
    import grpc.aio

    _GRPC_MAPPING: dict[
        grpc.StatusCode,
        tuple[type[KubeMQError], ErrorCode, bool],
    ] = {
        grpc.StatusCode.OK: (KubeMQError, ErrorCode.UNKNOWN, False),
        grpc.StatusCode.CANCELLED: (KubeMQCancellationError, ErrorCode.CANCELLED, False),
        grpc.StatusCode.UNKNOWN: (KubeMQError, ErrorCode.UNKNOWN, True),
        grpc.StatusCode.INVALID_ARGUMENT: (
            KubeMQValidationError,
            ErrorCode.VALIDATION_ERROR,
            False,
        ),
        grpc.StatusCode.DEADLINE_EXCEEDED: (KubeMQTimeoutError, ErrorCode.CONNECTION_TIMEOUT, True),
        grpc.StatusCode.NOT_FOUND: (KubeMQChannelError, ErrorCode.NOT_FOUND, False),
        grpc.StatusCode.ALREADY_EXISTS: (KubeMQValidationError, ErrorCode.ALREADY_EXISTS, False),
        grpc.StatusCode.PERMISSION_DENIED: (
            KubeMQAuthenticationError,
            ErrorCode.PERMISSION_DENIED,
            False,
        ),
        grpc.StatusCode.RESOURCE_EXHAUSTED: (
            KubeMQMessageError,
            ErrorCode.RESOURCE_EXHAUSTED,
            True,
        ),
        grpc.StatusCode.FAILED_PRECONDITION: (
            KubeMQValidationError,
            ErrorCode.VALIDATION_ERROR,
            False,
        ),
        grpc.StatusCode.ABORTED: (KubeMQTransactionError, ErrorCode.ABORTED, True),
        grpc.StatusCode.OUT_OF_RANGE: (KubeMQValidationError, ErrorCode.OUT_OF_RANGE, False),
        grpc.StatusCode.UNIMPLEMENTED: (KubeMQError, ErrorCode.UNIMPLEMENTED, False),
        grpc.StatusCode.INTERNAL: (KubeMQError, ErrorCode.INTERNAL, False),
        grpc.StatusCode.UNAVAILABLE: (KubeMQConnectionError, ErrorCode.UNAVAILABLE, True),
        grpc.StatusCode.DATA_LOSS: (KubeMQError, ErrorCode.DATA_LOSS, False),
        grpc.StatusCode.UNAUTHENTICATED: (KubeMQAuthenticationError, ErrorCode.AUTH_FAILED, False),
    }
    _HAS_GRPC = True
except ImportError:
    _HAS_GRPC = False
    _GRPC_MAPPING = {}


# String-based mapping for duck-typed gRPC-like errors
_STRING_GRPC_MAPPING: dict[str, tuple[type[KubeMQError], ErrorCode, bool]] = {
    "OK": (KubeMQError, ErrorCode.UNKNOWN, False),
    "CANCELLED": (KubeMQCancellationError, ErrorCode.CANCELLED, False),
    "UNKNOWN": (KubeMQError, ErrorCode.UNKNOWN, True),
    "INVALID_ARGUMENT": (KubeMQValidationError, ErrorCode.VALIDATION_ERROR, False),
    "DEADLINE_EXCEEDED": (KubeMQTimeoutError, ErrorCode.CONNECTION_TIMEOUT, True),
    "NOT_FOUND": (KubeMQChannelError, ErrorCode.NOT_FOUND, False),
    "ALREADY_EXISTS": (KubeMQValidationError, ErrorCode.ALREADY_EXISTS, False),
    "PERMISSION_DENIED": (KubeMQAuthenticationError, ErrorCode.PERMISSION_DENIED, False),
    "RESOURCE_EXHAUSTED": (KubeMQMessageError, ErrorCode.RESOURCE_EXHAUSTED, True),
    "FAILED_PRECONDITION": (KubeMQValidationError, ErrorCode.VALIDATION_ERROR, False),
    "ABORTED": (KubeMQTransactionError, ErrorCode.ABORTED, True),
    "OUT_OF_RANGE": (KubeMQValidationError, ErrorCode.OUT_OF_RANGE, False),
    "UNIMPLEMENTED": (KubeMQError, ErrorCode.UNIMPLEMENTED, False),
    "INTERNAL": (KubeMQError, ErrorCode.INTERNAL, False),
    "UNAVAILABLE": (KubeMQConnectionError, ErrorCode.UNAVAILABLE, True),
    "DATA_LOSS": (KubeMQError, ErrorCode.DATA_LOSS, False),
    "UNAUTHENTICATED": (KubeMQAuthenticationError, ErrorCode.AUTH_FAILED, False),
}


def from_grpc_error(
    error: Exception,
    *,
    operation: str = "",
    channel: str | None = None,
    cancellation_token: Any | None = None,
) -> KubeMQError:
    """Convert a gRPC error to the appropriate KubeMQ exception.

    Handles grpc.RpcError (sync), grpc.aio.AioRpcError (async),
    and duck-typed gRPC-like errors.

    All 17 gRPC status codes (0-16) are mapped. CANCELLED is split
    between client-initiated and server-initiated. Original gRPC
    error is preserved via __cause__ chaining.

    Args:
        error: The original gRPC or generic exception.
        operation: Name of the failed operation for error context.
        channel: Target channel name for error context.
        cancellation_token: Optional cancellation token for CANCELLED split.

    Returns:
        An appropriate KubeMQError subclass instance.
    """
    if not _HAS_GRPC:
        return KubeMQError(str(error), cause=error, operation=operation, channel=channel)

    import grpc
    import grpc.aio

    if not isinstance(error, grpc.RpcError):
        if hasattr(error, "code") and callable(error.code):
            return _convert_grpc_like_error(error, operation=operation, channel=channel)
        return KubeMQError(str(error), cause=error, operation=operation, channel=channel)

    grpc_code: grpc.StatusCode | None = None
    details = str(error)

    if hasattr(error, "code") and callable(error.code):
        grpc_code = error.code()
    if hasattr(error, "details") and callable(error.details):
        details = error.details() or str(error)

    # OK in an error handler indicates a bug
    if grpc_code == grpc.StatusCode.OK:
        logger.warning(
            "Received OK status in error handler — this indicates a bug (operation=%s, channel=%s)",
            operation,
            channel,
        )
        return KubeMQError(
            details or "Unexpected OK status",
            code=ErrorCode.UNKNOWN,
            cause=error,
            operation=operation,
            channel=channel,
        )

    # CANCELLED: split client-initiated vs server-initiated
    if grpc_code == grpc.StatusCode.CANCELLED:
        if _is_client_initiated_cancel(cancellation_token):
            return KubeMQCancellationError(
                details or "Operation cancelled by client",
                operation=operation,
                channel=channel,
                cause=error,
            )
        return KubeMQConnectionError(
            details or "Operation cancelled by server",
            code=ErrorCode.CANCELLED,
            is_retryable=True,
            operation=operation,
            channel=channel,
            cause=error,
        )

    mapping_entry = _GRPC_MAPPING.get(grpc_code)
    if mapping_entry is None:
        return KubeMQError(
            details or "Unknown gRPC error",
            code=ErrorCode.UNKNOWN,
            is_retryable=False,
            operation=operation,
            channel=channel,
            cause=error,
        )

    exc_class, error_code, retryable = mapping_entry

    rich_details = _extract_rich_details(error)

    return exc_class(
        details or "Unknown gRPC error",
        code=error_code,
        is_retryable=retryable,
        operation=operation,
        channel=channel,
        cause=error,
        details=rich_details,
    )


def _is_client_initiated_cancel(
    cancellation_token: Any | None = None,
) -> bool:
    """Check if the cancellation was initiated by the client."""
    if cancellation_token is not None:
        if hasattr(cancellation_token, "is_cancelled"):
            return bool(cancellation_token.is_cancelled)
        if hasattr(cancellation_token, "is_set") and callable(cancellation_token.is_set):
            return bool(cancellation_token.is_set())

    try:
        task = asyncio.current_task()
        if task is not None and task.cancelled():
            return True
    except RuntimeError:
        pass
    return False


_CREDENTIAL_PATTERNS = [
    re.compile(r"(bearer|authorization)\s*[:=]\s*\S+(\s+\S+)?", re.IGNORECASE),
    re.compile(r"eyJ[A-Za-z0-9_-]+\.eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+"),
]


def _scrub_credentials(text: str) -> str:
    """Remove potential credential material from gRPC error details.

    Applied to gRPC trailing metadata values, NOT to user-facing error
    messages. The patterns are intentionally narrow to minimise false
    positives:
    - JWT tokens (base64-encoded JSON with ``eyJ`` prefix)
    - Bearer/authorization header values
    """
    result = text
    for pattern in _CREDENTIAL_PATTERNS:
        result = pattern.sub("[REDACTED]", result)
    return result


def _extract_rich_details(error: Exception) -> dict[str, Any]:
    """Extract rich error details from gRPC trailing metadata.

    Credential material in metadata values is scrubbed before inclusion
    in the returned dict to prevent token leakage through error details.
    """
    details: dict[str, Any] = {}
    if hasattr(error, "trailing_metadata") and callable(error.trailing_metadata):
        try:
            metadata = error.trailing_metadata()
            if metadata:
                for key, value in metadata:
                    if key.startswith("grpc-"):
                        continue
                    scrubbed = _scrub_credentials(str(value)) if isinstance(value, str) else value
                    details[key] = scrubbed
        except Exception:
            pass
    return details


def _convert_grpc_like_error(
    error: Exception,
    *,
    operation: str = "",
    channel: str | None = None,
) -> KubeMQError:
    """Convert a duck-typed gRPC-like error to a KubeMQ exception."""
    raw_code = error.code() if hasattr(error, "code") and callable(error.code) else None
    details_raw = (
        error.details() if hasattr(error, "details") and callable(error.details) else str(error)
    )
    details = str(details_raw) if details_raw is not None else ""
    code_str = str(raw_code) if raw_code else ""

    mapping_entry = _STRING_GRPC_MAPPING.get(code_str)
    if mapping_entry is None:
        return KubeMQError(
            details or "Unknown gRPC error",
            code=ErrorCode.UNKNOWN,
            cause=error,
            operation=operation,
            channel=channel,
        )

    exc_class, error_code, retryable = mapping_entry
    return exc_class(
        details or "Unknown gRPC error",
        code=error_code,
        is_retryable=retryable,
        cause=error,
        operation=operation,
        channel=channel,
    )
