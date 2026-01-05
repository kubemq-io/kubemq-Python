"""Legacy exception module - imports from core for backward compatibility.

DEPRECATED: Import from kubemq.core.exceptions instead.
"""

from __future__ import annotations

import warnings
from typing import Any

from kubemq.core.exceptions import (
    KubeMQChannelError,
    KubeMQConnectionError,
    KubeMQError as BaseError,
    KubeMQMessageError,
    KubeMQTransactionError,
    KubeMQValidationError,
    from_grpc_error,
)

# Re-export new exceptions for gradual migration
__all__ = [
    "BaseError",
    "ValidationError",
    "ConnectionError",
    "SendEventError",
    "DeleteChannelError",
    "CreateChannelError",
    "ListChannelsError",
    "GRPCError",
    # New names (preferred)
    "KubeMQValidationError",
    "KubeMQConnectionError",
    "KubeMQMessageError",
    "KubeMQChannelError",
    "KubeMQTransactionError",
    "from_grpc_error",
]


class ValidationError(KubeMQValidationError):
    """DEPRECATED: Use KubeMQValidationError instead."""

    def __init__(self, message: Any) -> None:
        warnings.warn(
            "ValidationError is deprecated, use KubeMQValidationError",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(f"Validation Error: {message}")


class ConnectionError(KubeMQConnectionError):
    """DEPRECATED: Use KubeMQConnectionError instead."""

    def __init__(self, message: str) -> None:
        warnings.warn(
            "ConnectionError is deprecated, use KubeMQConnectionError",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(f"Connection Error: {message}")


class SendEventError(KubeMQMessageError):
    """DEPRECATED: Use KubeMQMessageError instead."""

    def __init__(self, message: str) -> None:
        warnings.warn(
            "SendEventError is deprecated, use KubeMQMessageError",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(f"Send Event Error: {message}")


class DeleteChannelError(KubeMQChannelError):
    """DEPRECATED: Use KubeMQChannelError instead."""

    def __init__(self, message: str) -> None:
        warnings.warn(
            "DeleteChannelError is deprecated, use KubeMQChannelError",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(f"Delete Channel Error: {message}")


class CreateChannelError(KubeMQChannelError):
    """DEPRECATED: Use KubeMQChannelError instead."""

    def __init__(self, message: str) -> None:
        warnings.warn(
            "CreateChannelError is deprecated, use KubeMQChannelError",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(f"Create Channel Error: {message}")


class ListChannelsError(KubeMQChannelError):
    """DEPRECATED: Use KubeMQChannelError instead."""

    def __init__(self, message: str) -> None:
        warnings.warn(
            "ListChannelsError is deprecated, use KubeMQChannelError",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(f"List Channels Error: {message}")


class GRPCError(Exception):
    """DEPRECATED: Use from_grpc_error() instead.

    This class inherits from Exception directly (not BaseError) to maintain
    backward compatibility with the original exception hierarchy.
    """

    def __init__(self, exc: str | Exception) -> None:
        warnings.warn(
            "GRPCError is deprecated, use from_grpc_error()",
            DeprecationWarning,
            stacklevel=2,
        )
        # Handle string messages (legacy decode_grpc_error usage)
        if isinstance(exc, str):
            self.message = exc
            self.code = None
            self.cause = None
            super().__init__(f"KubeMQ GRPC Error: {exc}")
            return

        grpc_exc = from_grpc_error(exc)
        # Store attributes for compatibility
        self.message = grpc_exc.message
        self.code = grpc_exc.code
        self.cause = exc
        # Build a formatted message similar to original behavior
        # Original format: "KubeMQ <ErrorType>: <details>"
        if isinstance(grpc_exc, KubeMQConnectionError):
            error_type = "KubeMQ Connection Error"
        elif isinstance(grpc_exc, KubeMQValidationError):
            error_type = "KubeMQ Validation Error"
        elif isinstance(grpc_exc, KubeMQMessageError):
            error_type = "KubeMQ Message Error"
        elif isinstance(grpc_exc, KubeMQChannelError):
            error_type = "KubeMQ Channel Error"
        else:
            error_type = "KubeMQ Error"

        formatted_message = (
            f"{error_type}: {grpc_exc.code}: {grpc_exc.message}"
            if grpc_exc.code
            else f"{error_type}: {grpc_exc.message}"
        )
        super().__init__(formatted_message)
