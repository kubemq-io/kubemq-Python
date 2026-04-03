from __future__ import annotations

import os

import grpc


def fast_id() -> str:
    """Generate a unique message ID using os.urandom (1.86x faster than uuid4).

    Produces a 32-character lowercase hex string with 128 bits of randomness,
    equivalent entropy to UUID4. Thread-safe (backed by OS CSPRNG).
    """
    return os.urandom(16).hex()


def decode_grpc_error(error: grpc.RpcError | Exception) -> str:
    """Decodes the error message from a gRPC error or general exception.

    Args:
        error: The gRPC error or exception to decode.

    Returns:
        str: The decoded error message.
    """
    if hasattr(error, "code") and error.code() == grpc.StatusCode.UNAVAILABLE:
        return "Connection Error: Server is unavailable"
    elif hasattr(error, "code") and error.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
        return "Timeout Error: Request has timed out"
    elif hasattr(error, "code") and error.code() == grpc.StatusCode.UNAUTHENTICATED:
        return "Authentication Error: Invalid authentication token"
    elif hasattr(error, "code") and error.code() == grpc.StatusCode.PERMISSION_DENIED:
        return "Permission Error: Permission denied"
    elif hasattr(error, "code") and error.code() == grpc.StatusCode.UNIMPLEMENTED:
        return "Unimplemented Error: The requested operation is not implemented"
    elif hasattr(error, "code") and error.code() == grpc.StatusCode.INTERNAL:
        return "Internal Error: An internal error has occurred"
    elif hasattr(error, "code") and error.code() == grpc.StatusCode.UNKNOWN:
        return "Unknown Error: An unknown error has occurred"
    elif hasattr(error, "details") and error.details():
        details = error.details()
        return details if details is not None else str(error)
    else:
        return str(error)


def is_channel_error(exception: Exception) -> bool:
    """Determines if an exception is related to channel connectivity issues.

    Args:
        exception (Exception): The exception to check

    Returns:
        bool: True if the exception is a channel error, False otherwise
    """
    # Check for common gRPC connectivity errors
    if isinstance(exception, grpc.RpcError):
        if hasattr(exception, "code") and exception.code() in [
            grpc.StatusCode.UNAVAILABLE,
            grpc.StatusCode.UNKNOWN,
            grpc.StatusCode.DEADLINE_EXCEEDED,
            grpc.StatusCode.CANCELLED,
        ]:
            return True
        return "Connection" in str(exception) or "channel" in str(exception).lower()

    # Check for other exception types that might be wrapping channel errors
    error_str = str(exception).lower()
    channel_error_phrases = [
        "channel closed",
        "cannot invoke rpc",
        "connection refused",
        "socket closed",
        "connection reset",
        "connection error",
        "not connected",
        "broken pipe",
        "transport failure",
    ]

    return any(phrase in error_str for phrase in channel_error_phrases)
