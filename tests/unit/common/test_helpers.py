"""Unit tests for kubemq.common.helpers module.

Tests for decode_grpc_error and is_channel_error functions.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import grpc

from kubemq.common.helpers import decode_grpc_error, is_channel_error


def create_grpc_error(status_code, details_msg=None):
    """Create a mock gRPC error with proper code() method."""
    error = MagicMock()
    error.code = MagicMock(return_value=status_code)
    if details_msg:
        error.details = MagicMock(return_value=details_msg)
    else:
        error.details = MagicMock(return_value=None)
    # Make it an instance of grpc.RpcError
    error.__class__ = grpc.RpcError
    return error


class TestDecodeGrpcError:
    """Tests for decode_grpc_error function."""

    def test_unavailable_error(self):
        """Test decoding UNAVAILABLE error."""
        error = create_grpc_error(grpc.StatusCode.UNAVAILABLE)

        result = decode_grpc_error(error)

        assert "Server is unavailable" in result

    def test_deadline_exceeded_error(self):
        """Test decoding DEADLINE_EXCEEDED error."""
        error = create_grpc_error(grpc.StatusCode.DEADLINE_EXCEEDED)

        result = decode_grpc_error(error)

        assert "Timeout Error" in result

    def test_unauthenticated_error(self):
        """Test decoding UNAUTHENTICATED error."""
        error = create_grpc_error(grpc.StatusCode.UNAUTHENTICATED)

        result = decode_grpc_error(error)

        assert "Authentication Error" in result

    def test_permission_denied_error(self):
        """Test decoding PERMISSION_DENIED error."""
        error = create_grpc_error(grpc.StatusCode.PERMISSION_DENIED)

        result = decode_grpc_error(error)

        assert "Permission Error" in result

    def test_unimplemented_error(self):
        """Test decoding UNIMPLEMENTED error."""
        error = create_grpc_error(grpc.StatusCode.UNIMPLEMENTED)

        result = decode_grpc_error(error)

        assert "Unimplemented Error" in result

    def test_internal_error(self):
        """Test decoding INTERNAL error."""
        error = create_grpc_error(grpc.StatusCode.INTERNAL)

        result = decode_grpc_error(error)

        assert "Internal Error" in result

    def test_unknown_error(self):
        """Test decoding UNKNOWN error."""
        error = create_grpc_error(grpc.StatusCode.UNKNOWN)

        result = decode_grpc_error(error)

        assert "Unknown Error" in result

    def test_error_with_details(self):
        """Test decoding error with details."""
        error = create_grpc_error(grpc.StatusCode.INVALID_ARGUMENT, "Custom error details")

        result = decode_grpc_error(error)

        assert result == "Custom error details"

    def test_error_without_code_or_details(self):
        """Test decoding error without code or details."""
        error = MagicMock()
        # Create error without code attribute
        del error.code
        del error.details
        error.__str__ = MagicMock(return_value="Generic error message")

        result = decode_grpc_error(error)

        assert "Generic error message" in result


class TestIsChannelError:
    """Tests for is_channel_error function."""

    def test_unavailable_grpc_error(self):
        """Test UNAVAILABLE is detected as channel error."""
        error = create_grpc_error(grpc.StatusCode.UNAVAILABLE)

        assert is_channel_error(error) is True

    def test_deadline_exceeded_grpc_error(self):
        """Test DEADLINE_EXCEEDED is detected as channel error."""
        error = create_grpc_error(grpc.StatusCode.DEADLINE_EXCEEDED)

        assert is_channel_error(error) is True

    def test_cancelled_grpc_error(self):
        """Test CANCELLED is detected as channel error."""
        error = create_grpc_error(grpc.StatusCode.CANCELLED)

        assert is_channel_error(error) is True

    def test_unknown_grpc_error(self):
        """Test UNKNOWN is detected as channel error."""
        error = create_grpc_error(grpc.StatusCode.UNKNOWN)

        assert is_channel_error(error) is True

    def test_non_channel_grpc_error(self):
        """Test non-channel gRPC error is not detected as channel error."""
        error = create_grpc_error(grpc.StatusCode.INVALID_ARGUMENT)
        error.__str__ = MagicMock(return_value="Invalid argument")

        assert is_channel_error(error) is False

    def test_connection_refused_exception(self):
        """Test connection refused exception is detected as channel error."""
        error = Exception("Connection refused")

        assert is_channel_error(error) is True

    def test_channel_closed_exception(self):
        """Test channel closed exception is detected as channel error."""
        error = Exception("channel closed during operation")

        assert is_channel_error(error) is True

    def test_socket_closed_exception(self):
        """Test socket closed exception is detected as channel error."""
        error = Exception("socket closed unexpectedly")

        assert is_channel_error(error) is True

    def test_connection_reset_exception(self):
        """Test connection reset exception is detected as channel error."""
        error = Exception("connection reset by peer")

        assert is_channel_error(error) is True

    def test_broken_pipe_exception(self):
        """Test broken pipe exception is detected as channel error."""
        error = Exception("broken pipe")

        assert is_channel_error(error) is True

    def test_transport_failure_exception(self):
        """Test transport failure exception is detected as channel error."""
        error = Exception("transport failure occurred")

        assert is_channel_error(error) is True

    def test_not_connected_exception(self):
        """Test not connected exception is detected as channel error."""
        error = Exception("client not connected to server")

        assert is_channel_error(error) is True

    def test_non_channel_exception(self):
        """Test non-channel exception is not detected as channel error."""
        error = Exception("Invalid parameter value")

        assert is_channel_error(error) is False

    def test_grpc_error_with_connection_in_message(self):
        """Test gRPC error with 'Connection' in message."""
        error = create_grpc_error(grpc.StatusCode.INTERNAL)
        error.__str__ = MagicMock(return_value="Connection lost to server")

        assert is_channel_error(error) is True
