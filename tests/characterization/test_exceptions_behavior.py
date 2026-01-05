"""
Characterization tests for exceptions behavior.

These tests capture the CURRENT behavior of the SDK before refactoring.
They must pass both before AND after any refactoring to ensure
backward compatibility.
"""

import pytest

from kubemq.common.exceptions import (
    BaseError,
    ConnectionError,
    CreateChannelError,
    DeleteChannelError,
    GRPCError,
    ListChannelsError,
    SendEventError,
    ValidationError,
)


@pytest.mark.characterization
class TestExceptionHierarchy:
    """Characterization tests for exception hierarchy."""

    def test_validation_error_inherits_base_error(self):
        """Capture: ValidationError inherits from BaseError."""
        assert issubclass(ValidationError, BaseError)

    def test_connection_error_inherits_base_error(self):
        """Capture: ConnectionError inherits from BaseError."""
        assert issubclass(ConnectionError, BaseError)

    def test_send_event_error_inherits_base_error(self):
        """Capture: SendEventError inherits from BaseError."""
        assert issubclass(SendEventError, BaseError)

    def test_delete_channel_error_inherits_base_error(self):
        """Capture: DeleteChannelError inherits from BaseError."""
        assert issubclass(DeleteChannelError, BaseError)

    def test_create_channel_error_inherits_base_error(self):
        """Capture: CreateChannelError inherits from BaseError."""
        assert issubclass(CreateChannelError, BaseError)

    def test_list_channels_error_inherits_base_error(self):
        """Capture: ListChannelsError inherits from BaseError."""
        assert issubclass(ListChannelsError, BaseError)

    def test_grpc_error_inherits_exception(self):
        """Capture: GRPCError inherits from Exception (not BaseError)."""
        assert issubclass(GRPCError, Exception)
        assert not issubclass(GRPCError, BaseError)


@pytest.mark.characterization
class TestExceptionMessageFormat:
    """Characterization tests for exception message formatting."""

    def test_validation_error_message_format(self):
        """Capture: ValidationError prefixes message with 'Validation Error:'."""
        error = ValidationError("test message")
        assert "Validation Error:" in str(error)
        assert "test message" in str(error)

    def test_connection_error_message_format(self):
        """Capture: ConnectionError prefixes message with 'Connection Error:'."""
        error = ConnectionError("test message")
        assert "Connection Error:" in str(error)
        assert "test message" in str(error)

    def test_send_event_error_message_format(self):
        """Capture: SendEventError prefixes message with 'Send Event Error:'."""
        error = SendEventError("test message")
        assert "Send Event Error:" in str(error)
        assert "test message" in str(error)

    def test_delete_channel_error_message_format(self):
        """Capture: DeleteChannelError prefixes message with 'Delete Channel Error:'."""
        error = DeleteChannelError("test message")
        assert "Delete Channel Error:" in str(error)
        assert "test message" in str(error)

    def test_create_channel_error_message_format(self):
        """Capture: CreateChannelError prefixes message with 'Create Channel Error:'."""
        error = CreateChannelError("test message")
        assert "Create Channel Error:" in str(error)
        assert "test message" in str(error)

    def test_list_channels_error_message_format(self):
        """Capture: ListChannelsError prefixes message with 'List Channels Error:'."""
        error = ListChannelsError("test message")
        assert "List Channels Error:" in str(error)
        assert "test message" in str(error)


@pytest.mark.characterization
class TestGRPCErrorBehavior:
    """Characterization tests for GRPCError behavior."""

    def test_grpc_error_with_plain_exception(self):
        """Capture: GRPCError handles plain exceptions."""
        original = Exception("plain error")
        error = GRPCError(original)
        assert "plain error" in str(error)

    def test_grpc_error_with_grpc_exception(self):
        """Capture: GRPCError extracts status and details from gRPC errors."""

        # Mock a gRPC-like exception
        class MockGRPCError(Exception):
            def code(self):
                return "UNAVAILABLE"

            def details(self):
                return "Server unavailable"

        mock_error = MockGRPCError()
        error = GRPCError(mock_error)
        assert "KubeMQ Connection Error" in str(error)
        assert "UNAVAILABLE" in str(error)
        assert "Server unavailable" in str(error)
