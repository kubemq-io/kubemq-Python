"""Unit tests for kubemq.queues.client module.

Tests for the sync Queues Client class.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kubemq.core.config import ClientConfig
from kubemq.queues.client import Client
from kubemq.queues.queues_message import QueueMessage
from kubemq.queues.queues_messages_waiting_pulled import (
    QueueMessagesPulled,
    QueueMessagesWaiting,
)
from kubemq.queues.queues_poll_response import QueuesPollResponse
from kubemq.queues.queues_send_result import QueueSendResult

# ==============================================================================
# Initialization Tests
# ==============================================================================


class TestQueuesClientInit:
    """Tests for Queues Client initialization."""

    def test_init_with_address(self):
        """Test initialization with address parameter."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            assert client._config.address == "localhost:50000"

    def test_init_with_config_object(self):
        """Test initialization with ClientConfig object."""
        config = ClientConfig(
            address="localhost:50000",
            client_id="test-client",
        )

        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            client = Client(config=config)

            assert client._config.address == "localhost:50000"
            assert client._config.client_id == "test-client"

    def test_init_with_client_id(self):
        """Test initialization with client_id parameter."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000", client_id="my-client")

            assert client._config.client_id == "my-client"

    def test_init_with_auth_token(self):
        """Test initialization with auth_token parameter."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            client = Client(
                address="localhost:50000",
                auth_token="test-token",
            )

            assert client._config.auth_token == "test-token"

    def test_init_with_queues_specific_params(self):
        """Test initialization with queues-specific parameters."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            client = Client(
                address="localhost:50000",
                send_timeout=5.0,
                connection_monitor_interval=2.0,
            )

            assert client.send_timeout == 5.0
            assert client.connection_monitor_interval == 2.0


# ==============================================================================
# Context Manager Tests
# ==============================================================================


class TestQueuesClientContextManager:
    """Tests for Queues Client context manager."""

    def test_sync_context_manager(self):
        """Test sync context manager (__enter__/__exit__)."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            with Client(address="localhost:50000") as client:
                assert client is not None

            # Verify close was called
            mock_transport.close.assert_called()

    @pytest.mark.asyncio
    async def test_async_context_manager(self):
        """Test async context manager (__aenter__/__aexit__)."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport.close_async = AsyncMock()
            mock_transport_class.return_value = mock_transport

            async with Client(address="localhost:50000") as client:
                assert client is not None

            # Verify close_async was called
            mock_transport.close_async.assert_called()


# ==============================================================================
# Send Message Tests
# ==============================================================================


class TestQueuesClientSendMessage:
    """Tests for Queues Client send_queues_message method."""

    def test_send_queues_message_returns_result(self):
        """Test send_queues_message returns QueueSendResult."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            # Mock the upstream sender
            mock_sender = MagicMock()
            mock_result = QueueSendResult(
                id="msg-123",
                sent_at=1234567890,
                expired_at=0,
                delayed_to=0,
                is_error=False,
                error="",
            )
            mock_sender.send.return_value = mock_result
            client._upstream_sender = mock_sender

            message = QueueMessage(
                channel="test-queue",
                body=b"test body",
            )

            result = client.send_queues_message(message)

            assert result.id == "msg-123"
            assert result.is_error is False
            # The message is encoded to protobuf before being sent
            mock_sender.send.assert_called_once()
            call_args = mock_sender.send.call_args[0][0]
            assert call_args.Channel == "test-queue"
            assert call_args.Body == b"test body"

    def test_send_queues_message_with_error(self):
        """Test send_queues_message handles errors."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            # Mock the upstream sender with error response
            mock_sender = MagicMock()
            mock_result = QueueSendResult(
                id="",
                sent_at=0,
                expired_at=0,
                delayed_to=0,
                is_error=True,
                error="Send failed",
            )
            mock_sender.send.return_value = mock_result
            client._upstream_sender = mock_sender

            message = QueueMessage(
                channel="test-queue",
                body=b"test body",
            )

            result = client.send_queues_message(message)

            assert result.is_error is True
            assert result.error == "Send failed"


# ==============================================================================
# Channel Management Tests
# ==============================================================================


class TestQueuesClientChannelManagement:
    """Tests for Queues Client channel management."""

    def test_create_queues_channel(self):
        """Test create_queues_channel creates a channel."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            with patch("kubemq.queues.client.create_channel_request") as mock_create:
                mock_create.return_value = True

                client = Client(address="localhost:50000")

                result = client.create_queues_channel("test-queue")

                assert result is True
                mock_create.assert_called_once_with(
                    client._transport,
                    client._config.client_id,
                    "test-queue",
                    "queues",
                )

    def test_delete_queues_channel(self):
        """Test delete_queues_channel deletes a channel."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            with patch("kubemq.queues.client.delete_channel_request") as mock_delete:
                mock_delete.return_value = True

                client = Client(address="localhost:50000")

                result = client.delete_queues_channel("test-queue")

                assert result is True
                mock_delete.assert_called_once_with(
                    client._transport,
                    client._config.client_id,
                    "test-queue",
                    "queues",
                )

    def test_list_queues_channels(self):
        """Test list_queues_channels returns channel list."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            with patch("kubemq.queues.client.list_queues_channels") as mock_list:
                mock_channel = MagicMock()
                mock_channel.name = "test-queue"
                mock_list.return_value = [mock_channel]

                client = Client(address="localhost:50000")

                result = client.list_queues_channels()

                assert len(result) == 1
                assert result[0].name == "test-queue"
                mock_list.assert_called_once_with(
                    client._transport,
                    client._config.client_id,
                    "",
                )

    def test_list_queues_channels_with_search(self):
        """Test list_queues_channels with search filter."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            with patch("kubemq.queues.client.list_queues_channels") as mock_list:
                mock_list.return_value = []

                client = Client(address="localhost:50000")

                client.list_queues_channels(channel_search="test")

                mock_list.assert_called_once_with(
                    client._transport,
                    client._config.client_id,
                    "test",
                )


# ==============================================================================
# Receive Messages Tests
# ==============================================================================


class TestQueuesClientReceiveMessages:
    """Tests for Queues Client receive_queues_messages method."""

    def test_receive_queues_messages(self):
        """Test receive_queues_messages receives messages."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            # Mock the downstream receiver
            mock_receiver = MagicMock()
            mock_response = MagicMock()
            mock_response.RefRequestId = "req-123"
            mock_response.TransactionId = "tx-123"
            mock_response.IsError = False
            mock_response.Error = ""
            mock_response.Messages = []
            mock_response.ActiveOffsets = []
            mock_receiver.send.return_value = mock_response
            client._downstream_receiver = mock_receiver

            result = client.receive_queues_messages(
                channel="test-queue",
                max_messages=10,
                wait_timeout_in_seconds=30,
            )

            assert isinstance(result, QueuesPollResponse)

    def test_receive_queues_messages_with_auto_ack(self):
        """Test receive_queues_messages with auto_ack enabled."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            # Mock the downstream receiver
            mock_receiver = MagicMock()
            mock_response = MagicMock()
            mock_response.RefRequestId = "req-123"
            mock_response.TransactionId = "tx-123"
            mock_response.IsError = False
            mock_response.Error = ""
            mock_response.Messages = []
            mock_response.ActiveOffsets = []
            mock_receiver.send.return_value = mock_response
            client._downstream_receiver = mock_receiver

            client.receive_queues_messages(
                channel="test-queue",
                auto_ack=True,
            )

            # Verify the request was made
            mock_receiver.send.assert_called_once()
            # Check that AutoAck was set in the request
            call_args = mock_receiver.send.call_args[0][0]
            assert call_args.AutoAck is True


# ==============================================================================
# Waiting (Peek) Tests
# ==============================================================================


class TestQueuesClientWaiting:
    """Tests for Queues Client waiting (peek) method."""

    def test_waiting_returns_messages(self):
        """Test waiting returns waiting messages."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True

            # Mock the gRPC client
            mock_grpc_client = MagicMock()
            mock_response = MagicMock()
            mock_response.IsError = False
            mock_response.Error = ""
            mock_response.Messages = []
            mock_grpc_client.ReceiveQueueMessages.return_value = mock_response
            mock_transport.kubemq_client.return_value = mock_grpc_client
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            result = client.waiting(
                channel="test-queue",
                max_messages=5,
                wait_timeout_in_seconds=10,
            )

            assert isinstance(result, QueueMessagesWaiting)
            assert result.is_error is False

    def test_waiting_raises_on_none_channel(self):
        """Test waiting raises ValueError when channel is None."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            with pytest.raises(ValueError, match="channel cannot be None"):
                client.waiting(
                    channel=None,
                    max_messages=5,
                    wait_timeout_in_seconds=10,
                )

    def test_waiting_raises_on_invalid_max_messages(self):
        """Test waiting raises ValueError when max_messages < 1."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            with pytest.raises(ValueError, match="max_messages must be greater than 0"):
                client.waiting(
                    channel="test-queue",
                    max_messages=0,
                    wait_timeout_in_seconds=10,
                )

    def test_waiting_raises_on_invalid_timeout(self):
        """Test waiting raises ValueError when wait_timeout < 1."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            with pytest.raises(ValueError, match="wait_timeout_in_seconds must be greater than 0"):
                client.waiting(
                    channel="test-queue",
                    max_messages=5,
                    wait_timeout_in_seconds=0,
                )


# ==============================================================================
# Pull Tests
# ==============================================================================


class TestQueuesClientPull:
    """Tests for Queues Client pull method."""

    def test_pull_returns_messages(self):
        """Test pull returns pulled messages."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True

            # Mock the gRPC client
            mock_grpc_client = MagicMock()
            mock_response = MagicMock()
            mock_response.IsError = False
            mock_response.Error = ""
            mock_response.Messages = []
            mock_grpc_client.ReceiveQueueMessages.return_value = mock_response
            mock_transport.kubemq_client.return_value = mock_grpc_client
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            result = client.pull(
                channel="test-queue",
                max_messages=5,
                wait_timeout_in_seconds=10,
            )

            assert isinstance(result, QueueMessagesPulled)
            assert result.is_error is False

    def test_pull_raises_on_none_channel(self):
        """Test pull raises ValueError when channel is None."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            with pytest.raises(ValueError, match="channel cannot be None"):
                client.pull(
                    channel=None,
                    max_messages=5,
                    wait_timeout_in_seconds=10,
                )

    def test_pull_raises_on_invalid_max_messages(self):
        """Test pull raises ValueError when max_messages < 1."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            with pytest.raises(ValueError, match="max_messages must be greater than 0"):
                client.pull(
                    channel="test-queue",
                    max_messages=0,
                    wait_timeout_in_seconds=10,
                )

    def test_pull_raises_on_invalid_timeout(self):
        """Test pull raises ValueError when wait_timeout < 1."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            with pytest.raises(ValueError, match="wait_timeout_in_seconds must be greater than 0"):
                client.pull(
                    channel="test-queue",
                    max_messages=5,
                    wait_timeout_in_seconds=0,
                )


# ==============================================================================
# Upstream/Downstream Getter Tests
# ==============================================================================


class TestQueuesClientGetters:
    """Tests for Queues Client lazy initialization getters."""

    def test_get_upstream_sender_creates_sender(self):
        """Test _get_upstream_sender creates UpstreamSender lazily."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            with patch("kubemq.queues.client.UpstreamSender") as mock_sender_class:
                mock_sender = MagicMock()
                mock_sender_class.return_value = mock_sender

                client = Client(address="localhost:50000")
                client._upstream_sender = None  # Reset to test lazy init

                result = client._get_upstream_sender()

                assert result == mock_sender
                mock_sender_class.assert_called_once()

    def test_get_downstream_receiver_creates_receiver(self):
        """Test _get_downstream_receiver creates DownstreamReceiver lazily."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            with patch("kubemq.queues.client.DownstreamReceiver") as mock_receiver_class:
                mock_receiver = MagicMock()
                mock_receiver_class.return_value = mock_receiver

                client = Client(address="localhost:50000")
                client._downstream_receiver = None  # Reset to test lazy init

                result = client._get_downstream_receiver()

                assert result == mock_receiver
                mock_receiver_class.assert_called_once()


# ==============================================================================
# Cleanup Tests
# ==============================================================================


class TestQueuesClientCleanup:
    """Tests for Queues Client resource cleanup."""

    def test_cleanup_resources_closes_senders(self):
        """Test _cleanup_resources closes upstream and downstream."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            # Mock the senders
            mock_upstream = MagicMock()
            mock_downstream = MagicMock()
            client._upstream_sender = mock_upstream
            client._downstream_receiver = mock_downstream

            client._cleanup_resources()

            mock_upstream.close.assert_called_once()
            mock_downstream.close.assert_called_once()
            assert client._upstream_sender is None
            assert client._downstream_receiver is None

    @pytest.mark.asyncio
    async def test_close_async_closes_resources(self):
        """Test close_async closes all resources."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport.close_async = AsyncMock()
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            # Mock the senders
            mock_upstream = MagicMock()
            mock_downstream = MagicMock()
            client._upstream_sender = mock_upstream
            client._downstream_receiver = mock_downstream

            await client.close_async()

            mock_upstream.close.assert_called_once()
            mock_downstream.close.assert_called_once()
            mock_transport.close_async.assert_called_once()
