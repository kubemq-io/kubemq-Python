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

    def test_peek_queue_messages_returns_messages(self):
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

            result = client.peek_queue_messages(
                channel="test-queue",
                max_messages=5,
                wait_timeout_in_seconds=10,
            )

            assert isinstance(result, QueueMessagesWaiting)
            assert result.is_error is False

    def test_peek_queue_messages_raises_on_none_channel(self):
        """Test waiting raises ValueError when channel is None."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            with pytest.raises(ValueError, match="channel cannot be None"):
                client.peek_queue_messages(
                    channel=None,
                    max_messages=5,
                    wait_timeout_in_seconds=10,
                )

    def test_peek_queue_messages_raises_on_invalid_max_messages(self):
        """Test waiting raises ValueError when max_messages < 1."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            with pytest.raises(ValueError, match="max_messages must be between 1 and 1024"):
                client.peek_queue_messages(
                    channel="test-queue",
                    max_messages=0,
                    wait_timeout_in_seconds=10,
                )

    def test_peek_queue_messages_raises_on_invalid_timeout(self):
        """Test waiting raises ValueError when wait_timeout < 1."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            with pytest.raises(
                ValueError, match="wait_timeout_in_seconds must be between 1 and 3600"
            ):
                client.peek_queue_messages(
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

            with pytest.raises(ValueError, match="max_messages must be between 1 and 1024"):
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

            with pytest.raises(
                ValueError, match="wait_timeout_in_seconds must be between 1 and 3600"
            ):
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


# ==============================================================================
# Additional Coverage Tests
# ==============================================================================

import warnings  # noqa: E402

import grpc  # noqa: E402

from kubemq.core.exceptions import KubeMQValidationError  # noqa: E402


class FakeRpcError(grpc.RpcError):
    """Minimal stub for gRPC errors."""

    def code(self):
        return grpc.StatusCode.UNAVAILABLE

    def details(self):
        return "server unavailable"


class TestQueuesClientCloseAsyncAdditional:
    """Additional tests for close_async covering lines 182-194."""

    @pytest.mark.asyncio
    async def test_close_async_sets_shutdown_event(self):
        """Test close_async sets shutdown event."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport.close_async = AsyncMock()
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")
            assert not client._shutdown_event.is_set()

            await client.close_async()

            assert client._shutdown_event.is_set()
            mock_transport.close_async.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_async_no_transport(self):
        """Test close_async handles None transport gracefully."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")
            client._transport = None
            client._upstream_sender = None
            client._downstream_receiver = None

            await client.close_async()

            assert client._shutdown_event.is_set()

    @pytest.mark.asyncio
    async def test_close_async_without_senders(self):
        """Test close_async when upstream/downstream are None."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport.close_async = AsyncMock()
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")
            client._upstream_sender = None
            client._downstream_receiver = None

            await client.close_async()

            assert client._shutdown_event.is_set()
            mock_transport.close_async.assert_called_once()


class TestQueuesClientMonitorConnection:
    """Tests for _monitor_connection covering lines 220-231."""

    def test_monitor_connection_detects_status_change(self):
        """Test _monitor_connection logs status changes."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            mock_transport.is_connected.side_effect = [False, True, True]

            call_count = 0
            __import__("time").sleep

            def fake_sleep(interval):
                nonlocal call_count
                call_count += 1
                if call_count >= 3:
                    client._shutdown_event.set()

            with patch("kubemq.queues.client.time.sleep", side_effect=fake_sleep):
                client._monitor_connection()

    def test_monitor_connection_exits_on_shutdown(self):
        """Test _monitor_connection exits when shutdown event is set."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")
            client._shutdown_event.set()

            client._monitor_connection()


class TestQueuesClientPingAsync:
    """Tests for ping_async covering line 209."""

    @pytest.mark.asyncio
    async def test_ping_async_delegates_to_sync(self):
        """Test ping_async calls sync ping via run_in_thread."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport.ping.return_value = MagicMock(
                host="localhost",
                version="1.0",
                server_start_time=0,
                server_up_time_seconds=100,
            )
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                result = await client.ping_async()

                deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
                assert len(deprecation_warnings) > 0
            assert result is not None


class TestQueuesClientChannelManagementAdditional:
    """Additional tests for channel management covering lines 339-405."""

    @pytest.mark.asyncio
    async def test_create_queues_channel_async(self):
        """Test create_queues_channel_async delegates to sync."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            with patch("kubemq.queues.client.create_channel_request") as mock_create:
                mock_create.return_value = True

                client = Client(address="localhost:50000")
                result = await client.create_queues_channel_async("test-queue")

                assert result is True

    @pytest.mark.asyncio
    async def test_delete_queues_channel_async(self):
        """Test delete_queues_channel_async delegates to sync."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            with patch("kubemq.queues.client.delete_channel_request") as mock_delete:
                mock_delete.return_value = True

                client = Client(address="localhost:50000")
                result = await client.delete_queues_channel_async("test-queue")

                assert result is True

    @pytest.mark.asyncio
    async def test_list_queues_channels_async(self):
        """Test list_queues_channels_async delegates to sync."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            with patch("kubemq.queues.client.list_queues_channels") as mock_list:
                mock_list.return_value = []

                client = Client(address="localhost:50000")
                result = await client.list_queues_channels_async()

                assert result == []


class TestQueuesClientPullAdditional:
    """Additional tests for pull covering lines 275-293."""

    def test_pull_returns_empty_when_no_messages(self):
        """Test pull returns empty QueueMessagesPulled when response.Messages is empty."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True

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
            assert len(result.messages) == 0

    def test_pull_returns_messages_when_present(self):
        """Test pull returns decoded messages when response has messages (lines 649-653)."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True

            mock_grpc_client = MagicMock()
            mock_response = MagicMock()
            mock_response.IsError = False
            mock_response.Error = ""

            mock_msg = MagicMock()
            mock_msg.MessageID = "msg-1"
            mock_msg.Channel = "test-queue"
            mock_msg.Metadata = ""
            mock_msg.Body = b"hello"
            mock_msg.ClientID = "sender"
            mock_msg.Tags = {}
            mock_msg.Attributes.Timestamp = 0
            mock_msg.Attributes.Sequence = 1
            mock_msg.Attributes.ReceiveCount = 0
            mock_msg.Attributes.ReRouted = False
            mock_msg.Attributes.ReRoutedFromQueue = ""
            mock_msg.Attributes.ExpirationAt = 0
            mock_msg.Attributes.DelayedTo = 0

            mock_response.Messages = [mock_msg]
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
            assert len(result.messages) == 1


class TestQueuesClientPullAsync:
    """Tests for pull_async covering lines 582-586."""

    @pytest.mark.asyncio
    async def test_pull_async_delegates_to_sync(self):
        """Test pull_async delegates to sync pull."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True

            mock_grpc_client = MagicMock()
            mock_response = MagicMock()
            mock_response.IsError = False
            mock_response.Error = ""
            mock_response.Messages = []
            mock_grpc_client.ReceiveQueueMessages.return_value = mock_response
            mock_transport.kubemq_client.return_value = mock_grpc_client
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            result = await client.pull_async(
                channel="test-queue",
                max_messages=5,
                wait_timeout_in_seconds=10,
            )

            assert isinstance(result, QueueMessagesPulled)


class TestQueuesClientWaitingAdditional:
    """Additional tests for waiting covering lines 604, 649-653, 671."""

    def test_peek_queue_messages_returns_empty_messages(self):
        """Test waiting returns empty QueueMessagesWaiting when no messages."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True

            mock_grpc_client = MagicMock()
            mock_response = MagicMock()
            mock_response.IsError = False
            mock_response.Error = ""
            mock_response.Messages = []
            mock_grpc_client.ReceiveQueueMessages.return_value = mock_response
            mock_transport.kubemq_client.return_value = mock_grpc_client
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            result = client.peek_queue_messages(
                channel="test-queue",
                max_messages=5,
                wait_timeout_in_seconds=10,
            )

            assert isinstance(result, QueueMessagesWaiting)
            assert len(result.messages) == 0

    def test_peek_queue_messages_returns_messages_when_present(self):
        """Test waiting returns decoded messages when response has messages."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True

            mock_grpc_client = MagicMock()
            mock_response = MagicMock()
            mock_response.IsError = False
            mock_response.Error = ""

            mock_msg = MagicMock()
            mock_msg.MessageID = "msg-1"
            mock_msg.Channel = "test-queue"
            mock_msg.Metadata = ""
            mock_msg.Body = b"hello"
            mock_msg.ClientID = "sender"
            mock_msg.Tags = {}
            mock_msg.Attributes.Timestamp = 0
            mock_msg.Attributes.Sequence = 1
            mock_msg.Attributes.ReceiveCount = 0
            mock_msg.Attributes.ReRouted = False
            mock_msg.Attributes.ReRoutedFromQueue = ""
            mock_msg.Attributes.ExpirationAt = 0
            mock_msg.Attributes.DelayedTo = 0

            mock_response.Messages = [mock_msg]
            mock_grpc_client.ReceiveQueueMessages.return_value = mock_response
            mock_transport.kubemq_client.return_value = mock_grpc_client
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            result = client.peek_queue_messages(
                channel="test-queue",
                max_messages=5,
                wait_timeout_in_seconds=10,
            )

            assert isinstance(result, QueueMessagesWaiting)
            assert len(result.messages) == 1

    @pytest.mark.asyncio
    async def test_peek_queue_messages_async_delegates_to_sync(self):
        """Test peek_queue_messages_async delegates to sync waiting (line 604)."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True

            mock_grpc_client = MagicMock()
            mock_response = MagicMock()
            mock_response.IsError = False
            mock_response.Error = ""
            mock_response.Messages = []
            mock_grpc_client.ReceiveQueueMessages.return_value = mock_response
            mock_transport.kubemq_client.return_value = mock_grpc_client
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            result = await client.peek_queue_messages_async(
                channel="test-queue",
                max_messages=5,
                wait_timeout_in_seconds=10,
            )

            assert isinstance(result, QueueMessagesWaiting)


class TestQueuesClientDeprecatedMethods:
    """Tests for deprecated methods covering lines 405, 434, 445-448, 530."""

    def test_send_queues_message_emits_deprecation_warning(self):
        """Test send_queues_message emits deprecation warning (line 405)."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

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

            message = QueueMessage(channel="test-queue", body=b"test")

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                result = client.send_queues_message(message)

                deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
                assert len(deprecation_warnings) > 0

            assert result.is_error is False

    def test_receive_queues_messages_emits_deprecation_warning(self):
        """Test receive_queues_messages emits deprecation warning (line 434)."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            mock_receiver = MagicMock()
            mock_response = MagicMock()
            mock_response.RefRequestId = "req-1"
            mock_response.TransactionId = "tx-1"
            mock_response.IsError = False
            mock_response.Error = ""
            mock_response.Messages = []
            mock_response.ActiveOffsets = []
            mock_receiver.send.return_value = mock_response
            client._downstream_receiver = mock_receiver

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                result = client.receive_queues_messages(channel="test-queue")

                deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
                assert len(deprecation_warnings) > 0

            assert isinstance(result, QueuesPollResponse)

    @pytest.mark.asyncio
    async def test_receive_queues_messages_async_delegates(self):
        """Test receive_queues_messages_async delegates to sync (lines 445-448)."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            mock_receiver = MagicMock()
            mock_response = MagicMock()
            mock_response.RefRequestId = "req-1"
            mock_response.TransactionId = "tx-1"
            mock_response.IsError = False
            mock_response.Error = ""
            mock_response.Messages = []
            mock_response.ActiveOffsets = []
            mock_receiver.send.return_value = mock_response
            client._downstream_receiver = mock_receiver

            result = await client.receive_queues_messages_async(channel="test-queue")

            assert isinstance(result, QueuesPollResponse)

    def test_send_queue_message_returns_error_on_none_result(self):
        """Test send_queue_message returns error result when sender returns None (line 284)."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            mock_sender = MagicMock()
            mock_sender.send.return_value = None
            client._upstream_sender = mock_sender

            message = QueueMessage(channel="test-queue", body=b"test")
            result = client.send_queue_message(message)

            assert result.is_error is True
            assert "no response" in result.error.lower()

    def test_send_queue_message_validation_error(self):
        """Test send_queue_message wraps ValueError in KubeMQValidationError."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            mock_sender = MagicMock()
            client._upstream_sender = mock_sender

            message = QueueMessage(channel="test-queue", body=b"test")

            validation_err = ValueError("QueueMessage validation failed")
            with patch.object(QueueMessage, "encode_message", side_effect=validation_err):
                with pytest.raises(KubeMQValidationError) as exc_info:
                    client.send_queue_message(message)
                assert exc_info.value.__cause__ is validation_err

    @pytest.mark.asyncio
    async def test_send_queues_message_async_emits_deprecation(self):
        """Test send_queues_message_async emits deprecation warning (line 530)."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            mock_sender = MagicMock()
            mock_result = QueueSendResult(
                id="msg-1",
                sent_at=0,
                expired_at=0,
                delayed_to=0,
                is_error=False,
                error="",
            )
            mock_sender.send.return_value = mock_result
            client._upstream_sender = mock_sender

            message = QueueMessage(channel="test-queue", body=b"test")

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                await client.send_queues_message_async(message)

                deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
                assert len(deprecation_warnings) > 0

    def test_receive_queue_messages_none_response(self):
        """Test receive_queue_messages returns empty when receiver returns None."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            mock_receiver = MagicMock()
            mock_receiver.send.return_value = None
            client._downstream_receiver = mock_receiver

            result = client.receive_queue_messages(channel="test-queue")

            assert isinstance(result, QueuesPollResponse)


class TestSyncClientAckAllQueueMessages:
    """GAP-H10: Tests for ack_all_queue_messages on sync client."""

    def test_ack_all_queue_messages_calls_transport(self):
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_stub = MagicMock()
            mock_resp = MagicMock()
            mock_resp.IsError = False
            mock_resp.AffectedMessages = 42
            mock_stub.AckAllQueueMessages.return_value = mock_resp
            mock_transport.kubemq_client.return_value = mock_stub
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")
            result = client.ack_all_queue_messages("test-queue")

            assert result == 42
            mock_stub.AckAllQueueMessages.assert_called_once()

    def test_ack_all_queue_messages_returns_affected_count(self):
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_stub = MagicMock()
            mock_resp = MagicMock()
            mock_resp.IsError = False
            mock_resp.AffectedMessages = 7
            mock_stub.AckAllQueueMessages.return_value = mock_resp
            mock_transport.kubemq_client.return_value = mock_stub
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")
            result = client.ack_all_queue_messages("q", wait_time_seconds=30)

            req_arg = mock_stub.AckAllQueueMessages.call_args[0][0]
            assert req_arg.WaitTimeSeconds == 30
            assert result == 7


# ==============================================================================
# Extended Coverage Tests — 95% target
# ==============================================================================


class TestSendQueueMessageSimple:
    """Tests for send_queue_message_simple() unary RPC path (lines 300-344)."""

    def test_send_queue_message_simple_success(self):
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True

            mock_grpc_client = MagicMock()
            mock_pb_result = MagicMock()
            mock_pb_result.MessageID = "msg-simple-1"
            mock_pb_result.SentAt = 0
            mock_pb_result.ExpirationAt = 0
            mock_pb_result.DelayedTo = 0
            mock_pb_result.IsError = False
            mock_pb_result.Error = ""
            mock_grpc_client.SendQueueMessage.return_value = mock_pb_result
            mock_transport.kubemq_client.return_value = mock_grpc_client
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")
            message = QueueMessage(channel="test-queue", body=b"hello")

            result = client.send_queue_message_simple(message)

            assert result is not None
            mock_grpc_client.SendQueueMessage.assert_called_once()

    def test_send_queue_message_simple_validation_error(self):
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")
            message = QueueMessage(channel="test-queue", body=b"test")

            validation_err = ValueError("QueueMessage validation failed")
            with patch.object(QueueMessage, "encode_message", side_effect=validation_err):
                with pytest.raises(KubeMQValidationError) as exc_info:
                    client.send_queue_message_simple(message)
                assert exc_info.value.__cause__ is validation_err

    def test_send_queue_message_simple_transport_error(self):
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True

            mock_grpc_client = MagicMock()
            mock_grpc_client.SendQueueMessage.side_effect = FakeRpcError()
            mock_transport.kubemq_client.return_value = mock_grpc_client
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")
            message = QueueMessage(channel="test-queue", body=b"test")

            with pytest.raises(grpc.RpcError):
                client.send_queue_message_simple(message)


class TestSendQueueMessagesBatch:
    """Tests for send_queue_messages_batch() (lines 387-427)."""

    def test_send_queue_messages_batch_success(self):
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True

            mock_grpc_client = MagicMock()
            mock_batch_response = MagicMock()
            mock_batch_response.BatchID = "batch-123"
            mock_batch_response.HaveErrors = False

            mock_r1 = MagicMock()
            mock_r1.MessageID = "msg-1"
            mock_r1.SentAt = 0
            mock_r1.ExpirationAt = 0
            mock_r1.DelayedTo = 0
            mock_r1.IsError = False
            mock_r1.Error = ""
            mock_r2 = MagicMock()
            mock_r2.MessageID = "msg-2"
            mock_r2.SentAt = 0
            mock_r2.ExpirationAt = 0
            mock_r2.DelayedTo = 0
            mock_r2.IsError = False
            mock_r2.Error = ""

            mock_batch_response.Results = [mock_r1, mock_r2]
            mock_grpc_client.SendQueueMessagesBatch.return_value = mock_batch_response
            mock_transport.kubemq_client.return_value = mock_grpc_client
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")
            messages = [
                QueueMessage(channel="q", body=b"msg1"),
                QueueMessage(channel="q", body=b"msg2"),
            ]

            batch_result = client.send_queue_messages_batch(messages)

            assert batch_result.batch_id == "batch-123"
            assert batch_result.have_errors is False
            assert len(batch_result.results) == 2
            mock_grpc_client.SendQueueMessagesBatch.assert_called_once()

    def test_send_queue_messages_batch_with_errors(self):
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True

            mock_grpc_client = MagicMock()
            mock_batch_response = MagicMock()
            mock_batch_response.BatchID = "batch-err"
            mock_batch_response.HaveErrors = True

            ok = MagicMock()
            ok.MessageID = "ok"
            ok.SentAt = 0
            ok.ExpirationAt = 0
            ok.DelayedTo = 0
            ok.IsError = False
            ok.Error = ""
            err = MagicMock()
            err.MessageID = "err"
            err.SentAt = 0
            err.ExpirationAt = 0
            err.DelayedTo = 0
            err.IsError = True
            err.Error = "channel not found"

            mock_batch_response.Results = [ok, err]
            mock_grpc_client.SendQueueMessagesBatch.return_value = mock_batch_response
            mock_transport.kubemq_client.return_value = mock_grpc_client
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")
            messages = [
                QueueMessage(channel="q", body=b"m1"),
                QueueMessage(channel="bad", body=b"m2"),
            ]

            batch_result = client.send_queue_messages_batch(messages)

            assert batch_result.have_errors is True
            assert batch_result.results[1].is_error is True


class TestReceiveQueueMessagesMetadata:
    """Tests for receive_queue_messages with metadata (lines 527-529)."""

    def test_receive_with_metadata_sets_metadata_on_request(self):
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            mock_receiver = MagicMock()
            mock_response = MagicMock()
            mock_response.RefRequestId = "req-meta"
            mock_response.TransactionId = "tx-meta"
            mock_response.IsError = False
            mock_response.Error = ""
            mock_response.Messages = []
            mock_response.ActiveOffsets = []
            mock_receiver.send.return_value = mock_response
            client._downstream_receiver = mock_receiver

            result = client.receive_queue_messages(
                channel="test-queue",
                metadata={"key1": "val1", "key2": "val2"},
            )

            assert isinstance(result, QueuesPollResponse)
            call_args = mock_receiver.send.call_args[0][0]
            assert call_args.Metadata["key1"] == "val1"
            assert call_args.Metadata["key2"] == "val2"


class TestReceiveQueueMessagesExceptionPath:
    """Tests for receive exception/finally (lines 543-546)."""

    def test_receive_transport_error_propagates(self):
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            mock_receiver = MagicMock()
            mock_receiver.send.side_effect = FakeRpcError()
            client._downstream_receiver = mock_receiver

            with pytest.raises(grpc.RpcError):
                client.receive_queue_messages(channel="test-queue")

    def test_receive_invalid_max_messages_raises(self):
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            with pytest.raises(ValueError, match="max_messages must be between 1 and 1024"):
                client.receive_queue_messages(channel="q", max_messages=0)

            with pytest.raises(ValueError, match="max_messages must be between 1 and 1024"):
                client.receive_queue_messages(channel="q", max_messages=1025)

    def test_receive_invalid_timeout_raises(self):
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            with pytest.raises(
                ValueError, match="wait_timeout_in_seconds must be between 0 and 3600"
            ):
                client.receive_queue_messages(channel="q", wait_timeout_in_seconds=-1)

    def test_receive_no_client_id_raises(self):
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")
            client._config.client_id = ""

            with pytest.raises(ValueError, match="ClientID required"):
                client.receive_queue_messages(channel="q")


class TestSendQueueMessageTransportError:
    """Tests for send_queue_message general exception path (lines 290-293)."""

    def test_send_queue_message_grpc_error_propagates(self):
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            mock_sender = MagicMock()
            mock_sender.send.side_effect = FakeRpcError()
            client._upstream_sender = mock_sender

            message = QueueMessage(channel="q", body=b"test")

            with pytest.raises(grpc.RpcError):
                client.send_queue_message(message)


class TestAckAllQueueMessagesErrorPath:
    """Tests for ack_all_queue_messages error path."""

    def test_ack_all_queue_messages_error_raises(self):
        from kubemq.core.exceptions import KubeMQMessageError

        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.is_connected.return_value = True
            mock_stub = MagicMock()
            mock_resp = MagicMock()
            mock_resp.IsError = True
            mock_resp.Error = "queue locked"
            mock_stub.AckAllQueueMessages.return_value = mock_resp
            mock_transport.kubemq_client.return_value = mock_stub
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            with pytest.raises(KubeMQMessageError, match="queue locked"):
                client.ack_all_queue_messages("locked-queue")
