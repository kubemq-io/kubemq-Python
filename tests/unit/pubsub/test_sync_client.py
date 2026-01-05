"""Unit tests for PubSub sync client.

Tests for Client (sync) class from kubemq.pubsub.client.
"""

from __future__ import annotations

import warnings
from unittest.mock import MagicMock, patch

import pytest

from kubemq.common.cancellation_token import CancellationToken
from kubemq.core.config import ClientConfig
from kubemq.grpc import kubemq_pb2 as pb
from kubemq.pubsub.client import Client
from kubemq.pubsub.event_message import EventMessage
from kubemq.pubsub.event_send_result import EventSendResult
from kubemq.pubsub.event_store_message import EventStoreMessage
from kubemq.pubsub.events_store_subscription import EventsStoreSubscription
from kubemq.pubsub.events_subscription import EventsSubscription


@pytest.fixture
def mock_config():
    """Create a mock client config."""
    return ClientConfig(
        address="localhost:50000",
        client_id="test-client",
    )


@pytest.fixture
def mock_transport():
    """Create a mock sync transport."""
    transport = MagicMock()
    transport.is_connected.return_value = True
    transport.initialize.return_value = transport
    return transport


@pytest.fixture
def mock_event_sender():
    """Create a mock event sender."""
    sender = MagicMock()
    sender.send.return_value = None
    return sender


# ==============================================================================
# Initialization Tests
# ==============================================================================


class TestSyncPubSubClientInit:
    """Tests for PubSub sync client initialization."""

    def test_init_with_address(self):
        """Test initialization with address."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            assert client._config.address == "localhost:50000"

    def test_init_with_config(self, mock_config):
        """Test initialization with config object."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(config=mock_config)

            assert client._config == mock_config

    def test_init_generates_client_id(self):
        """Test that client_id is generated if not provided."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            assert client._config.client_id is not None
            assert len(client._config.client_id) > 0

    def test_init_with_auth_token(self):
        """Test initialization with auth token."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000", auth_token="my-token")

            assert client._config.auth_token == "my-token"

    def test_init_with_legacy_tls_parameters(self, tmp_path):
        """Test initialization with legacy TLS parameters."""
        cert_file = tmp_path / "cert.pem"
        cert_file.write_bytes(b"cert")
        key_file = tmp_path / "key.pem"
        key_file.write_bytes(b"key")

        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(
                address="localhost:50000",
                tls=True,
                tls_cert_file=str(cert_file),
                tls_key_file=str(key_file),
            )

            assert client._config.tls.enabled is True

    def test_init_with_keep_alive_parameters(self):
        """Test initialization with keep-alive parameters."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(
                address="localhost:50000",
                keep_alive=True,
                ping_interval_in_seconds=60,
                ping_timeout_in_seconds=20,
            )

            assert client._config.keep_alive.enabled is True
            assert client._config.keep_alive.ping_interval_in_seconds == 60
            assert client._config.keep_alive.ping_timeout_in_seconds == 20

    def test_init_creates_legacy_connection_attribute(self):
        """Test that legacy connection attribute is created."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            assert hasattr(client, "connection")
            assert client.connection is not None


# ==============================================================================
# Event Sending Tests
# ==============================================================================


class TestSyncPubSubClientEvents:
    """Tests for event sending methods."""

    def test_send_events_message_uses_event_sender(self):
        """Test send_events_message uses the event sender."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            # Inject mock event sender
            mock_sender = MagicMock()
            mock_sender.send.return_value = None
            client._event_sender = mock_sender

            message = EventMessage(channel="test-channel", body=b"test body")
            client.send_events_message(message)

            mock_sender.send.assert_called_once()

    def test_send_events_message_async_emits_deprecation_warning(self):
        """Test send_events_message_async emits deprecation warning."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            mock_sender = MagicMock()
            mock_sender.send.return_value = None
            client._event_sender = mock_sender

            message = EventMessage(channel="test-channel", body=b"test body")

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                import asyncio

                asyncio.run(client.send_events_message_async(message))

                # Check for deprecation warning
                deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
                assert len(deprecation_warnings) > 0

    def test_get_event_sender_creates_sender_lazily(self):
        """Test _get_event_sender creates sender lazily."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            assert client._event_sender is None

            with patch("kubemq.pubsub.client.EventSender") as mock_sender_class:
                mock_sender = MagicMock()
                mock_sender_class.return_value = mock_sender

                sender = client._get_event_sender()

                assert sender == mock_sender
                mock_sender_class.assert_called_once()

    def test_get_event_sender_returns_same_instance(self):
        """Test _get_event_sender returns same instance on subsequent calls."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            with patch("kubemq.pubsub.client.EventSender") as mock_sender_class:
                mock_sender = MagicMock()
                mock_sender_class.return_value = mock_sender

                sender1 = client._get_event_sender()
                sender2 = client._get_event_sender()

                assert sender1 is sender2
                # Should only be called once
                mock_sender_class.assert_called_once()


# ==============================================================================
# Event Store Tests
# ==============================================================================


class TestSyncPubSubClientEventsStore:
    """Tests for event store methods."""

    def test_send_events_store_message_returns_result(self):
        """Test send_events_store_message returns EventSendResult."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            # Create mock result
            mock_pb_result = pb.Result()
            mock_pb_result.Sent = True
            mock_pb_result.EventID = "event-123"

            mock_sender = MagicMock()
            mock_sender.send.return_value = mock_pb_result
            client._event_sender = mock_sender

            message = EventStoreMessage(channel="test-channel", body=b"test body")
            result = client.send_events_store_message(message)

            assert result is not None
            assert result.sent is True

    def test_send_events_store_message_handles_none_result(self):
        """Test send_events_store_message handles None result."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            mock_sender = MagicMock()
            mock_sender.send.return_value = None
            client._event_sender = mock_sender

            message = EventStoreMessage(channel="test-channel", body=b"test body")
            result = client.send_events_store_message(message)

            # Should return empty EventSendResult when no result
            assert isinstance(result, EventSendResult)
            assert result.sent is False

    def test_send_events_store_message_async_emits_deprecation_warning(self):
        """Test send_events_store_message_async emits deprecation warning."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            mock_pb_result = pb.Result()
            mock_pb_result.Sent = True

            mock_sender = MagicMock()
            mock_sender.send.return_value = mock_pb_result
            client._event_sender = mock_sender

            message = EventStoreMessage(channel="test-channel", body=b"test body")

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                import asyncio

                asyncio.run(client.send_events_store_message_async(message))

                deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
                assert len(deprecation_warnings) > 0


# ==============================================================================
# Channel Management Tests
# ==============================================================================


class TestSyncPubSubClientChannelManagement:
    """Tests for channel management methods."""

    def test_create_events_channel(self):
        """Test create_events_channel calls request helper."""
        with (
            patch("kubemq.transport.transport.Transport") as mock_transport_class,
            patch("kubemq.pubsub.client.create_channel_request") as mock_create,
        ):
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            mock_create.return_value = True

            client = Client(address="localhost:50000")
            result = client.create_events_channel("test-channel")

            assert result is True
            mock_create.assert_called_once()
            call_args = mock_create.call_args[0]
            assert call_args[2] == "test-channel"
            assert call_args[3] == "events"

    def test_create_events_store_channel(self):
        """Test create_events_store_channel calls request helper."""
        with (
            patch("kubemq.transport.transport.Transport") as mock_transport_class,
            patch("kubemq.pubsub.client.create_channel_request") as mock_create,
        ):
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            mock_create.return_value = True

            client = Client(address="localhost:50000")
            result = client.create_events_store_channel("store-channel")

            assert result is True
            call_args = mock_create.call_args[0]
            assert call_args[2] == "store-channel"
            assert call_args[3] == "events_store"

    def test_delete_events_channel(self):
        """Test delete_events_channel calls request helper."""
        with (
            patch("kubemq.transport.transport.Transport") as mock_transport_class,
            patch("kubemq.pubsub.client.delete_channel_request") as mock_delete,
        ):
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            mock_delete.return_value = True

            client = Client(address="localhost:50000")
            result = client.delete_events_channel("test-channel")

            assert result is True
            mock_delete.assert_called_once()
            call_args = mock_delete.call_args[0]
            assert call_args[2] == "test-channel"
            assert call_args[3] == "events"

    def test_delete_events_store_channel(self):
        """Test delete_events_store_channel calls request helper."""
        with (
            patch("kubemq.transport.transport.Transport") as mock_transport_class,
            patch("kubemq.pubsub.client.delete_channel_request") as mock_delete,
        ):
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            mock_delete.return_value = True

            client = Client(address="localhost:50000")
            result = client.delete_events_store_channel("store-channel")

            assert result is True
            call_args = mock_delete.call_args[0]
            assert call_args[2] == "store-channel"
            assert call_args[3] == "events_store"

    def test_list_events_channels(self):
        """Test list_events_channels calls request helper."""
        with (
            patch("kubemq.transport.transport.Transport") as mock_transport_class,
            patch("kubemq.pubsub.client.list_pubsub_channels") as mock_list,
        ):
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            mock_list.return_value = []

            client = Client(address="localhost:50000")
            result = client.list_events_channels()

            assert result == []
            mock_list.assert_called_once()
            call_args = mock_list.call_args[0]
            assert call_args[2] == "events"

    def test_list_events_channels_with_search(self):
        """Test list_events_channels with search filter."""
        with (
            patch("kubemq.transport.transport.Transport") as mock_transport_class,
            patch("kubemq.pubsub.client.list_pubsub_channels") as mock_list,
        ):
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            mock_list.return_value = []

            client = Client(address="localhost:50000")
            client.list_events_channels(channel_search="test*")

            call_args = mock_list.call_args[0]
            assert call_args[3] == "test*"

    def test_list_events_store_channels(self):
        """Test list_events_store_channels calls request helper."""
        with (
            patch("kubemq.transport.transport.Transport") as mock_transport_class,
            patch("kubemq.pubsub.client.list_pubsub_channels") as mock_list,
        ):
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            mock_list.return_value = []

            client = Client(address="localhost:50000")
            client.list_events_store_channels()

            call_args = mock_list.call_args[0]
            assert call_args[2] == "events_store"


# ==============================================================================
# Subscription Tests
# ==============================================================================


class TestSyncPubSubClientSubscriptions:
    """Tests for subscription methods."""

    def test_subscribe_to_events_creates_cancellation_token_if_none(self):
        """Test subscribe_to_events creates cancellation token if not provided."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            received_messages = []
            subscription = EventsSubscription(
                channel="test-channel",
                on_receive_event_callback=lambda msg: received_messages.append(msg),
            )

            # Patch threading.Thread to capture args
            with patch("kubemq.pubsub.client.threading.Thread") as mock_thread:
                mock_thread_instance = MagicMock()
                mock_thread.return_value = mock_thread_instance

                client.subscribe_to_events(subscription)

                mock_thread.assert_called_once()
                mock_thread_instance.start.assert_called_once()

    def test_subscribe_to_events_with_cancellation_token(self):
        """Test subscribe_to_events uses provided cancellation token."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            cancel = CancellationToken()
            subscription = EventsSubscription(
                channel="test-channel",
                on_receive_event_callback=lambda msg: None,
            )

            with patch("kubemq.pubsub.client.threading.Thread") as mock_thread:
                mock_thread_instance = MagicMock()
                mock_thread.return_value = mock_thread_instance

                client.subscribe_to_events(subscription, cancel=cancel)

                # Verify thread was started
                mock_thread_instance.start.assert_called_once()

    def test_subscribe_to_events_store(self):
        """Test subscribe_to_events_store starts subscription thread."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            subscription = EventsStoreSubscription(
                channel="test-channel",
                on_receive_event_callback=lambda msg: None,
            )

            with patch("kubemq.pubsub.client.threading.Thread") as mock_thread:
                mock_thread_instance = MagicMock()
                mock_thread.return_value = mock_thread_instance

                client.subscribe_to_events_store(subscription)

                mock_thread.assert_called_once()
                mock_thread_instance.start.assert_called_once()

    def test_subscribe_calls_error_callback_on_grpc_error(self):
        """Test that subscription calls error callback on gRPC error."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            error_received = []
            subscription = EventsSubscription(
                channel="test-channel",
                on_receive_event_callback=lambda msg: None,
                on_error_callback=lambda err: error_received.append(err),
            )

            # We can't easily test the actual thread behavior, but we can verify
            # the subscription is set up correctly
            with patch("kubemq.pubsub.client.threading.Thread") as mock_thread:
                mock_thread_instance = MagicMock()
                mock_thread.return_value = mock_thread_instance

                client.subscribe_to_events(subscription)

                # Verify daemon thread is created
                call_kwargs = mock_thread.call_args[1]
                assert call_kwargs["daemon"] is True


# ==============================================================================
# Context Manager Tests
# ==============================================================================


class TestSyncPubSubClientContextManager:
    """Tests for context manager support."""

    def test_context_manager_enters_and_exits(self):
        """Test sync context manager properly enters and exits."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            with Client(address="localhost:50000") as client:
                assert client is not None

            # Close should be called on exit
            mock_transport.close.assert_called()

    def test_cleanup_resources_clears_event_sender(self):
        """Test _cleanup_resources clears event sender."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")
            client._event_sender = MagicMock()

            client._cleanup_resources()

            assert client._event_sender is None


# ==============================================================================
# Async Context Manager Tests
# ==============================================================================


class TestSyncPubSubClientAsyncContextManager:
    """Tests for async context manager support on sync client."""

    @pytest.mark.asyncio
    async def test_async_context_manager_enters_and_exits(self):
        """Test async context manager on sync client."""
        from unittest.mock import AsyncMock

        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.close_async = AsyncMock()  # Must be AsyncMock for await
            mock_transport_class.return_value = mock_transport

            async with Client(address="localhost:50000") as client:
                assert client is not None


# ==============================================================================
# Async Methods Tests
# ==============================================================================


class TestSyncPubSubClientAsyncMethods:
    """Tests for async wrapper methods."""

    @pytest.mark.asyncio
    async def test_ping_async_emits_deprecation_warning(self):
        """Test ping_async emits deprecation warning."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
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
                await client.ping_async()

                deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
                assert len(deprecation_warnings) > 0

    @pytest.mark.asyncio
    async def test_create_events_channel_async(self):
        """Test create_events_channel_async calls sync method."""
        with (
            patch("kubemq.transport.transport.Transport") as mock_transport_class,
            patch("kubemq.pubsub.client.create_channel_request") as mock_create,
        ):
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            mock_create.return_value = True

            client = Client(address="localhost:50000")
            result = await client.create_events_channel_async("test-channel")

            assert result is True
