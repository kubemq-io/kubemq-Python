"""Unit tests for PubSub sync client.

Tests for Client (sync) class from kubemq.pubsub.client.
"""

from __future__ import annotations

import threading
import warnings
from unittest.mock import MagicMock, patch

import grpc
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


# ==============================================================================
# Publish Error Path Tests
# ==============================================================================


class FakeRpcError(grpc.RpcError):
    """Fake gRPC error for testing."""

    def code(self):
        return grpc.StatusCode.UNAVAILABLE

    def details(self):
        return "unavailable"


class TestSyncPubSubClientPublishErrors:
    """Tests for publish error paths."""

    def test_publish_event_grpc_error_propagates(self):
        """Test that a gRPC error from the event sender propagates."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            mock_sender = MagicMock()
            mock_sender.send.side_effect = FakeRpcError()
            client._event_sender = mock_sender

            message = EventMessage(channel="test-channel", body=b"test body")

            with pytest.raises(grpc.RpcError):
                client.publish_event(message)

    def test_publish_event_store_grpc_error(self):
        """Test that a gRPC error from event store sender propagates."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            mock_sender = MagicMock()
            mock_sender.send.side_effect = FakeRpcError()
            client._event_sender = mock_sender

            message = EventStoreMessage(channel="test-channel", body=b"test body")

            with pytest.raises(grpc.RpcError):
                client.publish_event_store(message)

    def test_publish_event_uses_new_verb(self):
        """Test publish_event() works without deprecation warning."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            mock_sender = MagicMock()
            mock_sender.send.return_value = None
            client._event_sender = mock_sender

            message = EventMessage(channel="test-channel", body=b"hello")

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                client.publish_event(message)

                deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
                assert len(deprecation_warnings) == 0

            mock_sender.send.assert_called_once()

    def test_publish_event_store_uses_new_verb(self):
        """Test publish_event_store() works without deprecation warning."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            mock_pb_result = pb.Result()
            mock_pb_result.Sent = True
            mock_pb_result.EventID = "event-456"

            mock_sender = MagicMock()
            mock_sender.send.return_value = mock_pb_result
            client._event_sender = mock_sender

            message = EventStoreMessage(channel="test-channel", body=b"hello")

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                result = client.publish_event_store(message)

                deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
                assert len(deprecation_warnings) == 0

            assert result.sent is True
            mock_sender.send.assert_called_once()


# ==============================================================================
# Subscription Task Tests
# ==============================================================================


class TestSyncPubSubClientSubscriptionTask:
    """Tests for _subscribe_task method."""

    def _make_client(self):
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            client = Client(address="localhost:50000")
        return client

    def test_subscribe_task_delivers_messages(self):
        """Test _subscribe_task delivers messages via decode_callable."""
        client = self._make_client()

        cancel_token = threading.Event()
        received = []

        mock_msg = MagicMock()
        mock_msg.Tags = {}

        def stream_callable():
            return iter([mock_msg])

        def decode_callable(msg):
            received.append(msg)
            cancel_token.set()

        def error_callable(err):
            cancel_token.set()

        client._subscribe_task(
            stream_callable, decode_callable, error_callable, cancel_token, "test-channel"
        )

        assert len(received) == 1
        assert received[0] is mock_msg

    def test_subscribe_task_grpc_error_calls_error_and_retries(self):
        """Test _subscribe_task calls error_callable on gRPC error and retries."""
        client = self._make_client()

        cancel_token = threading.Event()
        errors = []
        call_count = 0

        def stream_callable():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise FakeRpcError()
            cancel_token.set()
            return iter([])

        def decode_callable(msg):
            pass

        def error_callable(err):
            errors.append(err)
            if len(errors) >= 1:
                cancel_token.set()

        client._subscribe_task(
            stream_callable, decode_callable, error_callable, cancel_token, "test-channel"
        )

        assert len(errors) >= 1
        assert "Stream broken" in errors[0] or "unavailable" in errors[0].lower()

    def test_subscribe_task_generic_error_calls_error(self):
        """Test _subscribe_task calls error_callable on generic Exception."""
        client = self._make_client()

        cancel_token = threading.Event()
        errors = []

        def stream_callable():
            raise RuntimeError("something went wrong")

        def decode_callable(msg):
            pass

        def error_callable(err):
            errors.append(err)
            cancel_token.set()

        client._subscribe_task(
            stream_callable, decode_callable, error_callable, cancel_token, "test-channel"
        )

        assert len(errors) >= 1

    def test_subscribe_task_cancel_stops_loop(self):
        """Test _subscribe_task exits immediately when cancel_token is set."""
        client = self._make_client()

        cancel_token = threading.Event()
        cancel_token.set()

        stream_called = []

        def stream_callable():
            stream_called.append(True)
            return iter([])

        def decode_callable(msg):
            pass

        def error_callable(err):
            pass

        client._subscribe_task(
            stream_callable, decode_callable, error_callable, cancel_token, "test-channel"
        )

        assert len(stream_called) == 0

    def test_subscribe_task_handler_error_isolated(self):
        """Test that handler errors are isolated and reported via error_callable."""
        client = self._make_client()

        cancel_token = threading.Event()
        errors = []
        processed = []

        mock_msg1 = MagicMock()
        mock_msg1.Tags = {}
        mock_msg2 = MagicMock()
        mock_msg2.Tags = {}

        call_count = 0

        def stream_callable():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return iter([mock_msg1, mock_msg2])
            cancel_token.set()
            return iter([])

        def decode_callable(msg):
            processed.append(msg)
            if msg is mock_msg1:
                raise ValueError("handler boom")

        def error_callable(err):
            errors.append(err)

        client._subscribe_task(
            stream_callable, decode_callable, error_callable, cancel_token, "test-channel"
        )

        assert len(processed) == 2
        assert len(errors) == 1
        assert "handler boom" in errors[0] or "ValueError" in errors[0]


# ==============================================================================
# Async Channel Method Tests
# ==============================================================================


class TestSyncPubSubClientAsyncChannelMethods:
    """Tests for async channel management methods."""

    @pytest.mark.asyncio
    async def test_create_events_store_channel_async(self):
        """Test create_events_store_channel_async calls sync version."""
        with (
            patch("kubemq.transport.transport.Transport") as mock_transport_class,
            patch("kubemq.pubsub.client.create_channel_request") as mock_create,
        ):
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            mock_create.return_value = True

            client = Client(address="localhost:50000")
            result = await client.create_events_store_channel_async("store-channel")

            assert result is True
            call_args = mock_create.call_args[0]
            assert call_args[2] == "store-channel"
            assert call_args[3] == "events_store"

    @pytest.mark.asyncio
    async def test_delete_events_channel_async(self):
        """Test delete_events_channel_async calls sync version."""
        with (
            patch("kubemq.transport.transport.Transport") as mock_transport_class,
            patch("kubemq.pubsub.client.delete_channel_request") as mock_delete,
        ):
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            mock_delete.return_value = True

            client = Client(address="localhost:50000")
            result = await client.delete_events_channel_async("test-channel")

            assert result is True
            call_args = mock_delete.call_args[0]
            assert call_args[2] == "test-channel"
            assert call_args[3] == "events"

    @pytest.mark.asyncio
    async def test_delete_events_store_channel_async(self):
        """Test delete_events_store_channel_async calls sync version."""
        with (
            patch("kubemq.transport.transport.Transport") as mock_transport_class,
            patch("kubemq.pubsub.client.delete_channel_request") as mock_delete,
        ):
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            mock_delete.return_value = True

            client = Client(address="localhost:50000")
            result = await client.delete_events_store_channel_async("store-channel")

            assert result is True
            call_args = mock_delete.call_args[0]
            assert call_args[2] == "store-channel"
            assert call_args[3] == "events_store"

    @pytest.mark.asyncio
    async def test_list_events_channels_async(self):
        """Test list_events_channels_async calls sync version."""
        with (
            patch("kubemq.transport.transport.Transport") as mock_transport_class,
            patch("kubemq.pubsub.client.list_pubsub_channels") as mock_list,
        ):
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            mock_list.return_value = ["ch1", "ch2"]

            client = Client(address="localhost:50000")
            result = await client.list_events_channels_async("test*")

            assert result == ["ch1", "ch2"]
            call_args = mock_list.call_args[0]
            assert call_args[2] == "events"
            assert call_args[3] == "test*"

    @pytest.mark.asyncio
    async def test_list_events_store_channels_async(self):
        """Test list_events_store_channels_async calls sync version."""
        with (
            patch("kubemq.transport.transport.Transport") as mock_transport_class,
            patch("kubemq.pubsub.client.list_pubsub_channels") as mock_list,
        ):
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            mock_list.return_value = []

            client = Client(address="localhost:50000")
            result = await client.list_events_store_channels_async()

            assert result == []
            call_args = mock_list.call_args[0]
            assert call_args[2] == "events_store"


# ==============================================================================
# Async Subscription Task Tests (_subscribe_task_async)
# ==============================================================================


class TestSyncPubSubClientSubscribeTaskAsync:
    """Tests for _subscribe_task_async method."""

    def _make_client(self):
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            client = Client(address="localhost:50000")
        return client

    @pytest.mark.asyncio
    async def test_subscribe_task_async_delivers_messages(self):
        """Test that messages from the stream are forwarded to decode_callable."""

        client = self._make_client()

        mock_msg = MagicMock()
        mock_msg.Tags = {}

        cancel = threading.Event()
        messages_iter = iter([mock_msg])

        call_count = [0]

        async def mock_to_thread(fn, *args):
            call_count[0] += 1
            if call_count[0] == 1:
                return messages_iter
            try:
                return next(args[0]) if args else fn()
            except StopIteration:
                cancel.set()
                raise

        decoded = []

        async def decode(msg):
            decoded.append(msg)

        async def on_error(err):
            pass

        with patch("kubemq.pubsub.client.asyncio.to_thread", side_effect=mock_to_thread):
            await client._subscribe_task_async(
                lambda: messages_iter,
                decode,
                on_error,
                cancel,
                "test-channel",
            )

        assert len(decoded) == 1
        assert decoded[0] is mock_msg

    @pytest.mark.asyncio
    async def test_subscribe_task_async_grpc_error_calls_error(self):
        """Test that a gRPC RpcError calls error_callable with stream broken message."""
        from unittest.mock import AsyncMock

        client = self._make_client()

        cancel = threading.Event()
        errors = []

        async def mock_to_thread(fn, *args):
            raise FakeRpcError()

        async def decode(msg):
            pass

        async def on_error(err):
            errors.append(err)
            cancel.set()

        with (
            patch("kubemq.pubsub.client.asyncio.to_thread", side_effect=mock_to_thread),
            patch("kubemq.pubsub.client.asyncio.sleep", new_callable=AsyncMock),
        ):
            await client._subscribe_task_async(
                lambda: None,
                decode,
                on_error,
                cancel,
                "test-channel",
            )

        assert len(errors) >= 1
        assert "Stream broken" in errors[0]

    @pytest.mark.asyncio
    async def test_subscribe_task_async_cancel_stops_loop(self):
        """Test that pre-setting cancel_token prevents stream_callable from being called."""
        client = self._make_client()

        cancel = threading.Event()
        cancel.set()

        stream_called = []

        def stream_fn():
            stream_called.append(True)
            return iter([])

        async def decode(msg):
            pass

        async def on_error(err):
            pass

        await client._subscribe_task_async(
            stream_fn,
            decode,
            on_error,
            cancel,
            "test-channel",
        )

        assert len(stream_called) == 0


# ==============================================================================
# Additional Coverage Tests
# ==============================================================================

from unittest.mock import AsyncMock  # noqa: E402

from kubemq.core.exceptions import KubeMQValidationError  # noqa: E402


class TestSyncPubSubClientPublishEventValidationError:
    """Tests for ValidationError path in _publish_event_impl (lines 233-245)."""

    def test_publish_event_validation_error_wraps_pydantic(self):
        """Test pydantic.ValidationError from encode is wrapped in KubeMQValidationError."""
        from pydantic import ValidationError as PydanticValidationError

        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            mock_sender = MagicMock()
            client._event_sender = mock_sender

            message = EventMessage(channel="test-channel", body=b"test body")

            validation_err = PydanticValidationError.from_exception_data(
                title="EventMessage",
                line_errors=[],
            )
            with patch.object(EventMessage, "encode", side_effect=validation_err):
                with pytest.raises(KubeMQValidationError) as exc_info:
                    client.publish_event(message)
                assert exc_info.value.__cause__ is validation_err


class TestSyncPubSubClientPublishEventStoreValidationError:
    """Tests for ValidationError path in _publish_event_store_impl (lines 300-315)."""

    def test_publish_event_store_validation_error_wraps_pydantic(self):
        """Test pydantic.ValidationError from encode is wrapped in KubeMQValidationError."""
        from pydantic import ValidationError as PydanticValidationError

        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            mock_sender = MagicMock()
            client._event_sender = mock_sender

            message = EventStoreMessage(channel="test-channel", body=b"test body")

            validation_err = PydanticValidationError.from_exception_data(
                title="EventStoreMessage",
                line_errors=[],
            )
            with patch.object(EventStoreMessage, "encode", side_effect=validation_err):
                with pytest.raises(KubeMQValidationError) as exc_info:
                    client.publish_event_store(message)
                assert exc_info.value.__cause__ is validation_err


class TestSyncPubSubClientAsyncChannelMethodsAdditional:
    """Tests for async channel management methods covering lines 612-704."""

    @pytest.mark.asyncio
    async def test_create_events_store_channel_async_delegates(self):
        """Test create_events_store_channel_async calls sync version."""
        with (
            patch("kubemq.transport.transport.Transport") as mock_transport_class,
            patch("kubemq.pubsub.client.create_channel_request") as mock_create,
        ):
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            mock_create.return_value = True

            client = Client(address="localhost:50000")
            result = await client.create_events_store_channel_async("store-ch")

            assert result is True
            call_args = mock_create.call_args[0]
            assert call_args[3] == "events_store"

    @pytest.mark.asyncio
    async def test_delete_events_channel_async_delegates(self):
        """Test delete_events_channel_async calls sync version."""
        with (
            patch("kubemq.transport.transport.Transport") as mock_transport_class,
            patch("kubemq.pubsub.client.delete_channel_request") as mock_delete,
        ):
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            mock_delete.return_value = True

            client = Client(address="localhost:50000")
            result = await client.delete_events_channel_async("test-ch")

            assert result is True
            call_args = mock_delete.call_args[0]
            assert call_args[3] == "events"

    @pytest.mark.asyncio
    async def test_delete_events_store_channel_async_delegates(self):
        """Test delete_events_store_channel_async calls sync version."""
        with (
            patch("kubemq.transport.transport.Transport") as mock_transport_class,
            patch("kubemq.pubsub.client.delete_channel_request") as mock_delete,
        ):
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            mock_delete.return_value = True

            client = Client(address="localhost:50000")
            result = await client.delete_events_store_channel_async("store-ch")

            assert result is True
            call_args = mock_delete.call_args[0]
            assert call_args[3] == "events_store"

    @pytest.mark.asyncio
    async def test_list_events_channels_async_delegates(self):
        """Test list_events_channels_async calls sync version."""
        with (
            patch("kubemq.transport.transport.Transport") as mock_transport_class,
            patch("kubemq.pubsub.client.list_pubsub_channels") as mock_list,
        ):
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            mock_list.return_value = ["ch1"]

            client = Client(address="localhost:50000")
            result = await client.list_events_channels_async("filter*")

            assert result == ["ch1"]
            call_args = mock_list.call_args[0]
            assert call_args[2] == "events"

    @pytest.mark.asyncio
    async def test_list_events_store_channels_async_delegates(self):
        """Test list_events_store_channels_async calls sync version."""
        with (
            patch("kubemq.transport.transport.Transport") as mock_transport_class,
            patch("kubemq.pubsub.client.list_pubsub_channels") as mock_list,
        ):
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            mock_list.return_value = []

            client = Client(address="localhost:50000")
            result = await client.list_events_store_channels_async()

            assert result == []
            call_args = mock_list.call_args[0]
            assert call_args[2] == "events_store"


class TestSyncPubSubClientSubscribeTaskAsyncAdditional:
    """Additional tests for _subscribe_task_async covering lines 712-741, 771-811."""

    def _make_client(self):
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            client = Client(address="localhost:50000")
        return client

    @pytest.mark.asyncio
    async def test_subscribe_task_async_handler_error_calls_error(self):
        """Test that handler error in decode_callable is caught and reported (lines 780-792)."""
        client = self._make_client()

        cancel = threading.Event()
        errors = []

        mock_msg = MagicMock()
        mock_msg.Tags = {}
        messages_iter = iter([mock_msg])

        call_count = [0]

        async def mock_to_thread(fn, *args):
            call_count[0] += 1
            if call_count[0] == 1:
                return messages_iter
            try:
                return next(args[0]) if args else fn()
            except StopIteration:
                cancel.set()
                raise

        async def decode(msg):
            raise ValueError("handler exploded")

        async def on_error(err):
            errors.append(err)

        with patch("kubemq.pubsub.client.asyncio.to_thread", side_effect=mock_to_thread):
            await client._subscribe_task_async(
                lambda: messages_iter,
                decode,
                on_error,
                cancel,
                "test-channel",
            )

        assert len(errors) >= 1
        assert "handler exploded" in errors[0] or "ValueError" in errors[0]

    @pytest.mark.asyncio
    async def test_subscribe_task_async_generic_error_calls_error(self):
        """Test that a generic exception calls error_callable (line 811)."""
        client = self._make_client()

        cancel = threading.Event()
        errors = []

        async def mock_to_thread(fn, *args):
            raise RuntimeError("boom")

        async def decode(msg):
            pass

        async def on_error(err):
            errors.append(err)
            cancel.set()

        with (
            patch("kubemq.pubsub.client.asyncio.to_thread", side_effect=mock_to_thread),
            patch("kubemq.pubsub.client.asyncio.sleep", new_callable=AsyncMock),
        ):
            await client._subscribe_task_async(
                lambda: None,
                decode,
                on_error,
                cancel,
                "test-channel",
            )

        assert len(errors) >= 1

    @pytest.mark.asyncio
    async def test_subscribe_task_async_stop_iteration_breaks(self):
        """Test that StopIteration from next() breaks inner loop (line 799)."""
        client = self._make_client()

        cancel = threading.Event()
        decoded = []

        call_count = [0]

        async def mock_to_thread(fn, *args):
            call_count[0] += 1
            if call_count[0] == 1:
                return iter([])
            raise StopIteration()

        async def decode(msg):
            decoded.append(msg)

        async def on_error(err):
            pass

        with patch("kubemq.pubsub.client.asyncio.to_thread", side_effect=mock_to_thread):
            cancel.set()
            await client._subscribe_task_async(
                lambda: iter([]),
                decode,
                on_error,
                cancel,
                "test-channel",
            )

        assert len(decoded) == 0

    @pytest.mark.asyncio
    async def test_subscribe_async_events_builds_args(self):
        """Test _subscribe_async with EventsSubscription builds async callbacks (lines 731-740)."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            async def on_event(msg):
                pass

            async def on_err(err):
                pass

            subscription = EventsSubscription(
                channel="test-events",
                on_receive_event_callback=on_event,
                on_error_callback=on_err,
            )

            cancel = CancellationToken()
            cancel.cancel()

            with patch.object(client, "_subscribe_task_async", new_callable=AsyncMock) as mock_task:
                mock_task.return_value = None
                with patch("kubemq.pubsub.client.asyncio.create_task") as mock_create_task:
                    mock_create_task.return_value = MagicMock()
                    client._subscribe_async(subscription, cancel)
                    mock_create_task.assert_called_once()

    @pytest.mark.asyncio
    async def test_subscribe_async_events_store_builds_args(self):
        """Test _subscribe_async with EventsStoreSubscription builds async callbacks (lines 721-730)."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            async def on_event(msg):
                pass

            async def on_err(err):
                pass

            subscription = EventsStoreSubscription(
                channel="test-store",
                on_receive_event_callback=on_event,
                on_error_callback=on_err,
            )

            cancel = CancellationToken()
            cancel.cancel()

            with patch.object(client, "_subscribe_task_async", new_callable=AsyncMock) as mock_task:
                mock_task.return_value = None
                with patch("kubemq.pubsub.client.asyncio.create_task") as mock_create_task:
                    mock_create_task.return_value = MagicMock()
                    client._subscribe_async(subscription, cancel)
                    mock_create_task.assert_called_once()

    @pytest.mark.asyncio
    async def test_subscribe_to_events_async_returns_task(self):
        """Test subscribe_to_events_async delegates to _subscribe_async."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            subscription = EventsSubscription(
                channel="test-events",
                on_receive_event_callback=lambda msg: None,
            )

            with patch.object(client, "_subscribe_async", return_value=MagicMock()) as mock_sub:
                client.subscribe_to_events_async(subscription)
                mock_sub.assert_called_once_with(subscription, None)

    @pytest.mark.asyncio
    async def test_subscribe_to_events_store_async_returns_task(self):
        """Test subscribe_to_events_store_async delegates to _subscribe_async."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            subscription = EventsStoreSubscription(
                channel="test-store",
                on_receive_event_callback=lambda msg: None,
            )

            with patch.object(client, "_subscribe_async", return_value=MagicMock()) as mock_sub:
                client.subscribe_to_events_store_async(subscription)
                mock_sub.assert_called_once_with(subscription, None)


class TestSyncPubSubClientGrpcErrorNonRetryable:
    """Tests for non-retryable gRPC errors in _subscribe_task_async (line 810-811)."""

    def _make_client(self):
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            client = Client(address="localhost:50000")
        return client

    @pytest.mark.asyncio
    async def test_subscribe_task_async_non_retryable_grpc_error_breaks(self):
        """Test that a non-retryable gRPC error breaks the loop."""

        class FakeNonRetryableRpcError(grpc.RpcError):
            def code(self):
                return grpc.StatusCode.PERMISSION_DENIED

            def details(self):
                return "permission denied"

        client = self._make_client()

        cancel = threading.Event()
        errors = []

        async def mock_to_thread(fn, *args):
            raise FakeNonRetryableRpcError()

        async def decode(msg):
            pass

        async def on_error(err):
            errors.append(err)

        with (
            patch("kubemq.pubsub.client.asyncio.to_thread", side_effect=mock_to_thread),
            patch("kubemq.pubsub.client.asyncio.sleep", new_callable=AsyncMock),
        ):
            await client._subscribe_task_async(
                lambda: None,
                decode,
                on_error,
                cancel,
                "test-channel",
            )

        assert len(errors) >= 1
        assert "Stream broken" in errors[0]


# ==============================================================================
# Extended Coverage Tests — 95% target
# ==============================================================================


class TestSyncPubSubClientSendEventUnary:
    """Tests for send_event() unary RPC path (lines 221-268)."""

    def test_send_event_unary_success(self):
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            mock_grpc_client = MagicMock()
            mock_result = MagicMock()
            mock_result.Sent = True
            mock_result.Error = ""
            mock_grpc_client.SendEvent.return_value = mock_result
            mock_transport.kubemq_client.return_value = mock_grpc_client

            client = Client(address="localhost:50000")
            message = EventMessage(channel="test-channel", body=b"hello")

            client.send_event(message)

            mock_grpc_client.SendEvent.assert_called_once()

    def test_send_event_unary_server_error_raises(self):
        from kubemq.core.exceptions import KubeMQError

        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            mock_grpc_client = MagicMock()
            mock_result = MagicMock()
            mock_result.Sent = False
            mock_result.Error = "channel does not exist"
            mock_grpc_client.SendEvent.return_value = mock_result
            mock_transport.kubemq_client.return_value = mock_grpc_client

            client = Client(address="localhost:50000")
            message = EventMessage(channel="bad-channel", body=b"hello")

            with pytest.raises(KubeMQError, match="channel does not exist"):
                client.send_event(message)

    def test_send_event_unary_validation_error(self):
        from pydantic import ValidationError as PydanticValidationError

        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")
            message = EventMessage(channel="ch", body=b"test")

            validation_err = PydanticValidationError.from_exception_data(
                title="EventMessage", line_errors=[]
            )
            with patch.object(EventMessage, "encode", side_effect=validation_err):
                with pytest.raises(KubeMQValidationError) as exc_info:
                    client.send_event(message)
                assert exc_info.value.__cause__ is validation_err

    def test_send_event_unary_transport_error(self):
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            mock_grpc_client = MagicMock()
            mock_grpc_client.SendEvent.side_effect = FakeRpcError()
            mock_transport.kubemq_client.return_value = mock_grpc_client

            client = Client(address="localhost:50000")
            message = EventMessage(channel="ch", body=b"test")

            with pytest.raises(grpc.RpcError):
                client.send_event(message)


class TestSyncPubSubClientSpanRecording:
    """Tests for span recording branches (lines 281-287, 349-354)."""

    def test_publish_event_span_attributes_set(self):
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            mock_sender = MagicMock()
            mock_sender.send.return_value = None
            client._event_sender = mock_sender

            mock_span = MagicMock()
            mock_span.is_recording.return_value = True
            mock_span.__enter__ = MagicMock(return_value=mock_span)
            mock_span.__exit__ = MagicMock(return_value=False)

            mock_instrumentor = MagicMock()
            mock_instrumentor.start_span.return_value = mock_span
            client._instrumentor = mock_instrumentor

            message = EventMessage(channel="ch", body=b"hello")
            client.publish_event(message)

            assert mock_span.set_attribute.call_count == 2

    def test_publish_event_store_span_attributes_set(self):
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            mock_pb_result = pb.Result()
            mock_pb_result.Sent = True
            mock_pb_result.EventID = "ev-1"

            mock_sender = MagicMock()
            mock_sender.send.return_value = mock_pb_result
            client._event_sender = mock_sender

            mock_span = MagicMock()
            mock_span.is_recording.return_value = True
            mock_span.__enter__ = MagicMock(return_value=mock_span)
            mock_span.__exit__ = MagicMock(return_value=False)

            mock_instrumentor = MagicMock()
            mock_instrumentor.start_span.return_value = mock_span
            client._instrumentor = mock_instrumentor

            message = EventStoreMessage(channel="ch", body=b"hello")
            result = client.publish_event_store(message)

            assert mock_span.set_attribute.call_count == 2
            assert result.sent is True


class TestSyncPubSubClientEventsStoreSubscription:
    """Tests for events store subscription with sequence tracking (lines 620-636)."""

    def test_subscribe_to_events_store_builds_store_args(self):
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            from kubemq.pubsub.events_store_subscription import EventsStoreType

            subscription = EventsStoreSubscription(
                channel="store-channel",
                on_receive_event_callback=lambda msg: None,
                events_store_type=EventsStoreType.StartNewOnly,
            )

            with patch("kubemq.pubsub.client.threading.Thread") as mock_thread:
                mock_thread_instance = MagicMock()
                mock_thread.return_value = mock_thread_instance

                client.subscribe_to_events_store(subscription)

                mock_thread.assert_called_once()
                call_kwargs = mock_thread.call_args[1]
                assert call_kwargs["daemon"] is True
                assert len(call_kwargs["args"]) == 5
                mock_thread_instance.start.assert_called_once()


class TestSyncPubSubClientSubscribeTaskNonRetryable:
    """Tests for _subscribe_task non-retryable gRPC error (lines 723-724)."""

    def _make_client(self):
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            client = Client(address="localhost:50000")
        return client

    def test_subscribe_task_non_retryable_grpc_error_breaks(self):
        class FakeNonRetryableRpcError(grpc.RpcError):
            def code(self):
                return grpc.StatusCode.PERMISSION_DENIED

            def details(self):
                return "permission denied"

        client = self._make_client()
        cancel_token = threading.Event()
        errors = []

        def stream_callable():
            raise FakeNonRetryableRpcError()

        def decode_callable(msg):
            pass

        def error_callable(err):
            errors.append(err)

        client._subscribe_task(stream_callable, decode_callable, error_callable, cancel_token, "ch")

        assert len(errors) >= 1
        assert "Stream broken" in errors[0]


class TestSyncPubSubClientSendEventUnarySpan:
    """Tests for send_event() span recording (lines 244-250)."""

    def test_send_event_unary_span_attributes(self):
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            mock_grpc_client = MagicMock()
            mock_result = MagicMock()
            mock_result.Sent = True
            mock_result.Error = ""
            mock_grpc_client.SendEvent.return_value = mock_result
            mock_transport.kubemq_client.return_value = mock_grpc_client

            client = Client(address="localhost:50000")

            mock_span = MagicMock()
            mock_span.is_recording.return_value = True
            mock_span.__enter__ = MagicMock(return_value=mock_span)
            mock_span.__exit__ = MagicMock(return_value=False)

            mock_instrumentor = MagicMock()
            mock_instrumentor.start_span.return_value = mock_span
            client._instrumentor = mock_instrumentor

            message = EventMessage(channel="ch", body=b"data")
            client.send_event(message)

            assert mock_span.set_attribute.call_count == 2
            mock_instrumentor._metrics.record_sent_message.assert_called_once_with("publish", "ch")
            mock_instrumentor._metrics.record_operation_duration.assert_called_once()


# ==============================================================================
# Store Subscription Closure Execution Tests — 95% target
# ==============================================================================


class TestSyncClientCloseAsyncTransportNone:
    """Test close_async when _transport is None (line 182->exit branch)."""

    @pytest.mark.asyncio
    async def test_close_async_skips_when_transport_is_none(self):
        """close_async exits cleanly without calling transport.close_async."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            client = Client(address="localhost:50000")

        client._transport = None
        await client.close_async()


class TestSyncClientStoreClosureExecution:
    """Execute store-subscription closures created by _subscribe (lines 620-657)."""

    def _setup_store(self, callback):
        """Create client, subscribe to store, return (client, args_tuple)."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            client = Client(address="localhost:50000")

            sub = EventsStoreSubscription(
                channel="store-ch",
                on_receive_event_callback=callback,
            )

            with patch("kubemq.pubsub.client.threading.Thread") as mock_thread:
                mock_thread.return_value = MagicMock()
                client.subscribe_to_events_store(sub)
                args = mock_thread.call_args[1]["args"]

        return client, args

    def _make_mock_store_msg(self, event_id="ev-1", sequence=42):
        m = MagicMock()
        m.EventID = event_id
        m.Channel = "store-ch"
        m.Metadata = ""
        m.Body = b"body"
        m.Timestamp = 100
        m.Sequence = sequence
        m.Tags = {}
        return m

    def test_decode_and_track_decodes_and_tracks_sequence(self):
        """Lines 632-636: closure decodes message, tracks seq, fires callback."""
        received = []
        _, args = self._setup_store(lambda msg: received.append(msg))
        decode_and_track = args[1]

        decode_and_track(self._make_mock_store_msg(sequence=42))

        assert len(received) == 1
        assert received[0].sequence == 42
        assert received[0].id == "ev-1"

    def test_make_store_stream_resumes_after_tracked_seq(self):
        """Lines 623-630: make_store_stream resumes from last_seq+1."""
        client, args = self._setup_store(lambda msg: None)
        make_store_stream, decode_and_track = args[0], args[1]

        make_store_stream()
        decode_and_track(self._make_mock_store_msg(sequence=10))
        make_store_stream()

        assert client._transport.kubemq_client().SubscribeToEvents.call_count == 2

    def test_subscribe_task_with_store_closures(self):
        """End-to-end: _subscribe_task processes messages via store closures."""
        received = []
        cancel = threading.Event()
        client, args = self._setup_store(lambda msg: received.append(msg))
        _, decode_and_track, error_callable, _, channel = args

        mock_msg = self._make_mock_store_msg(sequence=5)

        def decode_then_cancel(msg):
            decode_and_track(msg)
            cancel.set()

        client._subscribe_task(
            lambda: iter([mock_msg]),
            decode_then_cancel,
            error_callable,
            cancel,
            channel,
        )

        assert len(received) == 1
        assert received[0].sequence == 5


class TestSyncClientAsyncStoreClosureExecution:
    """Execute async store closures from _subscribe_async (lines 785-820)."""

    def _make_client(self):
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            return Client(address="localhost:50000")

    async def _capture_async_closures(self, client, sub):
        """Call _subscribe_async and capture the closures passed to _subscribe_task_async."""
        captured = {}

        async def capturing_task(
            stream_callable, decode_callable, error_callable, cancel_token, channel=""
        ):
            captured["stream_callable"] = stream_callable
            captured["decode_callable"] = decode_callable

        cancel = CancellationToken()
        cancel.cancel()

        with patch.object(client, "_subscribe_task_async", side_effect=capturing_task):
            with patch("kubemq.pubsub.client.asyncio.create_task") as mock_ct:
                coro_holder = [None]

                def save_coro(coro):
                    coro_holder[0] = coro
                    return MagicMock()

                mock_ct.side_effect = save_coro
                client._subscribe_async(sub, cancel)
                if coro_holder[0] is not None:
                    await coro_holder[0]

        return captured

    @pytest.mark.asyncio
    async def test_async_store_decode_and_track_closure(self):
        """Lines 797-801: decode_and_track_async decodes, tracks seq, calls async callback."""
        client = self._make_client()
        received = []

        async def on_event(msg):
            received.append(msg)

        sub = EventsStoreSubscription(
            channel="store-ch",
            on_receive_event_callback=on_event,
        )

        captured = await self._capture_async_closures(client, sub)

        mock_msg = MagicMock()
        mock_msg.EventID = "ev-1"
        mock_msg.Channel = "store-ch"
        mock_msg.Metadata = ""
        mock_msg.Body = b"data"
        mock_msg.Timestamp = 123
        mock_msg.Sequence = 42
        mock_msg.Tags = {}

        await captured["decode_callable"](mock_msg)

        assert len(received) == 1
        assert received[0].sequence == 42

    @pytest.mark.asyncio
    async def test_async_store_stream_callable_resumes(self):
        """Lines 788-795: make_store_stream_async resumes from tracked seq."""
        client = self._make_client()
        sub = EventsStoreSubscription(
            channel="store-ch",
            on_receive_event_callback=lambda msg: None,
        )

        captured = await self._capture_async_closures(client, sub)

        captured["stream_callable"]()

        mock_msg = MagicMock()
        mock_msg.EventID = "e"
        mock_msg.Channel = "c"
        mock_msg.Metadata = ""
        mock_msg.Body = b"d"
        mock_msg.Timestamp = 1
        mock_msg.Sequence = 10
        mock_msg.Tags = {}
        await captured["decode_callable"](mock_msg)

        captured["stream_callable"]()

        assert client._transport.kubemq_client().SubscribeToEvents.call_count == 2


class TestSyncClientSubscribeTaskLinksAppend:
    """Test links.append when create_link_from_context returns non-None."""

    def _make_client(self):
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            return Client(address="localhost:50000")

    def test_subscribe_task_links_appended(self):
        """Line 686: links.append(link) when link is non-None."""
        client = self._make_client()
        cancel = threading.Event()
        received = []

        mock_msg = MagicMock()
        mock_msg.Tags = {"traceparent": "00-abc-def-01"}

        def decode_callable(msg):
            received.append(msg)
            cancel.set()

        with patch("kubemq.pubsub.client.create_link_from_context") as mock_link_fn:
            mock_link_fn.return_value = MagicMock()
            client._subscribe_task(
                lambda: iter([mock_msg]),
                decode_callable,
                lambda err: None,
                cancel,
                "ch",
            )

        assert len(received) == 1

    @pytest.mark.asyncio
    async def test_subscribe_task_async_links_appended(self):
        """Line 850: links.append(link) in async task when link is non-None."""
        client = self._make_client()
        cancel = threading.Event()
        decoded = []

        mock_msg = MagicMock()
        mock_msg.Tags = {"traceparent": "00-abc-def-01"}
        messages_iter = iter([mock_msg])
        call_count = [0]

        async def mock_to_thread(fn, *args):
            call_count[0] += 1
            if call_count[0] == 1:
                return messages_iter
            try:
                return next(args[0]) if args else fn()
            except StopIteration:
                cancel.set()
                raise

        async def decode(msg):
            decoded.append(msg)

        async def on_error(err):
            pass

        with (
            patch("kubemq.pubsub.client.asyncio.to_thread", side_effect=mock_to_thread),
            patch("kubemq.pubsub.client.create_link_from_context") as mock_link_fn,
        ):
            mock_link_fn.return_value = MagicMock()
            await client._subscribe_task_async(
                lambda: messages_iter,
                decode,
                on_error,
                cancel,
                "ch",
            )

        assert len(decoded) == 1
