"""Tests for AsyncPubSubClient."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kubemq.common.async_cancellation_token import AsyncCancellationToken
from kubemq.core.config import ClientConfig
from kubemq.core.exceptions import KubeMQConnectionError
from kubemq.grpc import kubemq_pb2 as pb
from kubemq.pubsub.async_client import AsyncClient, AsyncPubSubClient
from kubemq.pubsub.event_message import EventMessage
from kubemq.pubsub.event_store_message import EventStoreMessage
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
    """Create a mock async transport."""
    transport = AsyncMock()
    transport.is_connected = True
    return transport


class TestAsyncClientAlias:
    """Test that the alias works correctly."""

    def test_alias_is_same_class(self):
        """Test that AsyncPubSubClient is an alias for AsyncClient."""
        assert AsyncPubSubClient is AsyncClient


class TestAsyncClientInitialization:
    """Tests for AsyncClient initialization."""

    def test_init_with_address(self):
        """Test initialization with address."""
        client = AsyncClient(address="localhost:50000")
        assert client._config.address == "localhost:50000"

    def test_init_with_config(self, mock_config):
        """Test initialization with config object."""
        client = AsyncClient(config=mock_config)
        assert client._config == mock_config

    def test_init_with_client_id(self):
        """Test initialization with client ID."""
        client = AsyncClient(address="localhost:50000", client_id="my-client")
        assert client._config.client_id == "my-client"

    def test_init_with_auth_token(self):
        """Test initialization with auth token."""
        client = AsyncClient(
            address="localhost:50000",
            auth_token="test-token",
        )
        assert client._config.auth_token == "test-token"


class TestAsyncClientSendEvent:
    """Tests for send_event method."""

    @pytest.mark.asyncio
    async def test_send_event_when_not_connected(self):
        """Test send_event raises when not connected."""
        client = AsyncClient(address="localhost:50000")
        message = EventMessage(channel="test", body=b"data")

        with pytest.raises(KubeMQConnectionError):
            await client.send_event(message)

    @pytest.mark.asyncio
    async def test_send_event_encodes_message(self, mock_transport):
        """Test send_event properly encodes the message."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        message = EventMessage(
            channel="test-channel",
            body=b"test body",
            metadata="test metadata",
        )

        await client.send_event(message)

        # Verify transport was called
        mock_transport.send_event.assert_called_once()

        # Get the pb.Event that was passed
        call_args = mock_transport.send_event.call_args
        pb_event = call_args[0][0]
        assert isinstance(pb_event, pb.Event)


class TestAsyncClientSendEventStore:
    """Tests for send_event_store method."""

    @pytest.mark.asyncio
    async def test_send_event_store_when_not_connected(self):
        """Test send_event_store raises when not connected."""
        client = AsyncClient(address="localhost:50000")
        message = EventStoreMessage(channel="test", body=b"data")

        with pytest.raises(KubeMQConnectionError):
            await client.send_event_store(message)

    @pytest.mark.asyncio
    async def test_send_event_store_returns_result(self, mock_transport):
        """Test send_event_store returns EventSendResult."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        # Setup mock response
        mock_result = pb.Result()
        mock_result.Sent = True
        mock_result.EventID = "event-123"
        mock_transport.send_event.return_value = mock_result

        message = EventStoreMessage(
            channel="test-channel",
            body=b"test body",
        )

        result = await client.send_event_store(message)

        assert result.sent is True


class TestAsyncClientSendBatch:
    """Tests for batch send methods."""

    @pytest.mark.asyncio
    async def test_send_events_batch_when_not_connected(self):
        """Test send_events_batch raises when not connected."""
        client = AsyncClient(address="localhost:50000")
        messages = [EventMessage(channel="test", body=b"data")]

        with pytest.raises(KubeMQConnectionError):
            await client.send_events_batch(messages)

    @pytest.mark.asyncio
    async def test_send_events_batch_respects_concurrency(self, mock_transport):
        """Test send_events_batch respects max_concurrent."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        messages = [EventMessage(channel="test", body=f"msg-{i}".encode()) for i in range(10)]

        results = await client.send_events_batch(messages, max_concurrent=5)

        assert len(results) == 10
        # All should be successful since transport mock doesn't raise
        assert all(r.sent for r in results)


class TestAsyncClientSubscription:
    """Tests for subscription methods."""

    @pytest.mark.asyncio
    async def test_subscribe_to_events_when_not_connected(self):
        """Test subscribe_to_events raises when not connected."""
        client = AsyncClient(address="localhost:50000")
        subscription = EventsSubscription(
            channel="test",
            on_receive_event_callback=lambda e: None,
        )

        with pytest.raises(KubeMQConnectionError):
            async for _ in client.subscribe_to_events(subscription):
                pass

    @pytest.mark.asyncio
    async def test_subscribe_to_events_creates_token_if_not_provided(self, mock_transport):
        """Test subscribe_to_events creates cancellation token if not provided."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        # Make the transport return an async iterator directly (not a coroutine)
        mock_transport.subscribe_to_events = MagicMock(return_value=AsyncIteratorMock([]))

        subscription = EventsSubscription(
            channel="test",
            on_receive_event_callback=lambda e: None,
        )

        # Should not raise
        async for _ in client.subscribe_to_events(subscription):
            pass


class TestAsyncClientContextManager:
    """Tests for context manager support."""

    @pytest.mark.asyncio
    async def test_context_manager_connects_and_disconnects(self):
        """Test that context manager properly connects and disconnects."""
        with (
            patch.object(AsyncClient, "connect", new_callable=AsyncMock) as mock_connect,
            patch.object(AsyncClient, "close", new_callable=AsyncMock) as mock_close,
        ):
            async with AsyncClient(address="localhost:50000"):
                mock_connect.assert_called_once()

            mock_close.assert_called_once()


class TestAsyncClientSubscriptionManagement:
    """Tests for subscription registration."""

    @pytest.mark.asyncio
    async def test_register_subscription(self, mock_transport):
        """Test that subscriptions are registered."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        token = AsyncCancellationToken()
        await client._register_subscription(token)

        assert token in client._active_subscriptions

    @pytest.mark.asyncio
    async def test_unregister_subscription(self, mock_transport):
        """Test that subscriptions are unregistered."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        token = AsyncCancellationToken()
        await client._register_subscription(token)
        await client._unregister_subscription(token)

        assert token not in client._active_subscriptions


class AsyncIteratorMock:
    """Mock for async iterators."""

    def __init__(self, items):
        self.items = iter(items)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self.items)
        except StopIteration:
            raise StopAsyncIteration


# ==============================================================================
# Extended Tests for Additional Coverage
# ==============================================================================


class TestAsyncClientSendEventsStoreBatch:
    """Tests for send_events_store_batch method."""

    @pytest.mark.asyncio
    async def test_send_events_store_batch_when_not_connected(self):
        """Test send_events_store_batch raises when not connected."""
        client = AsyncClient(address="localhost:50000")
        messages = [EventStoreMessage(channel="test", body=b"data")]

        with pytest.raises(KubeMQConnectionError):
            await client.send_events_store_batch(messages)

    @pytest.mark.asyncio
    async def test_send_events_store_batch_returns_results(self, mock_transport):
        """Test send_events_store_batch returns results for all messages."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        # Setup mock response
        mock_result = pb.Result()
        mock_result.Sent = True
        mock_result.EventID = "event-123"
        mock_transport.send_event.return_value = mock_result

        messages = [EventStoreMessage(channel="test", body=f"msg-{i}".encode()) for i in range(5)]

        results = await client.send_events_store_batch(messages, max_concurrent=3)

        assert len(results) == 5
        assert all(r.sent for r in results)

    @pytest.mark.asyncio
    async def test_send_events_store_batch_handles_partial_errors(self, mock_transport):
        """Test send_events_store_batch handles partial failures."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        # Make send_event fail on every other call
        call_count = [0]

        async def mock_send_event(event):
            call_count[0] += 1
            if call_count[0] % 2 == 0:
                raise Exception("Send failed")
            result = pb.Result()
            result.Sent = True
            result.EventID = f"event-{call_count[0]}"
            return result

        mock_transport.send_event.side_effect = mock_send_event

        messages = [EventStoreMessage(channel="test", body=f"msg-{i}".encode()) for i in range(4)]

        results = await client.send_events_store_batch(messages)

        assert len(results) == 4
        # Some should succeed, some should fail
        successful = [r for r in results if r.sent]
        failed = [r for r in results if not r.sent]
        assert len(successful) == 2
        assert len(failed) == 2


class TestAsyncClientSubscribeToEventsStore:
    """Tests for subscribe_to_events_store method."""

    @pytest.mark.asyncio
    async def test_subscribe_to_events_store_when_not_connected(self):
        """Test subscribe_to_events_store raises when not connected."""
        from kubemq.pubsub.events_store_subscription import EventsStoreSubscription

        client = AsyncClient(address="localhost:50000")
        subscription = EventsStoreSubscription(
            channel="test",
            on_receive_event_callback=lambda e: None,
        )

        with pytest.raises(KubeMQConnectionError):
            async for _ in client.subscribe_to_events_store(subscription):
                pass

    @pytest.mark.asyncio
    async def test_subscribe_to_events_store_yields_messages(self, mock_transport):
        """Test subscribe_to_events_store yields event store messages."""
        from kubemq.pubsub.events_store_subscription import EventsStoreSubscription

        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        # Create mock protobuf events
        mock_pb_event = MagicMock()
        mock_pb_event.EventID = "event-123"
        mock_pb_event.Channel = "test-channel"
        mock_pb_event.Metadata = "test-metadata"
        mock_pb_event.Body = b"test-body"
        mock_pb_event.Timestamp = 1234567890
        mock_pb_event.Sequence = 1
        mock_pb_event.Tags = {}

        # Make the transport return an async iterator
        mock_transport.subscribe_to_events = MagicMock(
            return_value=AsyncIteratorMock([mock_pb_event])
        )

        subscription = EventsStoreSubscription(
            channel="test",
            on_receive_event_callback=lambda e: None,
        )

        events = []
        async for event in client.subscribe_to_events_store(subscription):
            events.append(event)

        assert len(events) == 1
        assert events[0].id == "event-123"


class TestAsyncClientSubscribeWithCallback:
    """Tests for subscribe_with_callback method."""

    @pytest.mark.asyncio
    async def test_subscribe_with_callback_when_not_connected(self):
        """Test subscribe_with_callback raises when not connected."""
        client = AsyncClient(address="localhost:50000")
        subscription = EventsSubscription(
            channel="test",
            on_receive_event_callback=lambda e: None,
        )

        async def callback(event):
            pass

        with pytest.raises(KubeMQConnectionError):
            await client.subscribe_with_callback(subscription, callback)

    @pytest.mark.asyncio
    async def test_subscribe_with_callback_calls_callback(self, mock_transport):
        """Test subscribe_with_callback calls the callback for each event."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        # Create mock protobuf event
        mock_pb_event = MagicMock()
        mock_pb_event.EventID = "event-123"
        mock_pb_event.Channel = "test-channel"
        mock_pb_event.Metadata = ""
        mock_pb_event.Body = b"test-body"
        mock_pb_event.Tags = {}

        mock_transport.subscribe_to_events = MagicMock(
            return_value=AsyncIteratorMock([mock_pb_event])
        )

        subscription = EventsSubscription(
            channel="test",
            on_receive_event_callback=lambda e: None,
        )

        received_events = []

        async def callback(event):
            received_events.append(event)

        await client.subscribe_with_callback(subscription, callback)

        assert len(received_events) == 1

    @pytest.mark.asyncio
    async def test_subscribe_with_callback_calls_error_callback(self, mock_transport):
        """Test subscribe_with_callback calls error callback on error."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        # Create an async iterator that raises an exception
        class ErrorIterator:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise Exception("Stream error")

        mock_transport.subscribe_to_events = MagicMock(return_value=ErrorIterator())

        subscription = EventsSubscription(
            channel="test",
            on_receive_event_callback=lambda e: None,
        )

        errors = []

        async def callback(event):
            pass

        async def error_callback(error):
            errors.append(error)

        await client.subscribe_with_callback(subscription, callback, error_callback)

        assert len(errors) == 1
        assert "Stream error" in str(errors[0])


class TestAsyncClientSubscribeStoreWithCallback:
    """Tests for subscribe_store_with_callback method."""

    @pytest.mark.asyncio
    async def test_subscribe_store_with_callback_when_not_connected(self):
        """Test subscribe_store_with_callback raises when not connected."""
        from kubemq.pubsub.events_store_subscription import EventsStoreSubscription

        client = AsyncClient(address="localhost:50000")
        subscription = EventsStoreSubscription(
            channel="test",
            on_receive_event_callback=lambda e: None,
        )

        async def callback(event):
            pass

        with pytest.raises(KubeMQConnectionError):
            await client.subscribe_store_with_callback(subscription, callback)

    @pytest.mark.asyncio
    async def test_subscribe_store_with_callback_calls_callback(self, mock_transport):
        """Test subscribe_store_with_callback calls callback for each event."""
        from kubemq.pubsub.events_store_subscription import EventsStoreSubscription

        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        # Create mock protobuf event
        mock_pb_event = MagicMock()
        mock_pb_event.EventID = "event-123"
        mock_pb_event.Channel = "test-channel"
        mock_pb_event.Metadata = ""
        mock_pb_event.Body = b"test-body"
        mock_pb_event.Timestamp = 1234567890
        mock_pb_event.Sequence = 1
        mock_pb_event.Tags = {}

        mock_transport.subscribe_to_events = MagicMock(
            return_value=AsyncIteratorMock([mock_pb_event])
        )

        subscription = EventsStoreSubscription(
            channel="test",
            on_receive_event_callback=lambda e: None,
        )

        received_events = []

        async def callback(event):
            received_events.append(event)

        await client.subscribe_store_with_callback(subscription, callback)

        assert len(received_events) == 1
        assert received_events[0].id == "event-123"


class TestAsyncClientSubscriptionCallbackHandling:
    """Tests for subscription callback handling (sync vs async)."""

    @pytest.mark.asyncio
    async def test_subscribe_to_events_calls_sync_callback(self, mock_transport):
        """Test subscribe_to_events calls sync callback."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        # Create mock protobuf event
        mock_pb_event = MagicMock()
        mock_pb_event.EventID = "event-123"
        mock_pb_event.Channel = "test-channel"
        mock_pb_event.Metadata = ""
        mock_pb_event.Body = b"test-body"
        mock_pb_event.Tags = {}

        mock_transport.subscribe_to_events = MagicMock(
            return_value=AsyncIteratorMock([mock_pb_event])
        )

        received_events = []

        def sync_callback(event):
            received_events.append(event)

        subscription = EventsSubscription(
            channel="test",
            on_receive_event_callback=sync_callback,
        )

        async for _ in client.subscribe_to_events(subscription):
            pass

        assert len(received_events) == 1

    @pytest.mark.asyncio
    async def test_subscribe_to_events_calls_async_callback(self, mock_transport):
        """Test subscribe_to_events calls async callback."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        # Create mock protobuf event
        mock_pb_event = MagicMock()
        mock_pb_event.EventID = "event-123"
        mock_pb_event.Channel = "test-channel"
        mock_pb_event.Metadata = ""
        mock_pb_event.Body = b"test-body"
        mock_pb_event.Tags = {}

        mock_transport.subscribe_to_events = MagicMock(
            return_value=AsyncIteratorMock([mock_pb_event])
        )

        received_events = []

        async def async_callback(event):
            received_events.append(event)

        subscription = EventsSubscription(
            channel="test",
            on_receive_event_callback=async_callback,
        )

        async for _ in client.subscribe_to_events(subscription):
            pass

        assert len(received_events) == 1

    @pytest.mark.asyncio
    async def test_subscribe_to_events_calls_error_callback_on_exception(self, mock_transport):
        """Test subscribe_to_events calls error callback on exception."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        # Create an async iterator that raises an exception
        class ErrorIterator:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise Exception("Stream error")

        mock_transport.subscribe_to_events = MagicMock(return_value=ErrorIterator())

        errors = []

        def error_callback(error):
            errors.append(error)

        subscription = EventsSubscription(
            channel="test",
            on_receive_event_callback=lambda e: None,
            on_error_callback=error_callback,
        )

        with pytest.raises(Exception, match="Stream error"):
            async for _ in client.subscribe_to_events(subscription):
                pass

        assert len(errors) == 1
        assert "Stream error" in errors[0]
