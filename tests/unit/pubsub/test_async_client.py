"""Tests for AsyncPubSubClient."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kubemq.common.async_cancellation_token import AsyncCancellationToken
from kubemq.core.config import ClientConfig
from kubemq.core.exceptions import KubeMQConnectionError, KubeMQError, KubeMQStreamBrokenError
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
        """Test send_event properly encodes the message via bidi stream sender."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        mock_sender = AsyncMock()
        mock_sender.send = AsyncMock(return_value=None)
        client._event_sender = mock_sender

        message = EventMessage(
            channel="test-channel",
            body=b"test body",
            metadata="test metadata",
        )

        await client.send_event(message)

        mock_sender.send.assert_called_once()
        pb_event = mock_sender.send.call_args[0][0]
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
        """Test send_event_store returns EventSendResult via bidi stream sender."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        mock_result = pb.Result()
        mock_result.Sent = True
        mock_result.EventID = "event-123"

        mock_sender = AsyncMock()
        mock_sender.send = AsyncMock(return_value=mock_result)
        client._event_sender = mock_sender

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

        mock_sender = AsyncMock()
        mock_sender.send = AsyncMock(return_value=None)
        client._event_sender = mock_sender

        messages = [EventMessage(channel="test", body=f"msg-{i}".encode()) for i in range(10)]

        results = await client.send_events_batch(messages, max_concurrent=5)

        assert len(results) == 10
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

        mock_result = pb.Result()
        mock_result.Sent = True
        mock_result.EventID = "event-123"

        mock_sender = AsyncMock()
        mock_sender.send = AsyncMock(return_value=mock_result)
        client._event_sender = mock_sender

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

        call_count = [0]

        async def mock_send(event):
            call_count[0] += 1
            if call_count[0] % 2 == 0:
                raise Exception("Send failed")
            result = pb.Result()
            result.Sent = True
            result.EventID = f"event-{call_count[0]}"
            return result

        mock_sender = AsyncMock()
        mock_sender.send = AsyncMock(side_effect=mock_send)
        client._event_sender = mock_sender

        messages = [EventStoreMessage(channel="test", body=f"msg-{i}".encode()) for i in range(4)]

        results = await client.send_events_store_batch(messages)

        assert len(results) == 4
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


# ==============================================================================
# Additional Coverage Tests
# ==============================================================================


class TestAsyncClientPublishEventTelemetry:
    """Tests for publish_event / publish_event_store telemetry paths."""

    @pytest.mark.asyncio
    async def test_publish_event_calls_transport(self, mock_transport):
        """Verify publish_event encodes and calls bidi stream sender."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        mock_sender = AsyncMock()
        mock_sender.send = AsyncMock(return_value=None)
        client._event_sender = mock_sender

        message = EventMessage(
            channel="test-channel",
            body=b"hello",
            metadata="meta",
        )

        await client.publish_event(message)

        mock_sender.send.assert_called_once()
        pb_event = mock_sender.send.call_args[0][0]
        assert isinstance(pb_event, pb.Event)
        assert pb_event.Channel == "test-channel"
        assert pb_event.Body == b"hello"

    @pytest.mark.asyncio
    async def test_publish_event_store_returns_result(self, mock_transport):
        """Verify publish_event_store returns proper EventSendResult."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        mock_result = pb.Result()
        mock_result.Sent = True
        mock_result.EventID = "es-456"
        mock_result.Error = ""

        mock_sender = AsyncMock()
        mock_sender.send = AsyncMock(return_value=mock_result)
        client._event_sender = mock_sender

        message = EventStoreMessage(
            channel="store-channel",
            body=b"persistent",
        )

        result = await client.publish_event_store(message)

        assert result.sent is True
        assert result.id == "es-456"


class TestAsyncClientSubscribeToEventsYield:
    """Tests for subscribe_to_events / subscribe_to_events_store yielding messages."""

    @pytest.mark.asyncio
    async def test_subscribe_to_events_yields_messages(self, mock_transport):
        """Verify subscribe_to_events yields EventMessageReceived from pb events."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        mock_pb_event = MagicMock()
        mock_pb_event.EventID = "event-123"
        mock_pb_event.Channel = "test-channel"
        mock_pb_event.Metadata = "some-meta"
        mock_pb_event.Body = b"test-body"
        mock_pb_event.Tags = {"key": "value"}

        mock_transport.subscribe_to_events = MagicMock(
            return_value=AsyncIteratorMock([mock_pb_event])
        )

        subscription = EventsSubscription(
            channel="test-channel",
            on_receive_event_callback=lambda e: None,
        )

        events = []
        async for event in client.subscribe_to_events(subscription):
            events.append(event)

        assert len(events) == 1
        assert events[0].id == "event-123"
        assert events[0].channel == "test-channel"
        assert events[0].metadata == "some-meta"
        assert events[0].body == b"test-body"
        assert events[0].tags["key"] == "value"

    @pytest.mark.asyncio
    async def test_subscribe_to_events_store_calls_async_callback(self, mock_transport):
        """Verify subscribe_to_events_store invokes async callback with decoded event."""
        from kubemq.pubsub.events_store_subscription import EventsStoreSubscription

        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        mock_pb_event = MagicMock()
        mock_pb_event.EventID = "es-789"
        mock_pb_event.Channel = "store-channel"
        mock_pb_event.Metadata = "store-meta"
        mock_pb_event.Body = b"store-body"
        mock_pb_event.Timestamp = 1234567890
        mock_pb_event.Sequence = 42
        mock_pb_event.Tags = {"env": "test"}

        received = []

        async def async_cb(event):
            received.append(event)

        mock_transport.subscribe_to_events = MagicMock(
            return_value=AsyncIteratorMock([mock_pb_event])
        )

        subscription = EventsStoreSubscription(
            channel="store-channel",
            on_receive_event_callback=async_cb,
        )

        events = []
        async for event in client.subscribe_to_events_store(subscription):
            events.append(event)

        assert len(events) == 1
        assert events[0].id == "es-789"
        assert events[0].channel == "store-channel"
        assert events[0].body == b"store-body"
        assert events[0].sequence == 42
        assert len(received) == 1


class TestAsyncClientSubscribeWithCallbackConcurrent:
    """Tests for concurrent callback processing paths."""

    @pytest.mark.asyncio
    async def test_subscribe_with_callback_concurrent(self, mock_transport):
        """Concurrent path: max_concurrent_callbacks=3, deliver 5 events, all processed."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        pb_events = []
        for i in range(5):
            ev = MagicMock()
            ev.EventID = f"ev-{i}"
            ev.Channel = "ch"
            ev.Metadata = ""
            ev.Body = f"body-{i}".encode()
            ev.Tags = {}
            pb_events.append(ev)

        mock_transport.subscribe_to_events = MagicMock(return_value=AsyncIteratorMock(pb_events))

        received = []

        async def callback(event):
            received.append(event.id)

        await client.subscribe_with_callback(
            EventsSubscription(channel="ch", on_receive_event_callback=lambda e: None),
            callback,
            max_concurrent_callbacks=3,
        )

        assert sorted(received) == [f"ev-{i}" for i in range(5)]

    @pytest.mark.asyncio
    async def test_subscribe_store_with_callback_concurrent(self, mock_transport):
        """Concurrent path for event store: max_concurrent_callbacks=3, deliver 5 events."""
        from kubemq.pubsub.events_store_subscription import EventsStoreSubscription

        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        pb_events = []
        for i in range(5):
            ev = MagicMock()
            ev.EventID = f"es-{i}"
            ev.Channel = "store-ch"
            ev.Metadata = ""
            ev.Body = f"body-{i}".encode()
            ev.Timestamp = 1234567890
            ev.Sequence = i + 1
            ev.Tags = {}
            pb_events.append(ev)

        mock_transport.subscribe_to_events = MagicMock(return_value=AsyncIteratorMock(pb_events))

        received = []

        async def callback(event):
            received.append(event.id)

        await client.subscribe_store_with_callback(
            EventsStoreSubscription(channel="store-ch", on_receive_event_callback=lambda e: None),
            callback,
            max_concurrent_callbacks=3,
        )

        assert sorted(received) == [f"es-{i}" for i in range(5)]


class TestAsyncClientSubscriptionValidation:
    """Tests for concurrency validation in subscribe_with_callback / subscribe_store_with_callback."""

    @pytest.mark.asyncio
    async def test_subscribe_with_callback_rejects_zero_concurrency(self, mock_transport):
        """ValueError when max_concurrent_callbacks=0."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        with pytest.raises(ValueError, match="must be >= 1"):
            await client.subscribe_with_callback(
                EventsSubscription(channel="ch", on_receive_event_callback=lambda e: None),
                AsyncMock(),
                max_concurrent_callbacks=0,
            )

    @pytest.mark.asyncio
    async def test_subscribe_with_callback_rejects_over_1000(self, mock_transport):
        """ValueError when max_concurrent_callbacks=1001."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        with pytest.raises(ValueError, match="must be <= 1000"):
            await client.subscribe_with_callback(
                EventsSubscription(channel="ch", on_receive_event_callback=lambda e: None),
                AsyncMock(),
                max_concurrent_callbacks=1001,
            )

    @pytest.mark.asyncio
    async def test_subscribe_store_with_callback_rejects_zero(self, mock_transport):
        """ValueError when max_concurrent_callbacks=0 for store subscription."""
        from kubemq.pubsub.events_store_subscription import EventsStoreSubscription

        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        with pytest.raises(ValueError, match="must be >= 1"):
            await client.subscribe_store_with_callback(
                EventsStoreSubscription(channel="ch", on_receive_event_callback=lambda e: None),
                AsyncMock(),
                max_concurrent_callbacks=0,
            )

    @pytest.mark.asyncio
    async def test_subscribe_store_with_callback_rejects_over_1000(self, mock_transport):
        """ValueError when max_concurrent_callbacks=1001 for store subscription."""
        from kubemq.pubsub.events_store_subscription import EventsStoreSubscription

        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        with pytest.raises(ValueError, match="must be <= 1000"):
            await client.subscribe_store_with_callback(
                EventsStoreSubscription(channel="ch", on_receive_event_callback=lambda e: None),
                AsyncMock(),
                max_concurrent_callbacks=1001,
            )

    @pytest.mark.asyncio
    async def test_subscribe_with_callback_rejects_negative_concurrency(self, mock_transport):
        """ValueError when max_concurrent_callbacks is negative."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        with pytest.raises(ValueError, match="must be >= 1"):
            await client.subscribe_with_callback(
                EventsSubscription(channel="ch", on_receive_event_callback=lambda e: None),
                AsyncMock(),
                max_concurrent_callbacks=-5,
            )


class TestAsyncClientSubscribeHandlerErrorIsolation:
    """Tests for handler error isolation via error_callback."""

    @pytest.mark.asyncio
    async def test_subscribe_with_callback_handler_error_calls_error_callback(self, mock_transport):
        """Callback raises -> error_callback receives KubeMQHandlerError."""
        from kubemq.core.exceptions import KubeMQHandlerError

        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        mock_pb_event = MagicMock()
        mock_pb_event.EventID = "err-1"
        mock_pb_event.Channel = "ch"
        mock_pb_event.Metadata = ""
        mock_pb_event.Body = b"data"
        mock_pb_event.Tags = {}

        mock_transport.subscribe_to_events = MagicMock(
            return_value=AsyncIteratorMock([mock_pb_event])
        )

        errors = []

        async def bad_callback(event):
            raise RuntimeError("handler boom")

        async def err_callback(error):
            errors.append(error)

        await client.subscribe_with_callback(
            EventsSubscription(channel="ch", on_receive_event_callback=lambda e: None),
            bad_callback,
            error_callback=err_callback,
        )

        assert len(errors) == 1
        assert isinstance(errors[0], KubeMQHandlerError)
        assert "handler boom" in str(errors[0])

    @pytest.mark.asyncio
    async def test_subscribe_store_with_callback_handler_error_calls_error_callback(
        self, mock_transport
    ):
        """Store callback raises -> error_callback receives KubeMQHandlerError."""
        from kubemq.core.exceptions import KubeMQHandlerError
        from kubemq.pubsub.events_store_subscription import EventsStoreSubscription

        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        mock_pb_event = MagicMock()
        mock_pb_event.EventID = "err-2"
        mock_pb_event.Channel = "store-ch"
        mock_pb_event.Metadata = ""
        mock_pb_event.Body = b"store-data"
        mock_pb_event.Timestamp = 1234567890
        mock_pb_event.Sequence = 1
        mock_pb_event.Tags = {}

        mock_transport.subscribe_to_events = MagicMock(
            return_value=AsyncIteratorMock([mock_pb_event])
        )

        errors = []

        async def bad_callback(event):
            raise ValueError("store handler boom")

        async def err_callback(error):
            errors.append(error)

        await client.subscribe_store_with_callback(
            EventsStoreSubscription(channel="store-ch", on_receive_event_callback=lambda e: None),
            bad_callback,
            error_callback=err_callback,
        )

        assert len(errors) == 1
        assert isinstance(errors[0], KubeMQHandlerError)
        assert "store handler boom" in str(errors[0])


# ==============================================================================
# Coverage-targeted tests for uncovered lines
# ==============================================================================


def _make_pb_event(event_id="ev-1", channel="ch", body=b"data", tags=None):
    """Helper to create a mock protobuf event."""
    ev = MagicMock()
    ev.EventID = event_id
    ev.Channel = channel
    ev.Metadata = ""
    ev.Body = body
    ev.Tags = tags or {}
    ev.Timestamp = 1234567890
    ev.Sequence = 1
    return ev


class RaisingAsyncIterator:
    """Async iterator that yields items then raises."""

    def __init__(self, items, exc):
        self._items = iter(items)
        self._exc = exc

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._items)
        except StopIteration:
            raise self._exc


class OneShotRetryIterator:
    """First call raises, second call yields items then stops.

    Used to test retry logic: first iteration raises a retryable error,
    second iteration yields events normally.
    """

    def __init__(self, exc, items):
        self._exc = exc
        self._items = items
        self._call_count = 0

    def __call__(self, *args, **kwargs):
        self._call_count += 1
        if self._call_count == 1:
            return RaisingAsyncIterator([], self._exc)
        return AsyncIteratorMock(self._items)


def _make_connected_client(mock_transport):
    """Create an AsyncClient wired to a mock transport and mock event sender."""
    client = AsyncClient(address="localhost:50000")
    client._transport = mock_transport
    client._connected = True  # type: ignore[attr-defined]
    mock_sender = AsyncMock()
    mock_sender.send = AsyncMock(return_value=None)
    client._event_sender = mock_sender
    return client


# ---------------------------------------------------------------------------
# publish_event / publish_event_store: ValidationError, generic exception,
# telemetry span, and metrics recording
# ---------------------------------------------------------------------------


class TestPublishEventErrorPaths:
    """Cover lines 133-153, 190-207: ValidationError, generic exception,
    span recording, and metrics calls."""

    @pytest.mark.asyncio
    async def test_publish_event_validation_error(self, mock_transport):
        """ValidationError from encode -> KubeMQValidationError (lines 146-149)."""
        from pydantic import ValidationError as PydanticValidationError

        from kubemq.core.exceptions import KubeMQValidationError

        client = _make_connected_client(mock_transport)

        msg = EventMessage(channel="ch", body=b"x")
        with patch.object(
            EventMessage,
            "encode",
            side_effect=PydanticValidationError.from_exception_data(
                title="EventMessage",
                line_errors=[
                    {
                        "type": "value_error",
                        "loc": ("channel",),
                        "msg": "bad",
                        "input": "",
                        "ctx": {"error": ValueError("bad")},
                    }
                ],
            ),
        ):
            with pytest.raises(KubeMQValidationError):
                await client.publish_event(msg)

    @pytest.mark.asyncio
    async def test_publish_event_generic_exception_reraises(self, mock_transport):
        """Generic exception from event sender -> re-raised."""
        client = _make_connected_client(mock_transport)
        mock_sender = AsyncMock()
        mock_sender.send = AsyncMock(side_effect=RuntimeError("boom"))
        client._event_sender = mock_sender

        msg = EventMessage(channel="ch", body=b"x")
        with pytest.raises(RuntimeError, match="boom"):
            await client.publish_event(msg)

    @pytest.mark.asyncio
    async def test_publish_event_span_attributes_set(self, mock_transport):
        """When span.is_recording() is True, span attributes are set (lines 132-138)."""
        client = _make_connected_client(mock_transport)

        mock_span = MagicMock()
        mock_span.is_recording.return_value = True
        mock_span.__enter__ = MagicMock(return_value=mock_span)
        mock_span.__exit__ = MagicMock(return_value=False)

        mock_instrumentor = MagicMock()
        mock_instrumentor.start_span.return_value = mock_span
        client._instrumentor = mock_instrumentor

        msg = EventMessage(channel="ch", body=b"hello")
        await client.publish_event(msg)

        assert mock_span.set_attribute.call_count == 2

    @pytest.mark.asyncio
    async def test_publish_event_metrics_recorded_on_success(self, mock_transport):
        """Metrics record_sent_message called on success (line 145)."""
        client = _make_connected_client(mock_transport)

        mock_instrumentor = MagicMock()
        mock_span = MagicMock()
        mock_span.__enter__ = MagicMock(return_value=mock_span)
        mock_span.__exit__ = MagicMock(return_value=False)
        mock_span.is_recording.return_value = False
        mock_instrumentor.start_span.return_value = mock_span
        client._instrumentor = mock_instrumentor

        msg = EventMessage(channel="ch", body=b"x")
        await client.publish_event(msg)

        mock_instrumentor._metrics.record_sent_message.assert_called_once_with("publish", "ch")
        mock_instrumentor._metrics.record_operation_duration.assert_called_once()

    @pytest.mark.asyncio
    async def test_publish_event_metrics_recorded_on_failure(self, mock_transport):
        """Metrics record_operation_duration called even on failure (finally block)."""
        client = _make_connected_client(mock_transport)
        client._event_sender.send = AsyncMock(side_effect=RuntimeError("fail"))

        mock_instrumentor = MagicMock()
        mock_span = MagicMock()
        mock_span.__enter__ = MagicMock(return_value=mock_span)
        mock_span.__exit__ = MagicMock(return_value=False)
        mock_span.is_recording.return_value = False
        mock_instrumentor.start_span.return_value = mock_span
        client._instrumentor = mock_instrumentor

        msg = EventMessage(channel="ch", body=b"x")
        with pytest.raises(RuntimeError):
            await client.publish_event(msg)

        mock_instrumentor._metrics.record_operation_duration.assert_called_once()

    @pytest.mark.asyncio
    async def test_publish_event_store_validation_error(self, mock_transport):
        """ValidationError from encode -> KubeMQValidationError (lines 204-207)."""
        from pydantic import ValidationError as PydanticValidationError

        from kubemq.core.exceptions import KubeMQValidationError

        client = _make_connected_client(mock_transport)

        msg = EventStoreMessage(channel="ch", body=b"x")
        with patch.object(
            EventStoreMessage,
            "encode",
            side_effect=PydanticValidationError.from_exception_data(
                title="EventStoreMessage",
                line_errors=[
                    {
                        "type": "value_error",
                        "loc": ("channel",),
                        "msg": "bad",
                        "input": "",
                        "ctx": {"error": ValueError("bad")},
                    }
                ],
            ),
        ):
            with pytest.raises(KubeMQValidationError):
                await client.publish_event_store(msg)

    @pytest.mark.asyncio
    async def test_publish_event_store_generic_exception(self, mock_transport):
        """Generic exception from event sender -> re-raised."""
        client = _make_connected_client(mock_transport)
        client._event_sender.send = AsyncMock(side_effect=RuntimeError("store boom"))

        msg = EventStoreMessage(channel="ch", body=b"x")
        with pytest.raises(RuntimeError, match="store boom"):
            await client.publish_event_store(msg)

    @pytest.mark.asyncio
    async def test_publish_event_store_span_attributes(self, mock_transport):
        """Span attributes set when is_recording is True."""
        client = _make_connected_client(mock_transport)

        mock_span = MagicMock()
        mock_span.is_recording.return_value = True
        mock_span.__enter__ = MagicMock(return_value=mock_span)
        mock_span.__exit__ = MagicMock(return_value=False)

        mock_instrumentor = MagicMock()
        mock_instrumentor.start_span.return_value = mock_span
        client._instrumentor = mock_instrumentor

        mock_result = pb.Result()
        mock_result.Sent = True
        mock_result.EventID = "id-1"
        mock_result.Error = ""
        client._event_sender.send = AsyncMock(return_value=mock_result)

        msg = EventStoreMessage(channel="ch", body=b"hello")
        result = await client.publish_event_store(msg)

        assert mock_span.set_attribute.call_count == 2
        assert result.sent is True


# ---------------------------------------------------------------------------
# send_events_batch: partial failure
# ---------------------------------------------------------------------------


class TestSendEventsBatchFailure:
    """Cover lines 258-259: bounded_send exception path."""

    @pytest.mark.asyncio
    async def test_send_events_batch_partial_failure(self, mock_transport):
        """Some sends fail -> EventSendResult with sent=False."""
        client = _make_connected_client(mock_transport)

        call_count = [0]

        async def mock_send(pb_event):
            call_count[0] += 1
            if call_count[0] % 2 == 0:
                raise RuntimeError("send failed")
            return None

        client._event_sender.send = AsyncMock(side_effect=mock_send)

        messages = [EventMessage(channel="ch", body=f"m{i}".encode()) for i in range(4)]
        results = await client.send_events_batch(messages)

        assert len(results) == 4
        failed = [r for r in results if not r.sent]
        succeeded = [r for r in results if r.sent]
        assert len(failed) == 2
        assert len(succeeded) == 2
        for f in failed:
            assert "send failed" in f.error


# ---------------------------------------------------------------------------
# subscribe_to_events: handler error, retryable error, non-retryable,
# generic exception
# ---------------------------------------------------------------------------


class TestSubscribeToEventsHandlerErrors:
    """Cover lines 394-410, 415-420: handler errors in subscribe_to_events."""

    @pytest.mark.asyncio
    async def test_handler_error_with_error_callback(self, mock_transport):
        """Handler raises -> KubeMQHandlerError -> on_error_callback called (lines 394-408)."""

        client = _make_connected_client(mock_transport)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(return_value=AsyncIteratorMock([pb_ev]))

        errors = []

        def bad_handler(event):
            raise ValueError("handler crash")

        def err_cb(msg):
            errors.append(msg)

        sub = EventsSubscription(
            channel="ch",
            on_receive_event_callback=bad_handler,
            on_error_callback=err_cb,
        )

        events = []
        async for ev in client.subscribe_to_events(sub):
            events.append(ev)

        assert len(events) == 1
        assert len(errors) == 1
        assert "handler crash" in errors[0]

    @pytest.mark.asyncio
    async def test_handler_error_without_error_callback(self, mock_transport):
        """Handler raises, no error_callback -> logged (lines 409-410)."""
        client = _make_connected_client(mock_transport)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(return_value=AsyncIteratorMock([pb_ev]))

        def bad_handler(event):
            raise ValueError("handler crash")

        sub = EventsSubscription(
            channel="ch",
            on_receive_event_callback=bad_handler,
        )

        events = []
        async for ev in client.subscribe_to_events(sub):
            events.append(ev)

        assert len(events) == 1

    @pytest.mark.asyncio
    async def test_handler_error_async_error_callback(self, mock_transport):
        """Handler raises -> async on_error_callback called."""
        client = _make_connected_client(mock_transport)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(return_value=AsyncIteratorMock([pb_ev]))

        errors = []

        def bad_handler(event):
            raise ValueError("async err test")

        async def async_err_cb(msg):
            errors.append(msg)

        sub = EventsSubscription(
            channel="ch",
            on_receive_event_callback=bad_handler,
            on_error_callback=async_err_cb,
        )

        async for _ in client.subscribe_to_events(sub):
            pass

        assert len(errors) == 1
        assert "async err test" in errors[0]


class TestSubscribeToEventsStreamErrors:
    """Cover lines 431-470: retryable, non-retryable, generic errors."""

    @pytest.mark.asyncio
    async def test_retryable_error_backoff_and_retry(self, mock_transport):
        """Retryable KubeMQConnectionError -> backoff, retry, stream reconnect (lines 431-461)."""
        client = _make_connected_client(mock_transport)

        retryable_err = KubeMQConnectionError("connection lost", is_retryable=True)
        pb_ev = _make_pb_event()

        mock_transport.subscribe_to_events = OneShotRetryIterator(retryable_err, [pb_ev])

        errors = []

        def err_cb(msg):
            errors.append(msg)

        sub = EventsSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
            on_error_callback=err_cb,
        )

        events = []
        with patch("kubemq.pubsub.async_client.asyncio.sleep", new_callable=AsyncMock):
            async for ev in client.subscribe_to_events(sub):
                events.append(ev)

        assert len(events) == 1
        assert len(errors) == 1
        assert "Stream broken" in errors[0]

    @pytest.mark.asyncio
    async def test_non_retryable_error_with_callback(self, mock_transport):
        """Non-retryable KubeMQError with on_error_callback -> callback called, raises (lines 432-439)."""
        client = _make_connected_client(mock_transport)

        non_retryable = KubeMQError("fatal", is_retryable=False)

        mock_transport.subscribe_to_events = MagicMock(
            return_value=RaisingAsyncIterator([], non_retryable)
        )

        errors = []

        def err_cb(msg):
            errors.append(msg)

        sub = EventsSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
            on_error_callback=err_cb,
        )

        with pytest.raises(KubeMQError, match="fatal"):
            async for _ in client.subscribe_to_events(sub):
                pass

        assert len(errors) == 1

    @pytest.mark.asyncio
    async def test_non_retryable_error_without_callback(self, mock_transport):
        """Non-retryable KubeMQError, no callback -> re-raises."""
        client = _make_connected_client(mock_transport)

        non_retryable = KubeMQError("fatal no cb", is_retryable=False)
        mock_transport.subscribe_to_events = MagicMock(
            return_value=RaisingAsyncIterator([], non_retryable)
        )

        sub = EventsSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
        )

        with pytest.raises(KubeMQError, match="fatal no cb"):
            async for _ in client.subscribe_to_events(sub):
                pass

    @pytest.mark.asyncio
    async def test_generic_exception_with_callback(self, mock_transport):
        """Generic Exception with on_error_callback -> callback called then raises (lines 463-470)."""
        client = _make_connected_client(mock_transport)

        mock_transport.subscribe_to_events = MagicMock(
            return_value=RaisingAsyncIterator([], RuntimeError("unexpected"))
        )

        errors = []

        def err_cb(msg):
            errors.append(msg)

        sub = EventsSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
            on_error_callback=err_cb,
        )

        with pytest.raises(RuntimeError, match="unexpected"):
            async for _ in client.subscribe_to_events(sub):
                pass

        assert len(errors) == 1
        assert "unexpected" in errors[0]

    @pytest.mark.asyncio
    async def test_generic_exception_without_callback(self, mock_transport):
        """Generic Exception, no callback -> re-raises."""
        client = _make_connected_client(mock_transport)

        mock_transport.subscribe_to_events = MagicMock(
            return_value=RaisingAsyncIterator([], RuntimeError("no cb"))
        )

        sub = EventsSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
        )

        with pytest.raises(RuntimeError, match="no cb"):
            async for _ in client.subscribe_to_events(sub):
                pass

    @pytest.mark.asyncio
    async def test_generic_exception_with_async_callback(self, mock_transport):
        """Generic Exception with async on_error_callback (line 467)."""
        client = _make_connected_client(mock_transport)

        mock_transport.subscribe_to_events = MagicMock(
            return_value=RaisingAsyncIterator([], RuntimeError("async err"))
        )

        errors = []

        async def async_err_cb(msg):
            errors.append(msg)

        sub = EventsSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
            on_error_callback=async_err_cb,
        )

        with pytest.raises(RuntimeError, match="async err"):
            async for _ in client.subscribe_to_events(sub):
                pass

        assert len(errors) == 1


# ---------------------------------------------------------------------------
# subscribe_to_events_store: same patterns as events
# ---------------------------------------------------------------------------


class TestSubscribeToEventsStoreErrors:
    """Cover lines 507-613 for event store subscription errors."""

    @pytest.mark.asyncio
    async def test_handler_error_with_callback(self, mock_transport):
        """Handler raises -> on_error_callback called (lines 534-550)."""
        from kubemq.pubsub.events_store_subscription import EventsStoreSubscription

        client = _make_connected_client(mock_transport)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(return_value=AsyncIteratorMock([pb_ev]))

        errors = []

        def bad_handler(event):
            raise ValueError("store handler err")

        def err_cb(msg):
            errors.append(msg)

        sub = EventsStoreSubscription(
            channel="ch",
            on_receive_event_callback=bad_handler,
            on_error_callback=err_cb,
        )

        events = []
        async for ev in client.subscribe_to_events_store(sub):
            events.append(ev)

        assert len(events) == 1
        assert len(errors) == 1
        assert "store handler err" in errors[0]

    @pytest.mark.asyncio
    async def test_handler_error_without_callback(self, mock_transport):
        """Handler raises, no error_callback -> logged (line 550)."""
        from kubemq.pubsub.events_store_subscription import EventsStoreSubscription

        client = _make_connected_client(mock_transport)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(return_value=AsyncIteratorMock([pb_ev]))

        def bad_handler(event):
            raise ValueError("no cb test")

        sub = EventsStoreSubscription(
            channel="ch",
            on_receive_event_callback=bad_handler,
        )

        events = []
        async for ev in client.subscribe_to_events_store(sub):
            events.append(ev)

        assert len(events) == 1

    @pytest.mark.asyncio
    async def test_retryable_error_backoff(self, mock_transport):
        """Retryable error -> backoff and retry (lines 571-601)."""
        from kubemq.pubsub.events_store_subscription import EventsStoreSubscription

        client = _make_connected_client(mock_transport)

        retryable_err = KubeMQConnectionError("lost", is_retryable=True)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = OneShotRetryIterator(retryable_err, [pb_ev])

        errors = []

        def err_cb(msg):
            errors.append(msg)

        sub = EventsStoreSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
            on_error_callback=err_cb,
        )

        events = []
        with patch("kubemq.pubsub.async_client.asyncio.sleep", new_callable=AsyncMock):
            async for ev in client.subscribe_to_events_store(sub):
                events.append(ev)

        assert len(events) == 1
        assert len(errors) == 1
        assert "Stream broken" in errors[0]

    @pytest.mark.asyncio
    async def test_non_retryable_error_with_callback(self, mock_transport):
        """Non-retryable with callback (lines 571-579)."""
        from kubemq.pubsub.events_store_subscription import EventsStoreSubscription

        client = _make_connected_client(mock_transport)

        non_retryable = KubeMQError("store fatal", is_retryable=False)
        mock_transport.subscribe_to_events = MagicMock(
            return_value=RaisingAsyncIterator([], non_retryable)
        )

        errors = []

        def err_cb(msg):
            errors.append(msg)

        sub = EventsStoreSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
            on_error_callback=err_cb,
        )

        with pytest.raises(KubeMQError, match="store fatal"):
            async for _ in client.subscribe_to_events_store(sub):
                pass

        assert len(errors) == 1

    @pytest.mark.asyncio
    async def test_generic_exception_with_callback(self, mock_transport):
        """Generic Exception with callback (lines 603-610)."""
        from kubemq.pubsub.events_store_subscription import EventsStoreSubscription

        client = _make_connected_client(mock_transport)

        mock_transport.subscribe_to_events = MagicMock(
            return_value=RaisingAsyncIterator([], RuntimeError("store unexpected"))
        )

        errors = []

        def err_cb(msg):
            errors.append(msg)

        sub = EventsStoreSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
            on_error_callback=err_cb,
        )

        with pytest.raises(RuntimeError, match="store unexpected"):
            async for _ in client.subscribe_to_events_store(sub):
                pass

        assert len(errors) == 1

    @pytest.mark.asyncio
    async def test_generic_exception_without_callback(self, mock_transport):
        """Generic Exception, no callback -> re-raises."""
        from kubemq.pubsub.events_store_subscription import EventsStoreSubscription

        client = _make_connected_client(mock_transport)

        mock_transport.subscribe_to_events = MagicMock(
            return_value=RaisingAsyncIterator([], RuntimeError("store no cb"))
        )

        sub = EventsStoreSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
        )

        with pytest.raises(RuntimeError, match="store no cb"):
            async for _ in client.subscribe_to_events_store(sub):
                pass

    @pytest.mark.asyncio
    async def test_handler_error_with_async_error_callback(self, mock_transport):
        """Async handler raises with async error callback."""
        from kubemq.pubsub.events_store_subscription import EventsStoreSubscription

        client = _make_connected_client(mock_transport)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(return_value=AsyncIteratorMock([pb_ev]))

        errors = []

        async def bad_handler(event):
            raise ValueError("async store handler")

        async def async_err_cb(msg):
            errors.append(msg)

        sub = EventsStoreSubscription(
            channel="ch",
            on_receive_event_callback=bad_handler,
            on_error_callback=async_err_cb,
        )

        events = []
        async for ev in client.subscribe_to_events_store(sub):
            events.append(ev)

        assert len(events) == 1
        assert len(errors) == 1


# ---------------------------------------------------------------------------
# subscribe_with_callback: sequential, concurrent, error paths
# ---------------------------------------------------------------------------


class TestSubscribeWithCallbackSequential:
    """Cover lines 676-730: sequential callback path."""

    @pytest.mark.asyncio
    async def test_sequential_handler_error_with_callback(self, mock_transport):
        """Sequential: handler raises -> error_callback (lines 704-714)."""
        from kubemq.core.exceptions import KubeMQHandlerError

        client = _make_connected_client(mock_transport)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(return_value=AsyncIteratorMock([pb_ev]))

        errors = []

        async def bad_cb(event):
            raise ValueError("seq boom")

        async def err_cb(error):
            errors.append(error)

        await client.subscribe_with_callback(
            EventsSubscription(channel="ch", on_receive_event_callback=lambda e: None),
            bad_cb,
            error_callback=err_cb,
            max_concurrent_callbacks=1,
        )

        assert len(errors) == 1
        assert isinstance(errors[0], KubeMQHandlerError)

    @pytest.mark.asyncio
    async def test_sequential_handler_error_no_callback(self, mock_transport):
        """Sequential: handler raises, no error_callback -> logged (lines 715-716)."""
        client = _make_connected_client(mock_transport)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(return_value=AsyncIteratorMock([pb_ev]))

        async def bad_cb(event):
            raise ValueError("seq no cb")

        with patch("kubemq.pubsub.async_client._logger") as mock_logger:
            await client.subscribe_with_callback(
                EventsSubscription(channel="ch", on_receive_event_callback=lambda e: None),
                bad_cb,
                max_concurrent_callbacks=1,
            )
            mock_logger.error.assert_called()

    @pytest.mark.asyncio
    async def test_sequential_link_appended(self, mock_transport):
        """Verify link is created when parent context is present (line 692+)."""
        client = _make_connected_client(mock_transport)
        pb_ev = _make_pb_event(tags={"traceparent": "00-abc-def-01"})
        mock_transport.subscribe_to_events = MagicMock(return_value=AsyncIteratorMock([pb_ev]))

        received = []

        async def cb(event):
            received.append(event)

        await client.subscribe_with_callback(
            EventsSubscription(channel="ch", on_receive_event_callback=lambda e: None),
            cb,
            max_concurrent_callbacks=1,
        )

        assert len(received) == 1


class TestSubscribeWithCallbackConcurrentErrors:
    """Cover lines 734-762: concurrent callback error paths."""

    @pytest.mark.asyncio
    async def test_concurrent_handler_error_with_callback(self, mock_transport):
        """Concurrent: handler error -> error_callback (lines 738-743)."""
        client = _make_connected_client(mock_transport)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(return_value=AsyncIteratorMock([pb_ev]))

        errors = []

        async def bad_cb(event):
            raise ValueError("concurrent boom")

        async def err_cb(error):
            errors.append(error)

        await client.subscribe_with_callback(
            EventsSubscription(channel="ch", on_receive_event_callback=lambda e: None),
            bad_cb,
            error_callback=err_cb,
            max_concurrent_callbacks=3,
        )

        await asyncio.sleep(0.05)
        assert len(errors) == 1

    @pytest.mark.asyncio
    async def test_concurrent_handler_error_no_callback(self, mock_transport):
        """Concurrent: handler error, no error_callback -> logger.error (lines 744-749)."""
        client = _make_connected_client(mock_transport)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(return_value=AsyncIteratorMock([pb_ev]))

        async def bad_cb(event):
            raise ValueError("concurrent no cb")

        client._logger = MagicMock()

        await client.subscribe_with_callback(
            EventsSubscription(channel="ch", on_receive_event_callback=lambda e: None),
            bad_cb,
            max_concurrent_callbacks=3,
        )

        await asyncio.sleep(0.05)
        client._logger.error.assert_called()


class TestSubscribeWithCallbackStreamErrors:
    """Cover lines 766-797: KubeMQError and generic errors in subscribe_with_callback."""

    @pytest.mark.asyncio
    async def test_retryable_error_backoff(self, mock_transport):
        """Retryable KubeMQError -> backoff, retry (lines 766-790)."""
        client = _make_connected_client(mock_transport)

        retryable = KubeMQConnectionError("conn lost", is_retryable=True)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = OneShotRetryIterator(retryable, [pb_ev])

        errors = []
        received = []

        async def cb(event):
            received.append(event)

        async def err_cb(error):
            errors.append(error)

        with patch("kubemq.pubsub.async_client.asyncio.sleep", new_callable=AsyncMock):
            await client.subscribe_with_callback(
                EventsSubscription(channel="ch", on_receive_event_callback=lambda e: None),
                cb,
                error_callback=err_cb,
            )

        assert len(received) == 1
        assert len(errors) == 1
        assert isinstance(errors[0], KubeMQStreamBrokenError)

    @pytest.mark.asyncio
    async def test_non_retryable_with_error_callback(self, mock_transport):
        """Non-retryable KubeMQError with error_callback -> called, returns (lines 766-772)."""
        client = _make_connected_client(mock_transport)

        non_retryable = KubeMQError("not retriable", is_retryable=False)
        mock_transport.subscribe_to_events = MagicMock(
            return_value=RaisingAsyncIterator([], non_retryable)
        )

        errors = []

        async def err_cb(error):
            errors.append(error)

        await client.subscribe_with_callback(
            EventsSubscription(channel="ch", on_receive_event_callback=lambda e: None),
            AsyncMock(),
            error_callback=err_cb,
        )

        assert len(errors) == 1

    @pytest.mark.asyncio
    async def test_non_retryable_without_error_callback(self, mock_transport):
        """Non-retryable KubeMQError, no error_callback -> re-raises."""
        client = _make_connected_client(mock_transport)

        non_retryable = KubeMQError("fatal cb", is_retryable=False)
        mock_transport.subscribe_to_events = MagicMock(
            return_value=RaisingAsyncIterator([], non_retryable)
        )

        with pytest.raises(KubeMQError, match="fatal cb"):
            await client.subscribe_with_callback(
                EventsSubscription(channel="ch", on_receive_event_callback=lambda e: None),
                AsyncMock(),
            )

    @pytest.mark.asyncio
    async def test_generic_exception_with_error_callback(self, mock_transport):
        """Generic Exception with error_callback -> called, returns (lines 792-797)."""
        client = _make_connected_client(mock_transport)

        mock_transport.subscribe_to_events = MagicMock(
            return_value=RaisingAsyncIterator([], RuntimeError("generic"))
        )

        errors = []

        async def err_cb(error):
            errors.append(error)

        await client.subscribe_with_callback(
            EventsSubscription(channel="ch", on_receive_event_callback=lambda e: None),
            AsyncMock(),
            error_callback=err_cb,
        )

        assert len(errors) == 1

    @pytest.mark.asyncio
    async def test_generic_exception_without_error_callback(self, mock_transport):
        """Generic Exception, no error_callback -> re-raises."""
        client = _make_connected_client(mock_transport)

        mock_transport.subscribe_to_events = MagicMock(
            return_value=RaisingAsyncIterator([], RuntimeError("gen no cb"))
        )

        with pytest.raises(RuntimeError, match="gen no cb"):
            await client.subscribe_with_callback(
                EventsSubscription(channel="ch", on_receive_event_callback=lambda e: None),
                AsyncMock(),
            )


# ---------------------------------------------------------------------------
# subscribe_store_with_callback: sequential, concurrent, error paths
# ---------------------------------------------------------------------------


class TestSubscribeStoreWithCallbackSequential:
    """Cover lines 852-909: sequential callback path for event store."""

    @pytest.mark.asyncio
    async def test_sequential_handler_error_with_callback(self, mock_transport):
        """Sequential: handler raises -> error_callback (lines 884-893)."""
        from kubemq.core.exceptions import KubeMQHandlerError
        from kubemq.pubsub.events_store_subscription import EventsStoreSubscription

        client = _make_connected_client(mock_transport)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(return_value=AsyncIteratorMock([pb_ev]))

        errors = []

        async def bad_cb(event):
            raise ValueError("store seq boom")

        async def err_cb(error):
            errors.append(error)

        await client.subscribe_store_with_callback(
            EventsStoreSubscription(channel="ch", on_receive_event_callback=lambda e: None),
            bad_cb,
            error_callback=err_cb,
            max_concurrent_callbacks=1,
        )

        assert len(errors) == 1
        assert isinstance(errors[0], KubeMQHandlerError)

    @pytest.mark.asyncio
    async def test_sequential_handler_error_no_callback(self, mock_transport):
        """Sequential: handler raises, no error_callback -> logged (lines 894-895)."""
        from kubemq.pubsub.events_store_subscription import EventsStoreSubscription

        client = _make_connected_client(mock_transport)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(return_value=AsyncIteratorMock([pb_ev]))

        async def bad_cb(event):
            raise ValueError("store seq no cb")

        with patch("kubemq.pubsub.async_client._logger") as mock_logger:
            await client.subscribe_store_with_callback(
                EventsStoreSubscription(channel="ch", on_receive_event_callback=lambda e: None),
                bad_cb,
                max_concurrent_callbacks=1,
            )
            mock_logger.error.assert_called()


class TestSubscribeStoreWithCallbackConcurrentErrors:
    """Cover lines 910-940: concurrent callback errors for event store."""

    @pytest.mark.asyncio
    async def test_concurrent_handler_error_with_callback(self, mock_transport):
        """Concurrent: handler error -> error_callback (lines 916-921)."""
        from kubemq.pubsub.events_store_subscription import EventsStoreSubscription

        client = _make_connected_client(mock_transport)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(return_value=AsyncIteratorMock([pb_ev]))

        errors = []

        async def bad_cb(event):
            raise ValueError("store concurrent boom")

        async def err_cb(error):
            errors.append(error)

        await client.subscribe_store_with_callback(
            EventsStoreSubscription(channel="ch", on_receive_event_callback=lambda e: None),
            bad_cb,
            error_callback=err_cb,
            max_concurrent_callbacks=3,
        )

        await asyncio.sleep(0.05)
        assert len(errors) == 1

    @pytest.mark.asyncio
    async def test_concurrent_handler_error_no_callback(self, mock_transport):
        """Concurrent: handler error, no error_callback -> logger.error (lines 922-927)."""
        from kubemq.pubsub.events_store_subscription import EventsStoreSubscription

        client = _make_connected_client(mock_transport)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(return_value=AsyncIteratorMock([pb_ev]))

        async def bad_cb(event):
            raise ValueError("store concurrent no cb")

        client._logger = MagicMock()

        await client.subscribe_store_with_callback(
            EventsStoreSubscription(channel="ch", on_receive_event_callback=lambda e: None),
            bad_cb,
            max_concurrent_callbacks=3,
        )

        await asyncio.sleep(0.05)
        client._logger.error.assert_called()


class TestSubscribeStoreWithCallbackStreamErrors:
    """Cover lines 944-975: KubeMQError and generic errors in subscribe_store_with_callback."""

    @pytest.mark.asyncio
    async def test_retryable_error_backoff(self, mock_transport):
        """Retryable KubeMQError -> backoff, retry (lines 944-968)."""
        from kubemq.pubsub.events_store_subscription import EventsStoreSubscription

        client = _make_connected_client(mock_transport)

        retryable = KubeMQConnectionError("store conn lost", is_retryable=True)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = OneShotRetryIterator(retryable, [pb_ev])

        errors = []
        received = []

        async def cb(event):
            received.append(event)

        async def err_cb(error):
            errors.append(error)

        with patch("kubemq.pubsub.async_client.asyncio.sleep", new_callable=AsyncMock):
            await client.subscribe_store_with_callback(
                EventsStoreSubscription(channel="ch", on_receive_event_callback=lambda e: None),
                cb,
                error_callback=err_cb,
            )

        assert len(received) == 1
        assert len(errors) == 1
        assert isinstance(errors[0], KubeMQStreamBrokenError)

    @pytest.mark.asyncio
    async def test_non_retryable_with_error_callback(self, mock_transport):
        """Non-retryable with error_callback -> called, returns (lines 944-950)."""
        from kubemq.pubsub.events_store_subscription import EventsStoreSubscription

        client = _make_connected_client(mock_transport)

        non_retryable = KubeMQError("store not retriable", is_retryable=False)
        mock_transport.subscribe_to_events = MagicMock(
            return_value=RaisingAsyncIterator([], non_retryable)
        )

        errors = []

        async def err_cb(error):
            errors.append(error)

        await client.subscribe_store_with_callback(
            EventsStoreSubscription(channel="ch", on_receive_event_callback=lambda e: None),
            AsyncMock(),
            error_callback=err_cb,
        )

        assert len(errors) == 1

    @pytest.mark.asyncio
    async def test_non_retryable_without_callback(self, mock_transport):
        """Non-retryable, no error_callback -> re-raises."""
        from kubemq.pubsub.events_store_subscription import EventsStoreSubscription

        client = _make_connected_client(mock_transport)

        non_retryable = KubeMQError("store fatal", is_retryable=False)
        mock_transport.subscribe_to_events = MagicMock(
            return_value=RaisingAsyncIterator([], non_retryable)
        )

        with pytest.raises(KubeMQError, match="store fatal"):
            await client.subscribe_store_with_callback(
                EventsStoreSubscription(channel="ch", on_receive_event_callback=lambda e: None),
                AsyncMock(),
            )

    @pytest.mark.asyncio
    async def test_generic_exception_with_callback(self, mock_transport):
        """Generic Exception with error_callback -> called, returns (lines 970-975)."""
        from kubemq.pubsub.events_store_subscription import EventsStoreSubscription

        client = _make_connected_client(mock_transport)

        mock_transport.subscribe_to_events = MagicMock(
            return_value=RaisingAsyncIterator([], RuntimeError("store generic"))
        )

        errors = []

        async def err_cb(error):
            errors.append(error)

        await client.subscribe_store_with_callback(
            EventsStoreSubscription(channel="ch", on_receive_event_callback=lambda e: None),
            AsyncMock(),
            error_callback=err_cb,
        )

        assert len(errors) == 1

    @pytest.mark.asyncio
    async def test_generic_exception_without_callback(self, mock_transport):
        """Generic Exception, no error_callback -> re-raises."""
        from kubemq.pubsub.events_store_subscription import EventsStoreSubscription

        client = _make_connected_client(mock_transport)

        mock_transport.subscribe_to_events = MagicMock(
            return_value=RaisingAsyncIterator([], RuntimeError("store gen no cb"))
        )

        with pytest.raises(RuntimeError, match="store gen no cb"):
            await client.subscribe_store_with_callback(
                EventsStoreSubscription(channel="ch", on_receive_event_callback=lambda e: None),
                AsyncMock(),
            )


# ==============================================================================
# Extended Coverage Tests — 95% target
# ==============================================================================


class TestAsyncClientGetEventSenderLazyInit:
    """Tests for _get_event_sender() lazy init (lines 108-115)."""

    @pytest.mark.asyncio
    async def test_get_event_sender_creates_on_first_call(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        assert client._event_sender is None

        with patch("kubemq.pubsub.async_client.AsyncEventSender") as mock_sender_class:
            mock_sender = AsyncMock()
            mock_sender.start = AsyncMock()
            mock_sender_class.return_value = mock_sender

            sender = await client._get_event_sender()

            assert sender is mock_sender
            mock_sender.start.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_event_sender_returns_same_instance(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        mock_sender = AsyncMock()
        client._event_sender = mock_sender

        sender = await client._get_event_sender()
        assert sender is mock_sender


class TestAsyncClientSendEventUnary:
    """Tests for send_event_unary() (lines 173-219)."""

    @pytest.mark.asyncio
    async def test_send_event_unary_success(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        mock_result = MagicMock()
        mock_result.Sent = True
        mock_result.Error = ""
        mock_transport.send_event.return_value = mock_result

        message = EventMessage(channel="ch", body=b"hello")
        await client.send_event_unary(message)

        mock_transport.send_event.assert_called_once()

    @pytest.mark.asyncio
    async def test_send_event_unary_server_error_raises(self, mock_transport):
        from kubemq.core.exceptions import KubeMQError

        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        mock_result = MagicMock()
        mock_result.Sent = False
        mock_result.Error = "channel does not exist"
        mock_transport.send_event.return_value = mock_result

        message = EventMessage(channel="bad-ch", body=b"hello")
        with pytest.raises(KubeMQError, match="channel does not exist"):
            await client.send_event_unary(message)

    @pytest.mark.asyncio
    async def test_send_event_unary_validation_error(self, mock_transport):
        from pydantic import ValidationError as PydanticValidationError

        from kubemq.core.exceptions import KubeMQValidationError

        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        message = EventMessage(channel="ch", body=b"test")
        validation_err = PydanticValidationError.from_exception_data(
            title="EventMessage", line_errors=[]
        )
        with patch.object(EventMessage, "encode", side_effect=validation_err):
            with pytest.raises(KubeMQValidationError) as exc_info:
                await client.send_event_unary(message)
            assert exc_info.value.__cause__ is validation_err

    @pytest.mark.asyncio
    async def test_send_event_unary_transport_error(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        mock_transport.send_event.side_effect = RuntimeError("transport down")

        message = EventMessage(channel="ch", body=b"test")
        with pytest.raises(RuntimeError, match="transport down"):
            await client.send_event_unary(message)

    @pytest.mark.asyncio
    async def test_send_event_unary_span_attributes(self, mock_transport):
        client = _make_connected_client(mock_transport)
        mock_transport.send_event = AsyncMock()

        mock_result = MagicMock()
        mock_result.Sent = True
        mock_result.Error = ""
        mock_transport.send_event.return_value = mock_result

        mock_span = MagicMock()
        mock_span.is_recording.return_value = True
        mock_span.__enter__ = MagicMock(return_value=mock_span)
        mock_span.__exit__ = MagicMock(return_value=False)

        mock_instrumentor = MagicMock()
        mock_instrumentor.start_span.return_value = mock_span
        client._instrumentor = mock_instrumentor

        message = EventMessage(channel="ch", body=b"data")
        await client.send_event_unary(message)

        assert mock_span.set_attribute.call_count == 2


class TestAsyncClientSubscriptionCallbackErrorIsolation:
    """Tests for subscription callback branches: error_callback raises logged (lines 462-463)."""

    @pytest.mark.asyncio
    async def test_handler_error_callback_raises_is_logged(self, mock_transport):
        client = _make_connected_client(mock_transport)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(return_value=AsyncIteratorMock([pb_ev]))

        def bad_handler(event):
            raise ValueError("handler crash")

        async def bad_err_cb(msg):
            raise RuntimeError("error callback crashed")

        sub = EventsSubscription(
            channel="ch",
            on_receive_event_callback=bad_handler,
            on_error_callback=bad_err_cb,
        )

        events = []
        async for ev in client.subscribe_to_events(sub):
            events.append(ev)

        assert len(events) == 1

    @pytest.mark.asyncio
    async def test_subscribe_with_callback_error_callback_raises_is_logged(self, mock_transport):
        client = _make_connected_client(mock_transport)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(return_value=AsyncIteratorMock([pb_ev]))

        async def bad_cb(event):
            raise ValueError("handler boom")

        async def bad_err_cb(error):
            raise RuntimeError("error callback boom")

        with patch("kubemq.pubsub.async_client._logger") as mock_logger:
            await client.subscribe_with_callback(
                EventsSubscription(channel="ch", on_receive_event_callback=lambda e: None),
                bad_cb,
                error_callback=bad_err_cb,
                max_concurrent_callbacks=1,
            )
            mock_logger.exception.assert_called()


class TestAsyncClientSubscribeToEventsStoreResumeSequence:
    """Tests for subscribe_to_events_store resume from sequence (lines 565-570)."""

    @pytest.mark.asyncio
    async def test_store_subscription_resumes_from_sequence(self, mock_transport):
        from kubemq.pubsub.events_store_subscription import EventsStoreSubscription

        client = _make_connected_client(mock_transport)

        pb_ev1 = MagicMock()
        pb_ev1.EventID = "ev-1"
        pb_ev1.Channel = "store-ch"
        pb_ev1.Metadata = ""
        pb_ev1.Body = b"data1"
        pb_ev1.Timestamp = 1234567890
        pb_ev1.Sequence = 42
        pb_ev1.Tags = {}

        retryable = KubeMQConnectionError("lost", is_retryable=True)

        pb_ev2 = MagicMock()
        pb_ev2.EventID = "ev-2"
        pb_ev2.Channel = "store-ch"
        pb_ev2.Metadata = ""
        pb_ev2.Body = b"data2"
        pb_ev2.Timestamp = 1234567891
        pb_ev2.Sequence = 43
        pb_ev2.Tags = {}

        call_count = [0]
        captured_requests = []

        def mock_subscribe(request, token=None):
            call_count[0] += 1
            captured_requests.append(request)
            if call_count[0] == 1:
                return RaisingAsyncIterator([pb_ev1], retryable)
            return AsyncIteratorMock([pb_ev2])

        mock_transport.subscribe_to_events = mock_subscribe

        errors = []

        def err_cb(msg):
            errors.append(msg)

        sub = EventsStoreSubscription(
            channel="store-ch",
            on_receive_event_callback=lambda e: None,
            on_error_callback=err_cb,
        )

        events = []
        with patch("kubemq.pubsub.async_client.asyncio.sleep", new_callable=AsyncMock):
            async for ev in client.subscribe_to_events_store(sub):
                events.append(ev)

        assert len(events) == 2
        assert events[0].sequence == 42
        assert events[1].sequence == 43
        assert call_count[0] == 2


class TestAsyncClientSubscribeStoreWithCallbackResumeSequence:
    """Tests for subscribe_store_with_callback resume from sequence (lines 929-934)."""

    @pytest.mark.asyncio
    async def test_store_callback_resumes_from_sequence(self, mock_transport):
        from kubemq.pubsub.events_store_subscription import EventsStoreSubscription

        client = _make_connected_client(mock_transport)

        pb_ev1 = MagicMock()
        pb_ev1.EventID = "ev-1"
        pb_ev1.Channel = "store-ch"
        pb_ev1.Metadata = ""
        pb_ev1.Body = b"data1"
        pb_ev1.Timestamp = 1234567890
        pb_ev1.Sequence = 10
        pb_ev1.Tags = {}

        retryable = KubeMQConnectionError("lost", is_retryable=True)

        pb_ev2 = MagicMock()
        pb_ev2.EventID = "ev-2"
        pb_ev2.Channel = "store-ch"
        pb_ev2.Metadata = ""
        pb_ev2.Body = b"data2"
        pb_ev2.Timestamp = 1234567891
        pb_ev2.Sequence = 11
        pb_ev2.Tags = {}

        call_count = [0]

        def mock_subscribe(request, token=None):
            call_count[0] += 1
            if call_count[0] == 1:
                return RaisingAsyncIterator([pb_ev1], retryable)
            return AsyncIteratorMock([pb_ev2])

        mock_transport.subscribe_to_events = mock_subscribe

        received = []
        errors = []

        async def cb(event):
            received.append(event)

        async def err_cb(error):
            errors.append(error)

        with patch("kubemq.pubsub.async_client.asyncio.sleep", new_callable=AsyncMock):
            await client.subscribe_store_with_callback(
                EventsStoreSubscription(
                    channel="store-ch",
                    on_receive_event_callback=lambda e: None,
                ),
                cb,
                error_callback=err_cb,
            )

        assert len(received) == 2
        assert call_count[0] == 2


class TestAsyncClientCloseWithEventSender:
    """Tests for close() with active event sender."""

    @pytest.mark.asyncio
    async def test_close_closes_event_sender(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        mock_sender = AsyncMock()
        mock_sender.close = AsyncMock()
        client._event_sender = mock_sender

        with patch.object(type(client).__bases__[0], "close", new_callable=AsyncMock):
            await client.close()

        mock_sender.close.assert_called_once()
        assert client._event_sender is None


class TestAsyncClientSubscribeWithCallbackErrorCallbackRaisesStore:
    """Test error_callback itself raising in store with callback (lines 968-969)."""

    @pytest.mark.asyncio
    async def test_store_callback_error_callback_raises_is_logged(self, mock_transport):
        from kubemq.pubsub.events_store_subscription import EventsStoreSubscription

        client = _make_connected_client(mock_transport)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(return_value=AsyncIteratorMock([pb_ev]))

        async def bad_cb(event):
            raise ValueError("store handler boom")

        async def bad_err_cb(error):
            raise RuntimeError("error callback boom")

        with patch("kubemq.pubsub.async_client._logger") as mock_logger:
            await client.subscribe_store_with_callback(
                EventsStoreSubscription(channel="ch", on_receive_event_callback=lambda e: None),
                bad_cb,
                error_callback=bad_err_cb,
                max_concurrent_callbacks=1,
            )
            mock_logger.exception.assert_called()


class TestAsyncClientPublishEventStoreNoneResult:
    """Test publish_event_store returns empty result on None."""

    @pytest.mark.asyncio
    async def test_publish_event_store_none_result(self, mock_transport):
        client = _make_connected_client(mock_transport)
        client._event_sender.send = AsyncMock(return_value=None)

        msg = EventStoreMessage(channel="ch", body=b"x")
        result = await client.publish_event_store(msg)

        assert result.sent is False


# ==============================================================================
# Additional Targeted Coverage Tests — 95% target
# ==============================================================================

from kubemq.pubsub.event_message_received import EventMessageReceived  # noqa: E402
from kubemq.pubsub.event_store_message_received import EventStoreMessageReceived  # noqa: E402
from kubemq.pubsub.events_store_subscription import EventsStoreSubscription  # noqa: E402


class TestSubscribeToEventsProcessingError:
    """Cover lines 470-475: processing error in subscribe_to_events."""

    @pytest.mark.asyncio
    async def test_decode_error_propagates(self, mock_transport):
        """EventMessageReceived.decode raising -> proc_err path."""
        client = _make_connected_client(mock_transport)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(return_value=AsyncIteratorMock([pb_ev]))

        sub = EventsSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
        )

        with patch.object(
            EventMessageReceived, "decode", side_effect=RuntimeError("decode failed")
        ):
            with pytest.raises(RuntimeError, match="decode failed"):
                async for _ in client.subscribe_to_events(sub):
                    pass


class TestSubscribeToEventsStoreErrorCallbackRaises:
    """Cover lines 613-614: error callback itself raises in store subscribe."""

    @pytest.mark.asyncio
    async def test_store_error_callback_raises_is_logged(self, mock_transport):
        """error_callback raises in store async-iter subscribe -> logged."""
        client = _make_connected_client(mock_transport)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(return_value=AsyncIteratorMock([pb_ev]))

        def bad_handler(event):
            raise ValueError("handler crash")

        async def bad_err_cb(msg):
            raise RuntimeError("error callback crashed")

        sub = EventsStoreSubscription(
            channel="ch",
            on_receive_event_callback=bad_handler,
            on_error_callback=bad_err_cb,
        )

        events = []
        async for ev in client.subscribe_to_events_store(sub):
            events.append(ev)

        assert len(events) == 1


class TestSubscribeToEventsStoreProcessingError:
    """Cover lines 621-626: processing error in subscribe_to_events_store."""

    @pytest.mark.asyncio
    async def test_store_decode_error_propagates(self, mock_transport):
        """EventStoreMessageReceived.decode raising -> proc_err path."""
        client = _make_connected_client(mock_transport)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(return_value=AsyncIteratorMock([pb_ev]))

        sub = EventsStoreSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
        )

        with patch.object(
            EventStoreMessageReceived, "decode", side_effect=RuntimeError("store decode")
        ):
            with pytest.raises(RuntimeError, match="store decode"):
                async for _ in client.subscribe_to_events_store(sub):
                    pass


class TestSubscribeToEventsStoreNonRetryableAsyncCallback:
    """Cover line 642: non-retryable error with async on_error_callback in store."""

    @pytest.mark.asyncio
    async def test_non_retryable_async_callback(self, mock_transport):
        client = _make_connected_client(mock_transport)
        non_retryable = KubeMQError("fatal store", is_retryable=False)
        mock_transport.subscribe_to_events = MagicMock(
            return_value=RaisingAsyncIterator([], non_retryable)
        )

        errors = []

        async def async_err_cb(msg):
            errors.append(msg)

        sub = EventsStoreSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
            on_error_callback=async_err_cb,
        )

        with pytest.raises(KubeMQError, match="fatal store"):
            async for _ in client.subscribe_to_events_store(sub):
                pass

        assert len(errors) == 1


class TestSubscribeToEventsStoreGenericExceptionAsyncCallback:
    """Cover line 673: generic exception with async on_error_callback in store."""

    @pytest.mark.asyncio
    async def test_generic_exception_async_callback(self, mock_transport):
        client = _make_connected_client(mock_transport)
        mock_transport.subscribe_to_events = MagicMock(
            return_value=RaisingAsyncIterator([], RuntimeError("store async err"))
        )

        errors = []

        async def async_err_cb(msg):
            errors.append(msg)

        sub = EventsStoreSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
            on_error_callback=async_err_cb,
        )

        with pytest.raises(RuntimeError, match="store async err"):
            async for _ in client.subscribe_to_events_store(sub):
                pass

        assert len(errors) == 1


class TestSubscribeToEventsRetryableAsyncCallback:
    """Cover line 505: retryable error with async on_error_callback in events."""

    @pytest.mark.asyncio
    async def test_retryable_async_callback(self, mock_transport):
        client = _make_connected_client(mock_transport)
        retryable = KubeMQConnectionError("events lost", is_retryable=True)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = OneShotRetryIterator(retryable, [pb_ev])

        errors = []

        async def async_err_cb(msg):
            errors.append(msg)

        sub = EventsSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
            on_error_callback=async_err_cb,
        )

        events = []
        with patch("kubemq.pubsub.async_client.asyncio.sleep", new_callable=AsyncMock):
            async for ev in client.subscribe_to_events(sub):
                events.append(ev)

        assert len(events) == 1
        assert len(errors) == 1
        assert "Stream broken" in errors[0]


class TestSubscribeToEventsNonRetryableAsyncCallback:
    """Cover line 491: non-retryable error with async on_error_callback in events."""

    @pytest.mark.asyncio
    async def test_non_retryable_async_callback(self, mock_transport):
        client = _make_connected_client(mock_transport)
        non_retryable = KubeMQError("fatal events", is_retryable=False)
        mock_transport.subscribe_to_events = MagicMock(
            return_value=RaisingAsyncIterator([], non_retryable)
        )

        errors = []

        async def async_err_cb(msg):
            errors.append(msg)

        sub = EventsSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
            on_error_callback=async_err_cb,
        )

        with pytest.raises(KubeMQError, match="fatal events"):
            async for _ in client.subscribe_to_events(sub):
                pass

        assert len(errors) == 1


class TestSubscribeToEventsStoreRetryableAsyncCallback:
    """Cover line 656: retryable error with async on_error_callback in store."""

    @pytest.mark.asyncio
    async def test_retryable_async_callback(self, mock_transport):
        client = _make_connected_client(mock_transport)
        retryable = KubeMQConnectionError("store lost", is_retryable=True)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = OneShotRetryIterator(retryable, [pb_ev])

        errors = []

        async def async_err_cb(msg):
            errors.append(msg)

        sub = EventsStoreSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
            on_error_callback=async_err_cb,
        )

        events = []
        with patch("kubemq.pubsub.async_client.asyncio.sleep", new_callable=AsyncMock):
            async for ev in client.subscribe_to_events_store(sub):
                events.append(ev)

        assert len(events) == 1
        assert len(errors) == 1
        assert "Stream broken" in errors[0]


class TestSubscribeWithCallbackProcessingError:
    """Cover lines 786-791: processing error in subscribe_with_callback."""

    @pytest.mark.asyncio
    async def test_decode_error_propagates(self, mock_transport):
        """EventMessageReceived.decode raising -> proc_err re-raised."""
        client = _make_connected_client(mock_transport)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(return_value=AsyncIteratorMock([pb_ev]))

        async def callback(event):
            pass

        with patch.object(
            EventMessageReceived, "decode", side_effect=RuntimeError("cb decode fail")
        ):
            with pytest.raises(RuntimeError, match="cb decode fail"):
                await client.subscribe_with_callback(
                    EventsSubscription(channel="ch", on_receive_event_callback=lambda e: None),
                    callback,
                )


class TestSubscribeWithCallbackRetryableNoErrorCb:
    """Cover line 846 false branch: retryable error without error_callback."""

    @pytest.mark.asyncio
    async def test_retryable_no_callback_retries(self, mock_transport):
        """Retryable KubeMQError without error_callback still retries."""
        client = _make_connected_client(mock_transport)
        retryable = KubeMQConnectionError("conn lost", is_retryable=True)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = OneShotRetryIterator(retryable, [pb_ev])

        received = []

        async def cb(event):
            received.append(event)

        with patch("kubemq.pubsub.async_client.asyncio.sleep", new_callable=AsyncMock):
            await client.subscribe_with_callback(
                EventsSubscription(channel="ch", on_receive_event_callback=lambda e: None),
                cb,
            )

        assert len(received) == 1


class TestSubscribeStoreWithCallbackProcessingError:
    """Cover lines 975-980: processing error in subscribe_store_with_callback."""

    @pytest.mark.asyncio
    async def test_store_decode_error_propagates(self, mock_transport):
        """EventStoreMessageReceived.decode raising -> proc_err re-raised."""
        client = _make_connected_client(mock_transport)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(return_value=AsyncIteratorMock([pb_ev]))

        async def callback(event):
            pass

        with patch.object(
            EventStoreMessageReceived, "decode", side_effect=RuntimeError("store cb decode")
        ):
            with pytest.raises(RuntimeError, match="store cb decode"):
                await client.subscribe_store_with_callback(
                    EventsStoreSubscription(channel="ch", on_receive_event_callback=lambda e: None),
                    callback,
                )


class TestSubscribeStoreWithCallbackRetryableNoErrorCb:
    """Cover line 1036 false branch: retryable error without error_callback."""

    @pytest.mark.asyncio
    async def test_store_retryable_no_callback_retries(self, mock_transport):
        """Retryable KubeMQError without error_callback still retries."""
        client = _make_connected_client(mock_transport)
        retryable = KubeMQConnectionError("store lost", is_retryable=True)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = OneShotRetryIterator(retryable, [pb_ev])

        received = []

        async def cb(event):
            received.append(event)

        with patch("kubemq.pubsub.async_client.asyncio.sleep", new_callable=AsyncMock):
            await client.subscribe_store_with_callback(
                EventsStoreSubscription(channel="ch", on_receive_event_callback=lambda e: None),
                cb,
            )

        assert len(received) == 1


class TestSubscribeStoreWithCallbackConcurrentErrorCallbackRaises:
    """Cover lines 996-997: concurrent store callback error_callback raises."""

    @pytest.mark.asyncio
    async def test_concurrent_store_error_callback_raises(self, mock_transport):
        """error_callback raises in concurrent store path -> silently caught."""
        client = _make_connected_client(mock_transport)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(return_value=AsyncIteratorMock([pb_ev]))

        async def bad_cb(event):
            raise ValueError("concurrent store err")

        async def bad_err_cb(error):
            raise RuntimeError("err cb exploded")

        await client.subscribe_store_with_callback(
            EventsStoreSubscription(channel="ch", on_receive_event_callback=lambda e: None),
            bad_cb,
            error_callback=bad_err_cb,
            max_concurrent_callbacks=3,
        )

        await asyncio.sleep(0.05)
