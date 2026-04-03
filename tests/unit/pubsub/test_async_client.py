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
        """Test send_event_store returns EventStoreResult via bidi stream sender."""
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

        token = AsyncCancellationToken()

        # Make the transport return an async iterator that cancels the token when exhausted
        mock_transport.subscribe_to_events = MagicMock(
            return_value=CancellingAsyncIteratorMock([], token)
        )

        subscription = EventsSubscription(
            channel="test",
            on_receive_event_callback=lambda e: None,
        )

        # Should not raise
        async for _ in client.subscribe_to_events(subscription, cancellation_token=token):
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


class CancellingAsyncIteratorMock:
    """Mock async iterator that cancels a token after yielding all items."""

    def __init__(self, items, token: AsyncCancellationToken):
        self.items = iter(items)
        self.token = token

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self.items)
        except StopIteration:
            self.token.cancel()
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
        from kubemq.pubsub.events_store_subscription import (
            EventsStoreSubscription,
            EventStoreStartPosition,
        )

        client = AsyncClient(address="localhost:50000")
        subscription = EventsStoreSubscription(
            channel="test",
            on_receive_event_callback=lambda e: None,
            events_store_type=EventStoreStartPosition.StartFromNew,
        )

        with pytest.raises(KubeMQConnectionError):
            async for _ in client.subscribe_to_events_store(subscription):
                pass

    @pytest.mark.asyncio
    async def test_subscribe_to_events_store_yields_messages(self, mock_transport):
        """Test subscribe_to_events_store yields event store messages."""
        from kubemq.pubsub.events_store_subscription import (
            EventsStoreSubscription,
            EventStoreStartPosition,
        )

        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        token = AsyncCancellationToken()

        # Create mock protobuf events
        mock_pb_event = MagicMock()
        mock_pb_event.EventID = "event-123"
        mock_pb_event.Channel = "test-channel"
        mock_pb_event.Metadata = "test-metadata"
        mock_pb_event.Body = b"test-body"
        mock_pb_event.Timestamp = 1234567890
        mock_pb_event.Sequence = 1
        mock_pb_event.Tags = {}

        # Make the transport return an async iterator that cancels the token when done
        mock_transport.subscribe_to_events = MagicMock(
            return_value=CancellingAsyncIteratorMock([mock_pb_event], token)
        )

        subscription = EventsStoreSubscription(
            channel="test",
            on_receive_event_callback=lambda e: None,
            events_store_type=EventStoreStartPosition.StartFromNew,
        )

        events = []
        async for event in client.subscribe_to_events_store(subscription, cancellation_token=token):
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

        token = AsyncCancellationToken()

        # Create mock protobuf event
        mock_pb_event = MagicMock()
        mock_pb_event.EventID = "event-123"
        mock_pb_event.Channel = "test-channel"
        mock_pb_event.Metadata = ""
        mock_pb_event.Body = b"test-body"
        mock_pb_event.Tags = {}

        mock_transport.subscribe_to_events = MagicMock(
            return_value=CancellingAsyncIteratorMock([mock_pb_event], token)
        )

        subscription = EventsSubscription(
            channel="test",
            on_receive_event_callback=lambda e: None,
        )

        received_events = []

        async def callback(event):
            received_events.append(event)

        await client.subscribe_with_callback(subscription, callback, cancellation_token=token)

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
        from kubemq.pubsub.events_store_subscription import (
            EventsStoreSubscription,
            EventStoreStartPosition,
        )

        client = AsyncClient(address="localhost:50000")
        subscription = EventsStoreSubscription(
            channel="test",
            on_receive_event_callback=lambda e: None,
            events_store_type=EventStoreStartPosition.StartFromNew,
        )

        async def callback(event):
            pass

        with pytest.raises(KubeMQConnectionError):
            await client.subscribe_store_with_callback(subscription, callback)

    @pytest.mark.asyncio
    async def test_subscribe_store_with_callback_calls_callback(self, mock_transport):
        """Test subscribe_store_with_callback calls callback for each event."""
        from kubemq.pubsub.events_store_subscription import (
            EventsStoreSubscription,
            EventStoreStartPosition,
        )

        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        token = AsyncCancellationToken()

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
            return_value=CancellingAsyncIteratorMock([mock_pb_event], token)
        )

        subscription = EventsStoreSubscription(
            channel="test",
            on_receive_event_callback=lambda e: None,
            events_store_type=EventStoreStartPosition.StartFromNew,
        )

        received_events = []

        async def callback(event):
            received_events.append(event)

        await client.subscribe_store_with_callback(subscription, callback, cancellation_token=token)

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

        token = AsyncCancellationToken()

        # Create mock protobuf event
        mock_pb_event = MagicMock()
        mock_pb_event.EventID = "event-123"
        mock_pb_event.Channel = "test-channel"
        mock_pb_event.Metadata = ""
        mock_pb_event.Body = b"test-body"
        mock_pb_event.Tags = {}

        mock_transport.subscribe_to_events = MagicMock(
            return_value=CancellingAsyncIteratorMock([mock_pb_event], token)
        )

        received_events = []

        def sync_callback(event):
            received_events.append(event)

        subscription = EventsSubscription(
            channel="test",
            on_receive_event_callback=sync_callback,
        )

        async for _ in client.subscribe_to_events(subscription, cancellation_token=token):
            pass

        assert len(received_events) == 1

    @pytest.mark.asyncio
    async def test_subscribe_to_events_calls_async_callback(self, mock_transport):
        """Test subscribe_to_events calls async callback."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        token = AsyncCancellationToken()

        # Create mock protobuf event
        mock_pb_event = MagicMock()
        mock_pb_event.EventID = "event-123"
        mock_pb_event.Channel = "test-channel"
        mock_pb_event.Metadata = ""
        mock_pb_event.Body = b"test-body"
        mock_pb_event.Tags = {}

        mock_transport.subscribe_to_events = MagicMock(
            return_value=CancellingAsyncIteratorMock([mock_pb_event], token)
        )

        received_events = []

        async def async_callback(event):
            received_events.append(event)

        subscription = EventsSubscription(
            channel="test",
            on_receive_event_callback=async_callback,
        )

        async for _ in client.subscribe_to_events(subscription, cancellation_token=token):
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
    """Tests for publish_event / send_event_store telemetry paths."""

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
    async def test_send_event_store_returns_result(self, mock_transport):
        """Verify send_event_store returns proper EventStoreResult."""
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

        result = await client.send_event_store(message)

        assert result.sent is True
        assert result.id == "es-456"


class TestAsyncClientSubscribeToEventsYield:
    """Tests for subscribe_to_events / subscribe_to_events_store yielding messages."""

    @pytest.mark.asyncio
    async def test_subscribe_to_events_yields_messages(self, mock_transport):
        """Verify subscribe_to_events yields EventReceived from pb events."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        token = AsyncCancellationToken()

        mock_pb_event = MagicMock()
        mock_pb_event.EventID = "event-123"
        mock_pb_event.Channel = "test-channel"
        mock_pb_event.Metadata = "some-meta"
        mock_pb_event.Body = b"test-body"
        mock_pb_event.Tags = {"key": "value"}

        mock_transport.subscribe_to_events = MagicMock(
            return_value=CancellingAsyncIteratorMock([mock_pb_event], token)
        )

        subscription = EventsSubscription(
            channel="test-channel",
            on_receive_event_callback=lambda e: None,
        )

        events = []
        async for event in client.subscribe_to_events(subscription, cancellation_token=token):
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
        from kubemq.pubsub.events_store_subscription import (
            EventsStoreSubscription,
            EventStoreStartPosition,
        )

        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        token = AsyncCancellationToken()

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
            return_value=CancellingAsyncIteratorMock([mock_pb_event], token)
        )

        subscription = EventsStoreSubscription(
            channel="store-channel",
            on_receive_event_callback=async_cb,
            events_store_type=EventStoreStartPosition.StartFromNew,
        )

        events = []
        async for event in client.subscribe_to_events_store(subscription, cancellation_token=token):
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

        token = AsyncCancellationToken()

        pb_events = []
        for i in range(5):
            ev = MagicMock()
            ev.EventID = f"ev-{i}"
            ev.Channel = "ch"
            ev.Metadata = ""
            ev.Body = f"body-{i}".encode()
            ev.Tags = {}
            pb_events.append(ev)

        mock_transport.subscribe_to_events = MagicMock(
            return_value=CancellingAsyncIteratorMock(pb_events, token)
        )

        received = []

        async def callback(event):
            received.append(event.id)

        await client.subscribe_with_callback(
            EventsSubscription(channel="ch", on_receive_event_callback=lambda e: None),
            callback,
            cancellation_token=token,
            max_concurrent_callbacks=3,
        )

        assert sorted(received) == [f"ev-{i}" for i in range(5)]

    @pytest.mark.asyncio
    async def test_subscribe_store_with_callback_concurrent(self, mock_transport):
        """Concurrent path for event store: max_concurrent_callbacks=3, deliver 5 events."""
        from kubemq.pubsub.events_store_subscription import (
            EventsStoreSubscription,
            EventStoreStartPosition,
        )

        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        token = AsyncCancellationToken()

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

        mock_transport.subscribe_to_events = MagicMock(
            return_value=CancellingAsyncIteratorMock(pb_events, token)
        )

        received = []

        async def callback(event):
            received.append(event.id)

        await client.subscribe_store_with_callback(
            EventsStoreSubscription(channel="store-ch", on_receive_event_callback=lambda e: None, events_store_type=EventStoreStartPosition.StartFromNew),
            callback,
            cancellation_token=token,
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
        from kubemq.pubsub.events_store_subscription import (
            EventsStoreSubscription,
            EventStoreStartPosition,
        )

        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        with pytest.raises(ValueError, match="must be >= 1"):
            await client.subscribe_store_with_callback(
                EventsStoreSubscription(channel="ch", on_receive_event_callback=lambda e: None, events_store_type=EventStoreStartPosition.StartFromNew),
                AsyncMock(),
                max_concurrent_callbacks=0,
            )

    @pytest.mark.asyncio
    async def test_subscribe_store_with_callback_rejects_over_1000(self, mock_transport):
        """ValueError when max_concurrent_callbacks=1001 for store subscription."""
        from kubemq.pubsub.events_store_subscription import (
            EventsStoreSubscription,
            EventStoreStartPosition,
        )

        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        with pytest.raises(ValueError, match="must be <= 1000"):
            await client.subscribe_store_with_callback(
                EventsStoreSubscription(channel="ch", on_receive_event_callback=lambda e: None, events_store_type=EventStoreStartPosition.StartFromNew),
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

        token = AsyncCancellationToken()

        mock_pb_event = MagicMock()
        mock_pb_event.EventID = "err-1"
        mock_pb_event.Channel = "ch"
        mock_pb_event.Metadata = ""
        mock_pb_event.Body = b"data"
        mock_pb_event.Tags = {}

        mock_transport.subscribe_to_events = MagicMock(
            return_value=CancellingAsyncIteratorMock([mock_pb_event], token)
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
            cancellation_token=token,
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
        from kubemq.pubsub.events_store_subscription import (
            EventsStoreSubscription,
            EventStoreStartPosition,
        )

        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        token = AsyncCancellationToken()

        mock_pb_event = MagicMock()
        mock_pb_event.EventID = "err-2"
        mock_pb_event.Channel = "store-ch"
        mock_pb_event.Metadata = ""
        mock_pb_event.Body = b"store-data"
        mock_pb_event.Timestamp = 1234567890
        mock_pb_event.Sequence = 1
        mock_pb_event.Tags = {}

        mock_transport.subscribe_to_events = MagicMock(
            return_value=CancellingAsyncIteratorMock([mock_pb_event], token)
        )

        errors = []

        async def bad_callback(event):
            raise ValueError("store handler boom")

        async def err_callback(error):
            errors.append(error)

        await client.subscribe_store_with_callback(
            EventsStoreSubscription(channel="store-ch", on_receive_event_callback=lambda e: None, events_store_type=EventStoreStartPosition.StartFromNew),
            bad_callback,
            error_callback=err_callback,
            cancellation_token=token,
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


class CancellingOneShotRetryIterator:
    """Like OneShotRetryIterator but cancels a token after second iteration.

    First call raises a retryable error; second call yields items then
    cancels the token so the retry loop exits cleanly.
    """

    def __init__(self, exc, items, token: AsyncCancellationToken):
        self._exc = exc
        self._items = items
        self._token = token
        self._call_count = 0

    def __call__(self, *args, **kwargs):
        self._call_count += 1
        if self._call_count == 1:
            return RaisingAsyncIterator([], self._exc)
        return CancellingAsyncIteratorMock(self._items, self._token)


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
# publish_event / send_event_store: ValidationError, generic exception,
# telemetry span, and metrics recording
# ---------------------------------------------------------------------------


class TestPublishEventErrorPaths:
    """Cover lines 133-153, 190-207: ValidationError, generic exception,
    span recording, and metrics calls."""

    @pytest.mark.asyncio
    async def test_publish_event_validation_error(self, mock_transport):
        """ValueError from encode -> KubeMQValidationError."""
        from kubemq.core.exceptions import KubeMQValidationError

        client = _make_connected_client(mock_transport)

        msg = EventMessage(channel="ch", body=b"x")
        with patch.object(
            EventMessage,
            "encode",
            side_effect=ValueError("EventMessage validation failed"),
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
    async def test_send_event_store_validation_error(self, mock_transport):
        """ValueError from encode -> KubeMQValidationError."""
        from kubemq.core.exceptions import KubeMQValidationError

        client = _make_connected_client(mock_transport)

        msg = EventStoreMessage(channel="ch", body=b"x")
        with patch.object(
            EventStoreMessage,
            "encode",
            side_effect=ValueError("EventStoreMessage validation failed"),
        ):
            with pytest.raises(KubeMQValidationError):
                await client.send_event_store(msg)

    @pytest.mark.asyncio
    async def test_send_event_store_generic_exception(self, mock_transport):
        """Generic exception from event sender -> re-raised."""
        client = _make_connected_client(mock_transport)
        client._event_sender.send = AsyncMock(side_effect=RuntimeError("store boom"))

        msg = EventStoreMessage(channel="ch", body=b"x")
        with pytest.raises(RuntimeError, match="store boom"):
            await client.send_event_store(msg)

    @pytest.mark.asyncio
    async def test_send_event_store_span_attributes(self, mock_transport):
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
        result = await client.send_event_store(msg)

        assert mock_span.set_attribute.call_count == 2
        assert result.sent is True


# ---------------------------------------------------------------------------
# send_events_batch: partial failure
# ---------------------------------------------------------------------------


class TestSendEventsBatchFailure:
    """Cover lines 258-259: bounded_send exception path."""

    @pytest.mark.asyncio
    async def test_send_events_batch_partial_failure(self, mock_transport):
        """Some sends fail -> EventStoreResult with sent=False."""
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
        token = AsyncCancellationToken()
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(
            return_value=CancellingAsyncIteratorMock([pb_ev], token)
        )

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
        async for ev in client.subscribe_to_events(sub, cancellation_token=token):
            events.append(ev)

        assert len(events) == 1
        assert len(errors) == 1
        assert "handler crash" in errors[0]

    @pytest.mark.asyncio
    async def test_handler_error_without_error_callback(self, mock_transport):
        """Handler raises, no error_callback -> logged (lines 409-410)."""
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(
            return_value=CancellingAsyncIteratorMock([pb_ev], token)
        )

        def bad_handler(event):
            raise ValueError("handler crash")

        sub = EventsSubscription(
            channel="ch",
            on_receive_event_callback=bad_handler,
        )

        events = []
        async for ev in client.subscribe_to_events(sub, cancellation_token=token):
            events.append(ev)

        assert len(events) == 1

    @pytest.mark.asyncio
    async def test_handler_error_async_error_callback(self, mock_transport):
        """Handler raises -> async on_error_callback called."""
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(
            return_value=CancellingAsyncIteratorMock([pb_ev], token)
        )

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

        async for _ in client.subscribe_to_events(sub, cancellation_token=token):
            pass

        assert len(errors) == 1
        assert "async err test" in errors[0]


class TestSubscribeToEventsStreamErrors:
    """Cover lines 431-470: retryable, non-retryable, generic errors."""

    @pytest.mark.asyncio
    async def test_retryable_error_backoff_and_retry(self, mock_transport):
        """Retryable KubeMQConnectionError -> backoff, retry, stream reconnect (lines 431-461)."""
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()

        retryable_err = KubeMQConnectionError("connection lost", is_retryable=True)
        pb_ev = _make_pb_event()

        mock_transport.subscribe_to_events = CancellingOneShotRetryIterator(retryable_err, [pb_ev], token)

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
            async for ev in client.subscribe_to_events(sub, cancellation_token=token):
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
        from kubemq.pubsub.events_store_subscription import (
            EventsStoreSubscription,
            EventStoreStartPosition,
        )

        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(
            return_value=CancellingAsyncIteratorMock([pb_ev], token)
        )

        errors = []

        def bad_handler(event):
            raise ValueError("store handler err")

        def err_cb(msg):
            errors.append(msg)

        sub = EventsStoreSubscription(
            channel="ch",
            on_receive_event_callback=bad_handler,
            events_store_type=EventStoreStartPosition.StartFromNew,
            on_error_callback=err_cb,
        )

        events = []
        async for ev in client.subscribe_to_events_store(sub, cancellation_token=token):
            events.append(ev)

        assert len(events) == 1
        assert len(errors) == 1
        assert "store handler err" in errors[0]

    @pytest.mark.asyncio
    async def test_handler_error_without_callback(self, mock_transport):
        """Handler raises, no error_callback -> logged (line 550)."""
        from kubemq.pubsub.events_store_subscription import (
            EventsStoreSubscription,
            EventStoreStartPosition,
        )

        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(
            return_value=CancellingAsyncIteratorMock([pb_ev], token)
        )

        def bad_handler(event):
            raise ValueError("no cb test")

        sub = EventsStoreSubscription(
            channel="ch",
            on_receive_event_callback=bad_handler,
            events_store_type=EventStoreStartPosition.StartFromNew,
        )

        events = []
        async for ev in client.subscribe_to_events_store(sub, cancellation_token=token):
            events.append(ev)

        assert len(events) == 1

    @pytest.mark.asyncio
    async def test_retryable_error_backoff(self, mock_transport):
        """Retryable error -> backoff and retry (lines 571-601)."""
        from kubemq.pubsub.events_store_subscription import (
            EventsStoreSubscription,
            EventStoreStartPosition,
        )

        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()

        retryable_err = KubeMQConnectionError("lost", is_retryable=True)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = CancellingOneShotRetryIterator(retryable_err, [pb_ev], token)

        errors = []

        def err_cb(msg):
            errors.append(msg)

        sub = EventsStoreSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
            events_store_type=EventStoreStartPosition.StartFromNew,
            on_error_callback=err_cb,
        )

        events = []
        with patch("kubemq.pubsub.async_client.asyncio.sleep", new_callable=AsyncMock):
            async for ev in client.subscribe_to_events_store(sub, cancellation_token=token):
                events.append(ev)

        assert len(events) == 1
        assert len(errors) == 1
        assert "Stream broken" in errors[0]

    @pytest.mark.asyncio
    async def test_non_retryable_error_with_callback(self, mock_transport):
        """Non-retryable with callback (lines 571-579)."""
        from kubemq.pubsub.events_store_subscription import (
            EventsStoreSubscription,
            EventStoreStartPosition,
        )

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
            events_store_type=EventStoreStartPosition.StartFromNew,
            on_error_callback=err_cb,
        )

        with pytest.raises(KubeMQError, match="store fatal"):
            async for _ in client.subscribe_to_events_store(sub):
                pass

        assert len(errors) == 1

    @pytest.mark.asyncio
    async def test_generic_exception_with_callback(self, mock_transport):
        """Generic Exception with callback (lines 603-610)."""
        from kubemq.pubsub.events_store_subscription import (
            EventsStoreSubscription,
            EventStoreStartPosition,
        )

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
            events_store_type=EventStoreStartPosition.StartFromNew,
            on_error_callback=err_cb,
        )

        with pytest.raises(RuntimeError, match="store unexpected"):
            async for _ in client.subscribe_to_events_store(sub):
                pass

        assert len(errors) == 1

    @pytest.mark.asyncio
    async def test_generic_exception_without_callback(self, mock_transport):
        """Generic Exception, no callback -> re-raises."""
        from kubemq.pubsub.events_store_subscription import (
            EventsStoreSubscription,
            EventStoreStartPosition,
        )

        client = _make_connected_client(mock_transport)

        mock_transport.subscribe_to_events = MagicMock(
            return_value=RaisingAsyncIterator([], RuntimeError("store no cb"))
        )

        sub = EventsStoreSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
            events_store_type=EventStoreStartPosition.StartFromNew,
        )

        with pytest.raises(RuntimeError, match="store no cb"):
            async for _ in client.subscribe_to_events_store(sub):
                pass

    @pytest.mark.asyncio
    async def test_handler_error_with_async_error_callback(self, mock_transport):
        """Async handler raises with async error callback."""
        from kubemq.pubsub.events_store_subscription import (
            EventsStoreSubscription,
            EventStoreStartPosition,
        )

        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(
            return_value=CancellingAsyncIteratorMock([pb_ev], token)
        )

        errors = []

        async def bad_handler(event):
            raise ValueError("async store handler")

        async def async_err_cb(msg):
            errors.append(msg)

        sub = EventsStoreSubscription(
            channel="ch",
            on_receive_event_callback=bad_handler,
            events_store_type=EventStoreStartPosition.StartFromNew,
            on_error_callback=async_err_cb,
        )

        events = []
        async for ev in client.subscribe_to_events_store(sub, cancellation_token=token):
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
        token = AsyncCancellationToken()
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(
            return_value=CancellingAsyncIteratorMock([pb_ev], token)
        )

        errors = []

        async def bad_cb(event):
            raise ValueError("seq boom")

        async def err_cb(error):
            errors.append(error)

        await client.subscribe_with_callback(
            EventsSubscription(channel="ch", on_receive_event_callback=lambda e: None),
            bad_cb,
            error_callback=err_cb,
            cancellation_token=token,
            max_concurrent_callbacks=1,
        )

        assert len(errors) == 1
        assert isinstance(errors[0], KubeMQHandlerError)

    @pytest.mark.asyncio
    async def test_sequential_handler_error_no_callback(self, mock_transport):
        """Sequential: handler raises, no error_callback -> logged (lines 715-716)."""
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(
            return_value=CancellingAsyncIteratorMock([pb_ev], token)
        )

        async def bad_cb(event):
            raise ValueError("seq no cb")

        with patch("kubemq.pubsub.async_client._logger") as mock_logger:
            await client.subscribe_with_callback(
                EventsSubscription(channel="ch", on_receive_event_callback=lambda e: None),
                bad_cb,
                cancellation_token=token,
                max_concurrent_callbacks=1,
            )
            mock_logger.error.assert_called()

    @pytest.mark.asyncio
    async def test_sequential_link_appended(self, mock_transport):
        """Verify link is created when parent context is present (line 692+)."""
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()
        pb_ev = _make_pb_event(tags={"traceparent": "00-abc-def-01"})
        mock_transport.subscribe_to_events = MagicMock(
            return_value=CancellingAsyncIteratorMock([pb_ev], token)
        )

        received = []

        async def cb(event):
            received.append(event)

        await client.subscribe_with_callback(
            EventsSubscription(channel="ch", on_receive_event_callback=lambda e: None),
            cb,
            cancellation_token=token,
            max_concurrent_callbacks=1,
        )

        assert len(received) == 1


class TestSubscribeWithCallbackConcurrentErrors:
    """Cover lines 734-762: concurrent callback error paths."""

    @pytest.mark.asyncio
    async def test_concurrent_handler_error_with_callback(self, mock_transport):
        """Concurrent: handler error -> error_callback (lines 738-743)."""
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(
            return_value=CancellingAsyncIteratorMock([pb_ev], token)
        )

        errors = []

        async def bad_cb(event):
            raise ValueError("concurrent boom")

        async def err_cb(error):
            errors.append(error)

        await client.subscribe_with_callback(
            EventsSubscription(channel="ch", on_receive_event_callback=lambda e: None),
            bad_cb,
            error_callback=err_cb,
            cancellation_token=token,
            max_concurrent_callbacks=3,
        )

        await asyncio.sleep(0.05)
        assert len(errors) == 1

    @pytest.mark.asyncio
    async def test_concurrent_handler_error_no_callback(self, mock_transport):
        """Concurrent: handler error, no error_callback -> logger.error (lines 744-749)."""
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(
            return_value=CancellingAsyncIteratorMock([pb_ev], token)
        )

        async def bad_cb(event):
            raise ValueError("concurrent no cb")

        client._logger = MagicMock()

        await client.subscribe_with_callback(
            EventsSubscription(channel="ch", on_receive_event_callback=lambda e: None),
            bad_cb,
            cancellation_token=token,
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
        token = AsyncCancellationToken()

        retryable = KubeMQConnectionError("conn lost", is_retryable=True)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = CancellingOneShotRetryIterator(retryable, [pb_ev], token)

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
                cancellation_token=token,
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
        from kubemq.pubsub.events_store_subscription import (
            EventsStoreSubscription,
            EventStoreStartPosition,
        )

        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(
            return_value=CancellingAsyncIteratorMock([pb_ev], token)
        )

        errors = []

        async def bad_cb(event):
            raise ValueError("store seq boom")

        async def err_cb(error):
            errors.append(error)

        await client.subscribe_store_with_callback(
            EventsStoreSubscription(channel="ch", on_receive_event_callback=lambda e: None, events_store_type=EventStoreStartPosition.StartFromNew),
            bad_cb,
            error_callback=err_cb,
            cancellation_token=token,
            max_concurrent_callbacks=1,
        )

        assert len(errors) == 1
        assert isinstance(errors[0], KubeMQHandlerError)

    @pytest.mark.asyncio
    async def test_sequential_handler_error_no_callback(self, mock_transport):
        """Sequential: handler raises, no error_callback -> logged (lines 894-895)."""
        from kubemq.pubsub.events_store_subscription import (
            EventsStoreSubscription,
            EventStoreStartPosition,
        )

        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(
            return_value=CancellingAsyncIteratorMock([pb_ev], token)
        )

        async def bad_cb(event):
            raise ValueError("store seq no cb")

        with patch("kubemq.pubsub.async_client._logger") as mock_logger:
            await client.subscribe_store_with_callback(
                EventsStoreSubscription(channel="ch", on_receive_event_callback=lambda e: None, events_store_type=EventStoreStartPosition.StartFromNew),
                bad_cb,
                cancellation_token=token,
                max_concurrent_callbacks=1,
            )
            mock_logger.error.assert_called()


class TestSubscribeStoreWithCallbackConcurrentErrors:
    """Cover lines 910-940: concurrent callback errors for event store."""

    @pytest.mark.asyncio
    async def test_concurrent_handler_error_with_callback(self, mock_transport):
        """Concurrent: handler error -> error_callback (lines 916-921)."""
        from kubemq.pubsub.events_store_subscription import (
            EventsStoreSubscription,
            EventStoreStartPosition,
        )

        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(
            return_value=CancellingAsyncIteratorMock([pb_ev], token)
        )

        errors = []

        async def bad_cb(event):
            raise ValueError("store concurrent boom")

        async def err_cb(error):
            errors.append(error)

        await client.subscribe_store_with_callback(
            EventsStoreSubscription(channel="ch", on_receive_event_callback=lambda e: None, events_store_type=EventStoreStartPosition.StartFromNew),
            bad_cb,
            error_callback=err_cb,
            cancellation_token=token,
            max_concurrent_callbacks=3,
        )

        await asyncio.sleep(0.05)
        assert len(errors) == 1

    @pytest.mark.asyncio
    async def test_concurrent_handler_error_no_callback(self, mock_transport):
        """Concurrent: handler error, no error_callback -> logger.error (lines 922-927)."""
        from kubemq.pubsub.events_store_subscription import (
            EventsStoreSubscription,
            EventStoreStartPosition,
        )

        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(
            return_value=CancellingAsyncIteratorMock([pb_ev], token)
        )

        async def bad_cb(event):
            raise ValueError("store concurrent no cb")

        client._logger = MagicMock()

        await client.subscribe_store_with_callback(
            EventsStoreSubscription(channel="ch", on_receive_event_callback=lambda e: None, events_store_type=EventStoreStartPosition.StartFromNew),
            bad_cb,
            cancellation_token=token,
            max_concurrent_callbacks=3,
        )

        await asyncio.sleep(0.05)
        client._logger.error.assert_called()


class TestSubscribeStoreWithCallbackStreamErrors:
    """Cover lines 944-975: KubeMQError and generic errors in subscribe_store_with_callback."""

    @pytest.mark.asyncio
    async def test_retryable_error_backoff(self, mock_transport):
        """Retryable KubeMQError -> backoff, retry (lines 944-968)."""
        from kubemq.pubsub.events_store_subscription import (
            EventsStoreSubscription,
            EventStoreStartPosition,
        )

        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()

        retryable = KubeMQConnectionError("store conn lost", is_retryable=True)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = CancellingOneShotRetryIterator(retryable, [pb_ev], token)

        errors = []
        received = []

        async def cb(event):
            received.append(event)

        async def err_cb(error):
            errors.append(error)

        with patch("kubemq.pubsub.async_client.asyncio.sleep", new_callable=AsyncMock):
            await client.subscribe_store_with_callback(
                EventsStoreSubscription(channel="ch", on_receive_event_callback=lambda e: None, events_store_type=EventStoreStartPosition.StartFromNew),
                cb,
                error_callback=err_cb,
                cancellation_token=token,
            )

        assert len(received) == 1
        assert len(errors) == 1
        assert isinstance(errors[0], KubeMQStreamBrokenError)

    @pytest.mark.asyncio
    async def test_non_retryable_with_error_callback(self, mock_transport):
        """Non-retryable with error_callback -> called, returns (lines 944-950)."""
        from kubemq.pubsub.events_store_subscription import (
            EventsStoreSubscription,
            EventStoreStartPosition,
        )

        client = _make_connected_client(mock_transport)

        non_retryable = KubeMQError("store not retriable", is_retryable=False)
        mock_transport.subscribe_to_events = MagicMock(
            return_value=RaisingAsyncIterator([], non_retryable)
        )

        errors = []

        async def err_cb(error):
            errors.append(error)

        await client.subscribe_store_with_callback(
            EventsStoreSubscription(channel="ch", on_receive_event_callback=lambda e: None, events_store_type=EventStoreStartPosition.StartFromNew),
            AsyncMock(),
            error_callback=err_cb,
        )

        assert len(errors) == 1

    @pytest.mark.asyncio
    async def test_non_retryable_without_callback(self, mock_transport):
        """Non-retryable, no error_callback -> re-raises."""
        from kubemq.pubsub.events_store_subscription import (
            EventsStoreSubscription,
            EventStoreStartPosition,
        )

        client = _make_connected_client(mock_transport)

        non_retryable = KubeMQError("store fatal", is_retryable=False)
        mock_transport.subscribe_to_events = MagicMock(
            return_value=RaisingAsyncIterator([], non_retryable)
        )

        with pytest.raises(KubeMQError, match="store fatal"):
            await client.subscribe_store_with_callback(
                EventsStoreSubscription(channel="ch", on_receive_event_callback=lambda e: None, events_store_type=EventStoreStartPosition.StartFromNew),
                AsyncMock(),
            )

    @pytest.mark.asyncio
    async def test_generic_exception_with_callback(self, mock_transport):
        """Generic Exception with error_callback -> called, returns (lines 970-975)."""
        from kubemq.pubsub.events_store_subscription import (
            EventsStoreSubscription,
            EventStoreStartPosition,
        )

        client = _make_connected_client(mock_transport)

        mock_transport.subscribe_to_events = MagicMock(
            return_value=RaisingAsyncIterator([], RuntimeError("store generic"))
        )

        errors = []

        async def err_cb(error):
            errors.append(error)

        await client.subscribe_store_with_callback(
            EventsStoreSubscription(channel="ch", on_receive_event_callback=lambda e: None, events_store_type=EventStoreStartPosition.StartFromNew),
            AsyncMock(),
            error_callback=err_cb,
        )

        assert len(errors) == 1

    @pytest.mark.asyncio
    async def test_generic_exception_without_callback(self, mock_transport):
        """Generic Exception, no error_callback -> re-raises."""
        from kubemq.pubsub.events_store_subscription import (
            EventsStoreSubscription,
            EventStoreStartPosition,
        )

        client = _make_connected_client(mock_transport)

        mock_transport.subscribe_to_events = MagicMock(
            return_value=RaisingAsyncIterator([], RuntimeError("store gen no cb"))
        )

        with pytest.raises(RuntimeError, match="store gen no cb"):
            await client.subscribe_store_with_callback(
                EventsStoreSubscription(channel="ch", on_receive_event_callback=lambda e: None, events_store_type=EventStoreStartPosition.StartFromNew),
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
        from kubemq.core.exceptions import KubeMQValidationError

        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        message = EventMessage(channel="ch", body=b"test")
        validation_err = ValueError("EventMessage validation failed")
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
        token = AsyncCancellationToken()
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(
            return_value=CancellingAsyncIteratorMock([pb_ev], token)
        )

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
        async for ev in client.subscribe_to_events(sub, cancellation_token=token):
            events.append(ev)

        assert len(events) == 1

    @pytest.mark.asyncio
    async def test_subscribe_with_callback_error_callback_raises_is_logged(self, mock_transport):
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(
            return_value=CancellingAsyncIteratorMock([pb_ev], token)
        )

        async def bad_cb(event):
            raise ValueError("handler boom")

        async def bad_err_cb(error):
            raise RuntimeError("error callback boom")

        with patch("kubemq.pubsub.async_client._logger") as mock_logger:
            await client.subscribe_with_callback(
                EventsSubscription(channel="ch", on_receive_event_callback=lambda e: None),
                bad_cb,
                error_callback=bad_err_cb,
                cancellation_token=token,
                max_concurrent_callbacks=1,
            )
            mock_logger.exception.assert_called()


class TestAsyncClientSubscribeToEventsStoreResumeSequence:
    """Tests for subscribe_to_events_store resume from sequence (lines 565-570)."""

    @pytest.mark.asyncio
    async def test_store_subscription_resumes_from_sequence(self, mock_transport):
        from kubemq.pubsub.events_store_subscription import (
            EventsStoreSubscription,
            EventStoreStartPosition,
        )

        client = _make_connected_client(mock_transport)
        cancel_token = AsyncCancellationToken()

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
            return CancellingAsyncIteratorMock([pb_ev2], cancel_token)

        mock_transport.subscribe_to_events = mock_subscribe

        errors = []

        def err_cb(msg):
            errors.append(msg)

        sub = EventsStoreSubscription(
            channel="store-ch",
            on_receive_event_callback=lambda e: None,
            events_store_type=EventStoreStartPosition.StartFromNew,
            on_error_callback=err_cb,
        )

        events = []
        with patch("kubemq.pubsub.async_client.asyncio.sleep", new_callable=AsyncMock):
            async for ev in client.subscribe_to_events_store(sub, cancellation_token=cancel_token):
                events.append(ev)

        assert len(events) == 2
        assert events[0].sequence == 42
        assert events[1].sequence == 43
        assert call_count[0] == 2


class TestAsyncClientSubscribeStoreWithCallbackResumeSequence:
    """Tests for subscribe_store_with_callback resume from sequence (lines 929-934)."""

    @pytest.mark.asyncio
    async def test_store_callback_resumes_from_sequence(self, mock_transport):
        from kubemq.pubsub.events_store_subscription import (
            EventsStoreSubscription,
            EventStoreStartPosition,
        )

        client = _make_connected_client(mock_transport)
        cancel_token = AsyncCancellationToken()

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
            return CancellingAsyncIteratorMock([pb_ev2], cancel_token)

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
                    events_store_type=EventStoreStartPosition.StartFromNew,
                ),
                cb,
                error_callback=err_cb,
                cancellation_token=cancel_token,
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
        from kubemq.pubsub.events_store_subscription import (
            EventsStoreSubscription,
            EventStoreStartPosition,
        )

        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(
            return_value=CancellingAsyncIteratorMock([pb_ev], token)
        )

        async def bad_cb(event):
            raise ValueError("store handler boom")

        async def bad_err_cb(error):
            raise RuntimeError("error callback boom")

        with patch("kubemq.pubsub.async_client._logger") as mock_logger:
            await client.subscribe_store_with_callback(
                EventsStoreSubscription(channel="ch", on_receive_event_callback=lambda e: None, events_store_type=EventStoreStartPosition.StartFromNew),
                bad_cb,
                error_callback=bad_err_cb,
                cancellation_token=token,
                max_concurrent_callbacks=1,
            )
            mock_logger.exception.assert_called()


class TestAsyncClientPublishEventStoreNoneResult:
    """Test send_event_store returns empty result on None."""

    @pytest.mark.asyncio
    async def test_send_event_store_none_result(self, mock_transport):
        client = _make_connected_client(mock_transport)
        client._event_sender.send = AsyncMock(return_value=None)

        msg = EventStoreMessage(channel="ch", body=b"x")
        result = await client.send_event_store(msg)

        assert result.sent is False


# ==============================================================================
# Additional Targeted Coverage Tests — 95% target
# ==============================================================================

from kubemq.pubsub.event_message_received import EventReceived  # noqa: E402
from kubemq.pubsub.event_store_message_received import EventStoreReceived  # noqa: E402
from kubemq.pubsub.events_store_subscription import (  # noqa: E402
    EventsStoreSubscription,
    EventStoreStartPosition,
)


class TestSubscribeToEventsProcessingError:
    """Cover lines 470-475: processing error in subscribe_to_events."""

    @pytest.mark.asyncio
    async def test_decode_error_propagates(self, mock_transport):
        """EventReceived.decode raising -> proc_err path."""
        client = _make_connected_client(mock_transport)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(return_value=AsyncIteratorMock([pb_ev]))

        sub = EventsSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
        )

        with patch.object(EventReceived, "decode", side_effect=RuntimeError("decode failed")):
            with pytest.raises(RuntimeError, match="decode failed"):
                async for _ in client.subscribe_to_events(sub):
                    pass


class TestSubscribeToEventsStoreErrorCallbackRaises:
    """Cover lines 613-614: error callback itself raises in store subscribe."""

    @pytest.mark.asyncio
    async def test_store_error_callback_raises_is_logged(self, mock_transport):
        """error_callback raises in store async-iter subscribe -> logged."""
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(
            return_value=CancellingAsyncIteratorMock([pb_ev], token)
        )

        def bad_handler(event):
            raise ValueError("handler crash")

        async def bad_err_cb(msg):
            raise RuntimeError("error callback crashed")

        sub = EventsStoreSubscription(
            channel="ch",
            on_receive_event_callback=bad_handler,
            events_store_type=EventStoreStartPosition.StartFromNew,
            on_error_callback=bad_err_cb,
        )

        events = []
        async for ev in client.subscribe_to_events_store(sub, cancellation_token=token):
            events.append(ev)

        assert len(events) == 1


class TestSubscribeToEventsStoreProcessingError:
    """Cover lines 621-626: processing error in subscribe_to_events_store."""

    @pytest.mark.asyncio
    async def test_store_decode_error_propagates(self, mock_transport):
        """EventStoreReceived.decode raising -> proc_err path."""
        client = _make_connected_client(mock_transport)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(return_value=AsyncIteratorMock([pb_ev]))

        sub = EventsStoreSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
            events_store_type=EventStoreStartPosition.StartFromNew,
        )

        with patch.object(EventStoreReceived, "decode", side_effect=RuntimeError("store decode")):
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
            events_store_type=EventStoreStartPosition.StartFromNew,
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
            events_store_type=EventStoreStartPosition.StartFromNew,
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
        token = AsyncCancellationToken()
        retryable = KubeMQConnectionError("events lost", is_retryable=True)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = CancellingOneShotRetryIterator(retryable, [pb_ev], token)

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
            async for ev in client.subscribe_to_events(sub, cancellation_token=token):
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
        token = AsyncCancellationToken()
        retryable = KubeMQConnectionError("store lost", is_retryable=True)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = CancellingOneShotRetryIterator(retryable, [pb_ev], token)

        errors = []

        async def async_err_cb(msg):
            errors.append(msg)

        sub = EventsStoreSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
            events_store_type=EventStoreStartPosition.StartFromNew,
            on_error_callback=async_err_cb,
        )

        events = []
        with patch("kubemq.pubsub.async_client.asyncio.sleep", new_callable=AsyncMock):
            async for ev in client.subscribe_to_events_store(sub, cancellation_token=token):
                events.append(ev)

        assert len(events) == 1
        assert len(errors) == 1
        assert "Stream broken" in errors[0]


class TestSubscribeWithCallbackProcessingError:
    """Cover lines 786-791: processing error in subscribe_with_callback."""

    @pytest.mark.asyncio
    async def test_decode_error_propagates(self, mock_transport):
        """EventReceived.decode raising -> proc_err re-raised."""
        client = _make_connected_client(mock_transport)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(return_value=AsyncIteratorMock([pb_ev]))

        async def callback(event):
            pass

        with patch.object(EventReceived, "decode", side_effect=RuntimeError("cb decode fail")):
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
        token = AsyncCancellationToken()
        retryable = KubeMQConnectionError("conn lost", is_retryable=True)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = CancellingOneShotRetryIterator(retryable, [pb_ev], token)

        received = []

        async def cb(event):
            received.append(event)

        with patch("kubemq.pubsub.async_client.asyncio.sleep", new_callable=AsyncMock):
            await client.subscribe_with_callback(
                EventsSubscription(channel="ch", on_receive_event_callback=lambda e: None),
                cb,
                cancellation_token=token,
            )

        assert len(received) == 1


class TestSubscribeStoreWithCallbackProcessingError:
    """Cover lines 975-980: processing error in subscribe_store_with_callback."""

    @pytest.mark.asyncio
    async def test_store_decode_error_propagates(self, mock_transport):
        """EventStoreReceived.decode raising -> proc_err re-raised."""
        client = _make_connected_client(mock_transport)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(return_value=AsyncIteratorMock([pb_ev]))

        async def callback(event):
            pass

        with patch.object(
            EventStoreReceived, "decode", side_effect=RuntimeError("store cb decode")
        ):
            with pytest.raises(RuntimeError, match="store cb decode"):
                await client.subscribe_store_with_callback(
                    EventsStoreSubscription(channel="ch", on_receive_event_callback=lambda e: None, events_store_type=EventStoreStartPosition.StartFromNew),
                    callback,
                )


class TestSubscribeStoreWithCallbackRetryableNoErrorCb:
    """Cover line 1036 false branch: retryable error without error_callback."""

    @pytest.mark.asyncio
    async def test_store_retryable_no_callback_retries(self, mock_transport):
        """Retryable KubeMQError without error_callback still retries."""
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()
        retryable = KubeMQConnectionError("store lost", is_retryable=True)
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = CancellingOneShotRetryIterator(retryable, [pb_ev], token)

        received = []

        async def cb(event):
            received.append(event)

        with patch("kubemq.pubsub.async_client.asyncio.sleep", new_callable=AsyncMock):
            await client.subscribe_store_with_callback(
                EventsStoreSubscription(channel="ch", on_receive_event_callback=lambda e: None, events_store_type=EventStoreStartPosition.StartFromNew),
                cb,
                cancellation_token=token,
            )

        assert len(received) == 1


class TestSubscribeStoreWithCallbackConcurrentErrorCallbackRaises:
    """Cover lines 996-997: concurrent store callback error_callback raises."""

    @pytest.mark.asyncio
    async def test_concurrent_store_error_callback_raises(self, mock_transport):
        """error_callback raises in concurrent store path -> silently caught."""
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(
            return_value=CancellingAsyncIteratorMock([pb_ev], token)
        )

        async def bad_cb(event):
            raise ValueError("concurrent store err")

        async def bad_err_cb(error):
            raise RuntimeError("err cb exploded")

        await client.subscribe_store_with_callback(
            EventsStoreSubscription(channel="ch", on_receive_event_callback=lambda e: None, events_store_type=EventStoreStartPosition.StartFromNew),
            bad_cb,
            error_callback=bad_err_cb,
            cancellation_token=token,
            max_concurrent_callbacks=3,
        )

        await asyncio.sleep(0.05)


# ==============================================================================
# Fast subscribe paths — subscribe_to_events_fast / subscribe_to_events_store_fast
# ==============================================================================


class ErrorThenCancelIterator:
    """First call raises error, second call returns CancellingAsyncIteratorMock."""

    def __init__(self, error, items, token):
        self.calls = 0
        self.error = error
        self.items = items
        self.token = token

    def __call__(self, *args, **kwargs):
        self.calls += 1
        if self.calls == 1:
            raise self.error
        return CancellingAsyncIteratorMock(self.items, self.token)


class _CancelAndRaiseIterator:
    """Cancels token then raises an exception."""

    def __init__(self, token, exc):
        self._token = token
        self._exc = exc

    def __aiter__(self):
        return self

    async def __anext__(self):
        self._token.cancel()
        raise self._exc


class TestAsyncClientSubscribeToEventsFast:
    """Tests for subscribe_to_events_fast() — lines ~728-777."""

    @pytest.mark.asyncio
    async def test_fast_yields_messages(self, mock_transport):
        """subscribe_to_events_fast yields decoded EventReceived."""
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()
        pb_ev = _make_pb_event(event_id="fast-1")
        mock_transport.subscribe_to_events = MagicMock(
            return_value=CancellingAsyncIteratorMock([pb_ev], token)
        )

        sub = EventsSubscription(channel="ch", on_receive_event_callback=lambda e: None)

        events = []
        async for ev in client.subscribe_to_events_fast(sub, cancellation_token=token):
            events.append(ev)

        assert len(events) == 1
        assert events[0].id == "fast-1"

    @pytest.mark.asyncio
    async def test_fast_stream_end_retry(self, mock_transport):
        """Stream ends -> reconnects (lines 740-751)."""
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()
        pb_ev = _make_pb_event(event_id="retry-1")

        call_count = [0]

        def mock_subscribe(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return AsyncIteratorMock([])
            return CancellingAsyncIteratorMock([pb_ev], token)

        mock_transport.subscribe_to_events = mock_subscribe

        sub = EventsSubscription(channel="ch", on_receive_event_callback=lambda e: None)

        events = []
        with patch("kubemq.pubsub.async_client.asyncio.sleep", new_callable=AsyncMock):
            async for ev in client.subscribe_to_events_fast(sub, cancellation_token=token):
                events.append(ev)

        assert len(events) == 1
        assert call_count[0] == 2

    @pytest.mark.asyncio
    async def test_fast_retryable_kubemq_error(self, mock_transport):
        """Retryable KubeMQError -> retry (lines 754-766)."""
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()
        retryable = KubeMQConnectionError("conn lost", is_retryable=True)
        pb_ev = _make_pb_event(event_id="retry-2")
        mock_transport.subscribe_to_events = CancellingOneShotRetryIterator(retryable, [pb_ev], token)

        sub = EventsSubscription(channel="ch", on_receive_event_callback=lambda e: None)

        events = []
        with patch("kubemq.pubsub.async_client.asyncio.sleep", new_callable=AsyncMock):
            async for ev in client.subscribe_to_events_fast(sub, cancellation_token=token):
                events.append(ev)

        assert len(events) == 1

    @pytest.mark.asyncio
    async def test_fast_non_retryable_kubemq_error_raises(self, mock_transport):
        """Non-retryable KubeMQError -> raises (line 757-758)."""
        client = _make_connected_client(mock_transport)

        non_retryable = KubeMQError("fatal fast", is_retryable=False)
        mock_transport.subscribe_to_events = MagicMock(
            return_value=RaisingAsyncIterator([], non_retryable)
        )

        sub = EventsSubscription(channel="ch", on_receive_event_callback=lambda e: None)

        with pytest.raises(KubeMQError, match="fatal fast"):
            async for _ in client.subscribe_to_events_fast(sub):
                pass

    @pytest.mark.asyncio
    async def test_fast_kubemq_client_closed_error(self, mock_transport):
        """KubeMQClientClosedError -> propagates (line 752-753)."""
        from kubemq.core.exceptions import KubeMQClientClosedError

        client = _make_connected_client(mock_transport)

        mock_transport.subscribe_to_events = MagicMock(
            return_value=RaisingAsyncIterator([], KubeMQClientClosedError("closed"))
        )

        sub = EventsSubscription(channel="ch", on_receive_event_callback=lambda e: None)

        with pytest.raises(KubeMQClientClosedError):
            async for _ in client.subscribe_to_events_fast(sub):
                pass

    @pytest.mark.asyncio
    async def test_fast_generic_exception_retry(self, mock_transport):
        """Generic Exception -> retry with backoff (lines 767-777)."""
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()
        pb_ev = _make_pb_event(event_id="gen-retry")

        mock_transport.subscribe_to_events = ErrorThenCancelIterator(
            RuntimeError("transient"), [pb_ev], token
        )

        sub = EventsSubscription(channel="ch", on_receive_event_callback=lambda e: None)

        events = []
        with patch("kubemq.pubsub.async_client.asyncio.sleep", new_callable=AsyncMock):
            async for ev in client.subscribe_to_events_fast(sub, cancellation_token=token):
                events.append(ev)

        assert len(events) == 1

    @pytest.mark.asyncio
    async def test_fast_kubemq_error_cancelled_returns(self, mock_transport):
        """KubeMQError when token is cancelled -> return (line 755-756)."""
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()

        retryable = KubeMQConnectionError("lost", is_retryable=True)
        mock_transport.subscribe_to_events = MagicMock(
            return_value=_CancelAndRaiseIterator(token, retryable)
        )

        sub = EventsSubscription(channel="ch", on_receive_event_callback=lambda e: None)

        events = []
        async for ev in client.subscribe_to_events_fast(sub, cancellation_token=token):
            events.append(ev)

        assert len(events) == 0

    @pytest.mark.asyncio
    async def test_fast_generic_exception_cancelled_returns(self, mock_transport):
        """Generic exception when token is cancelled -> return (line 768-769)."""
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()

        mock_transport.subscribe_to_events = MagicMock(
            return_value=_CancelAndRaiseIterator(token, RuntimeError("gone"))
        )

        sub = EventsSubscription(channel="ch", on_receive_event_callback=lambda e: None)

        events = []
        async for ev in client.subscribe_to_events_fast(sub, cancellation_token=token):
            events.append(ev)

        assert len(events) == 0

    @pytest.mark.asyncio
    async def test_fast_creates_token_if_not_provided(self, mock_transport):
        """subscribe_to_events_fast creates token if none supplied (line 728)."""
        client = _make_connected_client(mock_transport)

        non_retryable = KubeMQError("fatal", is_retryable=False)
        mock_transport.subscribe_to_events = MagicMock(
            return_value=RaisingAsyncIterator([], non_retryable)
        )

        sub = EventsSubscription(channel="ch", on_receive_event_callback=lambda e: None)

        with pytest.raises(KubeMQError):
            async for _ in client.subscribe_to_events_fast(sub):
                pass


class TestAsyncClientSubscribeToEventsStoreFast:
    """Tests for subscribe_to_events_store_fast() — lines ~790-851."""

    @pytest.mark.asyncio
    async def test_fast_store_yields_messages(self, mock_transport):
        """subscribe_to_events_store_fast yields decoded EventStoreReceived."""
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()
        pb_ev = _make_pb_event(event_id="sfast-1")
        mock_transport.subscribe_to_events = MagicMock(
            return_value=CancellingAsyncIteratorMock([pb_ev], token)
        )

        sub = EventsStoreSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
            events_store_type=EventStoreStartPosition.StartFromNew,
        )

        events = []
        async for ev in client.subscribe_to_events_store_fast(sub, cancellation_token=token):
            events.append(ev)

        assert len(events) == 1
        assert events[0].id == "sfast-1"

    @pytest.mark.asyncio
    async def test_fast_store_stream_end_retry(self, mock_transport):
        """Store stream ends -> reconnects (lines 815-825)."""
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()
        pb_ev = _make_pb_event(event_id="store-retry")

        call_count = [0]

        def mock_subscribe(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return AsyncIteratorMock([])
            return CancellingAsyncIteratorMock([pb_ev], token)

        mock_transport.subscribe_to_events = mock_subscribe

        sub = EventsStoreSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
            events_store_type=EventStoreStartPosition.StartFromNew,
        )

        events = []
        with patch("kubemq.pubsub.async_client.asyncio.sleep", new_callable=AsyncMock):
            async for ev in client.subscribe_to_events_store_fast(sub, cancellation_token=token):
                events.append(ev)

        assert len(events) == 1
        assert call_count[0] == 2

    @pytest.mark.asyncio
    async def test_fast_store_sequence_resumption(self, mock_transport):
        """On reconnect, resumes from last sequence (lines 800-808)."""
        client = _make_connected_client(mock_transport)
        cancel_token = AsyncCancellationToken()

        pb_ev1 = MagicMock()
        pb_ev1.EventID = "ev-1"
        pb_ev1.Channel = "ch"
        pb_ev1.Metadata = ""
        pb_ev1.Body = b"d1"
        pb_ev1.Timestamp = 100
        pb_ev1.Sequence = 42
        pb_ev1.Tags = {}

        pb_ev2 = MagicMock()
        pb_ev2.EventID = "ev-2"
        pb_ev2.Channel = "ch"
        pb_ev2.Metadata = ""
        pb_ev2.Body = b"d2"
        pb_ev2.Timestamp = 101
        pb_ev2.Sequence = 43
        pb_ev2.Tags = {}

        retryable = KubeMQConnectionError("lost", is_retryable=True)
        call_count = [0]

        def mock_subscribe(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return RaisingAsyncIterator([pb_ev1], retryable)
            return CancellingAsyncIteratorMock([pb_ev2], cancel_token)

        mock_transport.subscribe_to_events = mock_subscribe

        sub = EventsStoreSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
            events_store_type=EventStoreStartPosition.StartFromNew,
        )

        events = []
        with patch("kubemq.pubsub.async_client.asyncio.sleep", new_callable=AsyncMock):
            async for ev in client.subscribe_to_events_store_fast(sub, cancellation_token=cancel_token):
                events.append(ev)

        assert len(events) == 2
        assert events[0].sequence == 42
        assert events[1].sequence == 43
        assert call_count[0] == 2

    @pytest.mark.asyncio
    async def test_fast_store_retryable_kubemq_error(self, mock_transport):
        """Retryable KubeMQError -> retry (lines 828-840)."""
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()
        retryable = KubeMQConnectionError("store lost", is_retryable=True)
        pb_ev = _make_pb_event(event_id="store-retry-2")
        mock_transport.subscribe_to_events = CancellingOneShotRetryIterator(retryable, [pb_ev], token)

        sub = EventsStoreSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
            events_store_type=EventStoreStartPosition.StartFromNew,
        )

        events = []
        with patch("kubemq.pubsub.async_client.asyncio.sleep", new_callable=AsyncMock):
            async for ev in client.subscribe_to_events_store_fast(sub, cancellation_token=token):
                events.append(ev)

        assert len(events) == 1

    @pytest.mark.asyncio
    async def test_fast_store_non_retryable_raises(self, mock_transport):
        """Non-retryable KubeMQError -> raises (lines 831-832)."""
        client = _make_connected_client(mock_transport)

        non_retryable = KubeMQError("store fatal", is_retryable=False)
        mock_transport.subscribe_to_events = MagicMock(
            return_value=RaisingAsyncIterator([], non_retryable)
        )

        sub = EventsStoreSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
            events_store_type=EventStoreStartPosition.StartFromNew,
        )

        with pytest.raises(KubeMQError, match="store fatal"):
            async for _ in client.subscribe_to_events_store_fast(sub):
                pass

    @pytest.mark.asyncio
    async def test_fast_store_client_closed_error(self, mock_transport):
        """KubeMQClientClosedError -> propagates (line 826-827)."""
        from kubemq.core.exceptions import KubeMQClientClosedError

        client = _make_connected_client(mock_transport)

        mock_transport.subscribe_to_events = MagicMock(
            return_value=RaisingAsyncIterator([], KubeMQClientClosedError("closed"))
        )

        sub = EventsStoreSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
            events_store_type=EventStoreStartPosition.StartFromNew,
        )

        with pytest.raises(KubeMQClientClosedError):
            async for _ in client.subscribe_to_events_store_fast(sub):
                pass

    @pytest.mark.asyncio
    async def test_fast_store_generic_exception_retry(self, mock_transport):
        """Generic Exception -> retry (lines 841-851)."""
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()
        pb_ev = _make_pb_event(event_id="store-gen-retry")

        mock_transport.subscribe_to_events = ErrorThenCancelIterator(
            RuntimeError("transient store"), [pb_ev], token
        )

        sub = EventsStoreSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
            events_store_type=EventStoreStartPosition.StartFromNew,
        )

        events = []
        with patch("kubemq.pubsub.async_client.asyncio.sleep", new_callable=AsyncMock):
            async for ev in client.subscribe_to_events_store_fast(sub, cancellation_token=token):
                events.append(ev)

        assert len(events) == 1

    @pytest.mark.asyncio
    async def test_fast_store_kubemq_error_cancelled_returns(self, mock_transport):
        """KubeMQError when token is cancelled -> return (line 829-830)."""
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()

        retryable = KubeMQConnectionError("lost", is_retryable=True)
        mock_transport.subscribe_to_events = MagicMock(
            return_value=_CancelAndRaiseIterator(token, retryable)
        )

        sub = EventsStoreSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
            events_store_type=EventStoreStartPosition.StartFromNew,
        )

        events = []
        async for ev in client.subscribe_to_events_store_fast(sub, cancellation_token=token):
            events.append(ev)

        assert len(events) == 0

    @pytest.mark.asyncio
    async def test_fast_store_generic_exception_cancelled_returns(self, mock_transport):
        """Generic exception when token is cancelled -> return (line 842-843)."""
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()

        mock_transport.subscribe_to_events = MagicMock(
            return_value=_CancelAndRaiseIterator(token, RuntimeError("gone"))
        )

        sub = EventsStoreSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
            events_store_type=EventStoreStartPosition.StartFromNew,
        )

        events = []
        async for ev in client.subscribe_to_events_store_fast(sub, cancellation_token=token):
            events.append(ev)

        assert len(events) == 0

    @pytest.mark.asyncio
    async def test_fast_store_creates_token_if_not_provided(self, mock_transport):
        """subscribe_to_events_store_fast creates token if none supplied (line 790)."""
        client = _make_connected_client(mock_transport)

        non_retryable = KubeMQError("fatal", is_retryable=False)
        mock_transport.subscribe_to_events = MagicMock(
            return_value=RaisingAsyncIterator([], non_retryable)
        )

        sub = EventsStoreSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
            events_store_type=EventStoreStartPosition.StartFromNew,
        )

        with pytest.raises(KubeMQError):
            async for _ in client.subscribe_to_events_store_fast(sub):
                pass


# ==============================================================================
# subscribe_to_events_store error paths — lines ~660-697, 997-1019
# ==============================================================================


class TestAsyncClientSubscribeToEventsStoreErrorPaths:
    """Additional error path tests for subscribe_to_events_store."""

    @pytest.mark.asyncio
    async def test_store_stream_end_reconnect(self, mock_transport):
        """Stream ends without cancel -> reconnects (lines 994-1004)."""
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()
        pb_ev = _make_pb_event(event_id="reconnect-1")

        call_count = [0]

        def mock_subscribe(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return AsyncIteratorMock([])
            return CancellingAsyncIteratorMock([pb_ev], token)

        mock_transport.subscribe_to_events = mock_subscribe

        sub = EventsStoreSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
            events_store_type=EventStoreStartPosition.StartFromNew,
        )

        events = []
        with patch("kubemq.pubsub.async_client.asyncio.sleep", new_callable=AsyncMock):
            async for ev in client.subscribe_to_events_store(sub, cancellation_token=token):
                events.append(ev)

        assert len(events) == 1
        assert call_count[0] == 2

    @pytest.mark.asyncio
    async def test_store_client_closed_error_propagates(self, mock_transport):
        """KubeMQClientClosedError -> re-raised (line 1006-1007)."""
        from kubemq.core.exceptions import KubeMQClientClosedError

        client = _make_connected_client(mock_transport)

        mock_transport.subscribe_to_events = MagicMock(
            return_value=RaisingAsyncIterator([], KubeMQClientClosedError("closed"))
        )

        sub = EventsStoreSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
            events_store_type=EventStoreStartPosition.StartFromNew,
        )

        with pytest.raises(KubeMQClientClosedError):
            async for _ in client.subscribe_to_events_store(sub):
                pass

    @pytest.mark.asyncio
    async def test_store_kubemq_error_when_cancelled_raises(self, mock_transport):
        """KubeMQError when token is already cancelled -> raises (line 1010-1011)."""
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()

        retryable = KubeMQConnectionError("lost", is_retryable=True)
        mock_transport.subscribe_to_events = MagicMock(
            return_value=_CancelAndRaiseIterator(token, retryable)
        )

        sub = EventsStoreSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
            events_store_type=EventStoreStartPosition.StartFromNew,
        )

        with pytest.raises(KubeMQConnectionError):
            async for _ in client.subscribe_to_events_store(sub, cancellation_token=token):
                pass

    @pytest.mark.asyncio
    async def test_store_non_retryable_with_sync_error_callback(self, mock_transport):
        """Non-retryable with sync on_error_callback (lines 1012-1018)."""
        client = _make_connected_client(mock_transport)

        non_retryable = KubeMQError("fatal sync cb", is_retryable=False)
        mock_transport.subscribe_to_events = MagicMock(
            return_value=RaisingAsyncIterator([], non_retryable)
        )

        errors = []

        def sync_err_cb(msg):
            errors.append(msg)

        sub = EventsStoreSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
            events_store_type=EventStoreStartPosition.StartFromNew,
            on_error_callback=sync_err_cb,
        )

        with pytest.raises(KubeMQError, match="fatal sync cb"):
            async for _ in client.subscribe_to_events_store(sub):
                pass

        assert len(errors) == 1

    @pytest.mark.asyncio
    async def test_store_retryable_sync_error_callback(self, mock_transport):
        """Retryable error with sync on_error_callback (lines 1027-1032)."""
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()
        retryable = KubeMQConnectionError("lost sync", is_retryable=True)
        pb_ev = _make_pb_event(event_id="sync-retry")

        mock_transport.subscribe_to_events = CancellingOneShotRetryIterator(retryable, [pb_ev], token)

        errors = []

        def sync_err_cb(msg):
            errors.append(msg)

        sub = EventsStoreSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
            events_store_type=EventStoreStartPosition.StartFromNew,
            on_error_callback=sync_err_cb,
        )

        events = []
        with patch("kubemq.pubsub.async_client.asyncio.sleep", new_callable=AsyncMock):
            async for ev in client.subscribe_to_events_store(sub, cancellation_token=token):
                events.append(ev)

        assert len(events) == 1
        assert len(errors) == 1
        assert "Stream broken" in errors[0]

    @pytest.mark.asyncio
    async def test_store_generic_exception_sync_callback(self, mock_transport):
        """Generic exception with sync on_error_callback (lines 1043-1049)."""
        client = _make_connected_client(mock_transport)

        mock_transport.subscribe_to_events = MagicMock(
            return_value=RaisingAsyncIterator([], RuntimeError("sync generic"))
        )

        errors = []

        def sync_err_cb(msg):
            errors.append(msg)

        sub = EventsStoreSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
            events_store_type=EventStoreStartPosition.StartFromNew,
            on_error_callback=sync_err_cb,
        )

        with pytest.raises(RuntimeError, match="sync generic"):
            async for _ in client.subscribe_to_events_store(sub):
                pass

        assert len(errors) == 1

    @pytest.mark.asyncio
    async def test_store_generic_exception_async_callback(self, mock_transport):
        """Generic exception with async on_error_callback (lines 1046-1047)."""
        client = _make_connected_client(mock_transport)

        mock_transport.subscribe_to_events = MagicMock(
            return_value=RaisingAsyncIterator([], RuntimeError("async generic store"))
        )

        errors = []

        async def async_err_cb(msg):
            errors.append(msg)

        sub = EventsStoreSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
            events_store_type=EventStoreStartPosition.StartFromNew,
            on_error_callback=async_err_cb,
        )

        with pytest.raises(RuntimeError, match="async generic store"):
            async for _ in client.subscribe_to_events_store(sub):
                pass

        assert len(errors) == 1


# ==============================================================================
# subscribe_with_callback fast event paths — lines ~1220-1268
# ==============================================================================


class TestSubscribeWithCallbackFastPaths:
    """Cover subscribe_with_callback stream-end / error paths for fast events."""

    @pytest.mark.asyncio
    async def test_callback_stream_end_reconnect(self, mock_transport):
        """Stream ends -> reconnect (lines 1220-1230)."""
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()
        pb_ev = _make_pb_event(event_id="cb-reconnect")

        call_count = [0]

        def mock_subscribe(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return AsyncIteratorMock([])
            return CancellingAsyncIteratorMock([pb_ev], token)

        mock_transport.subscribe_to_events = mock_subscribe

        received = []

        async def cb(event):
            received.append(event)

        with patch("kubemq.pubsub.async_client.asyncio.sleep", new_callable=AsyncMock):
            await client.subscribe_with_callback(
                EventsSubscription(channel="ch", on_receive_event_callback=lambda e: None),
                cb,
                cancellation_token=token,
            )

        assert len(received) == 1
        assert call_count[0] == 2

    @pytest.mark.asyncio
    async def test_callback_client_closed_error(self, mock_transport):
        """KubeMQClientClosedError -> propagates (line 1232-1233)."""
        from kubemq.core.exceptions import KubeMQClientClosedError

        client = _make_connected_client(mock_transport)

        mock_transport.subscribe_to_events = MagicMock(
            return_value=RaisingAsyncIterator([], KubeMQClientClosedError("closed"))
        )

        with pytest.raises(KubeMQClientClosedError):
            await client.subscribe_with_callback(
                EventsSubscription(channel="ch", on_receive_event_callback=lambda e: None),
                AsyncMock(),
            )

    @pytest.mark.asyncio
    async def test_callback_kubemq_error_cancelled_returns(self, mock_transport):
        """KubeMQError when token cancelled -> return (line 1236-1237)."""
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()

        retryable = KubeMQConnectionError("lost", is_retryable=True)
        mock_transport.subscribe_to_events = MagicMock(
            return_value=_CancelAndRaiseIterator(token, retryable)
        )

        await client.subscribe_with_callback(
            EventsSubscription(channel="ch", on_receive_event_callback=lambda e: None),
            AsyncMock(),
            cancellation_token=token,
        )


# ==============================================================================
# subscribe_store_with_callback fast event paths — lines ~1336-1493
# ==============================================================================


class TestSubscribeStoreWithCallbackFastPaths:
    """Cover subscribe_store_with_callback stream-end / error / resume paths."""

    @pytest.mark.asyncio
    async def test_store_callback_stream_end_reconnect(self, mock_transport):
        """Stream ends -> reconnect (lines 1443-1453)."""
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()
        pb_ev = _make_pb_event(event_id="scb-reconnect")

        call_count = [0]

        def mock_subscribe(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return AsyncIteratorMock([])
            return CancellingAsyncIteratorMock([pb_ev], token)

        mock_transport.subscribe_to_events = mock_subscribe

        received = []

        async def cb(event):
            received.append(event)

        with patch("kubemq.pubsub.async_client.asyncio.sleep", new_callable=AsyncMock):
            await client.subscribe_store_with_callback(
                EventsStoreSubscription(channel="ch", on_receive_event_callback=lambda e: None, events_store_type=EventStoreStartPosition.StartFromNew),
                cb,
                cancellation_token=token,
            )

        assert len(received) == 1
        assert call_count[0] == 2

    @pytest.mark.asyncio
    async def test_store_callback_client_closed_error(self, mock_transport):
        """KubeMQClientClosedError -> propagates (line 1455-1456)."""
        from kubemq.core.exceptions import KubeMQClientClosedError

        client = _make_connected_client(mock_transport)

        mock_transport.subscribe_to_events = MagicMock(
            return_value=RaisingAsyncIterator([], KubeMQClientClosedError("closed"))
        )

        with pytest.raises(KubeMQClientClosedError):
            await client.subscribe_store_with_callback(
                EventsStoreSubscription(channel="ch", on_receive_event_callback=lambda e: None, events_store_type=EventStoreStartPosition.StartFromNew),
                AsyncMock(),
            )

    @pytest.mark.asyncio
    async def test_store_callback_kubemq_error_cancelled_returns(self, mock_transport):
        """KubeMQError when token cancelled -> return (line 1459-1460)."""
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()

        retryable = KubeMQConnectionError("lost", is_retryable=True)
        mock_transport.subscribe_to_events = MagicMock(
            return_value=_CancelAndRaiseIterator(token, retryable)
        )

        await client.subscribe_store_with_callback(
            EventsStoreSubscription(channel="ch", on_receive_event_callback=lambda e: None, events_store_type=EventStoreStartPosition.StartFromNew),
            AsyncMock(),
            cancellation_token=token,
        )

    @pytest.mark.asyncio
    async def test_store_callback_concurrent_path(self, mock_transport):
        """Concurrent store callback path with sequence tracking (lines 1409-1441)."""
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()

        pb_ev = MagicMock()
        pb_ev.EventID = "scb-conc"
        pb_ev.Channel = "ch"
        pb_ev.Metadata = ""
        pb_ev.Body = b"data"
        pb_ev.Timestamp = 100
        pb_ev.Sequence = 5
        pb_ev.Tags = {}

        mock_transport.subscribe_to_events = MagicMock(
            return_value=CancellingAsyncIteratorMock([pb_ev], token)
        )

        received = []

        async def cb(event):
            received.append(event)

        await client.subscribe_store_with_callback(
            EventsStoreSubscription(channel="ch", on_receive_event_callback=lambda e: None, events_store_type=EventStoreStartPosition.StartFromNew),
            cb,
            cancellation_token=token,
            max_concurrent_callbacks=3,
        )

        await asyncio.sleep(0.05)
        assert len(received) == 1

    @pytest.mark.asyncio
    async def test_store_callback_concurrent_resume_from_sequence(self, mock_transport):
        """Concurrent store callback resumes from sequence on reconnect (lines 1349-1357)."""
        client = _make_connected_client(mock_transport)
        cancel_token = AsyncCancellationToken()

        pb_ev1 = MagicMock()
        pb_ev1.EventID = "scb-1"
        pb_ev1.Channel = "ch"
        pb_ev1.Metadata = ""
        pb_ev1.Body = b"d1"
        pb_ev1.Timestamp = 100
        pb_ev1.Sequence = 10
        pb_ev1.Tags = {}

        retryable = KubeMQConnectionError("lost", is_retryable=True)

        pb_ev2 = MagicMock()
        pb_ev2.EventID = "scb-2"
        pb_ev2.Channel = "ch"
        pb_ev2.Metadata = ""
        pb_ev2.Body = b"d2"
        pb_ev2.Timestamp = 101
        pb_ev2.Sequence = 11
        pb_ev2.Tags = {}

        call_count = [0]

        def mock_subscribe(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return RaisingAsyncIterator([pb_ev1], retryable)
            return CancellingAsyncIteratorMock([pb_ev2], cancel_token)

        mock_transport.subscribe_to_events = mock_subscribe

        received = []
        errors = []

        async def cb(event):
            received.append(event)

        async def err_cb(error):
            errors.append(error)

        with patch("kubemq.pubsub.async_client.asyncio.sleep", new_callable=AsyncMock):
            await client.subscribe_store_with_callback(
                EventsStoreSubscription(channel="ch", on_receive_event_callback=lambda e: None, events_store_type=EventStoreStartPosition.StartFromNew),
                cb,
                error_callback=err_cb,
                cancellation_token=cancel_token,
            )

        assert len(received) == 2
        assert call_count[0] == 2


# ==============================================================================
# Additional targeted coverage — fast paths, deprecated methods, close branches
# ==============================================================================


class TestAsyncClientPublishEventFast:
    """Cover lines 201-205: publish_event_fast()."""

    @pytest.mark.asyncio
    async def test_publish_event_fast_sends(self, mock_transport):
        """publish_event_fast encodes and sends via bidi sender."""
        client = _make_connected_client(mock_transport)
        msg = EventMessage(channel="ch", body=b"fast-data")

        await client.publish_event_fast(msg)

        client._event_sender.send.assert_called_once()
        pb_event = client._event_sender.send.call_args[0][0]
        assert pb_event.Channel == "ch"
        assert pb_event.Body == b"fast-data"


class TestAsyncClientSendEventStoreFast:
    """Cover lines 207-212: send_event_store_fast()."""

    @pytest.mark.asyncio
    async def test_send_event_store_fast_returns_result(self, mock_transport):
        """send_event_store_fast decodes Result into EventStoreResult."""
        client = _make_connected_client(mock_transport)

        mock_result = pb.Result()
        mock_result.Sent = True
        mock_result.EventID = "fast-store-1"
        mock_result.Error = ""
        client._event_sender.send = AsyncMock(return_value=mock_result)

        msg = EventStoreMessage(channel="ch", body=b"fast-store")
        result = await client.send_event_store_fast(msg)

        assert result.sent is True
        assert result.id == "fast-store-1"

    @pytest.mark.asyncio
    async def test_send_event_store_fast_none_result(self, mock_transport):
        """send_event_store_fast with None result returns empty EventStoreResult."""
        client = _make_connected_client(mock_transport)
        client._event_sender.send = AsyncMock(return_value=None)

        msg = EventStoreMessage(channel="ch", body=b"x")
        result = await client.send_event_store_fast(msg)

        assert result.sent is False


class TestAsyncClientDeprecatedSendEventsStoreMessage:
    """Cover line 392: send_events_store_message() deprecated delegate."""

    @pytest.mark.asyncio
    async def test_send_events_store_message_delegates(self, mock_transport):
        """send_events_store_message delegates to send_event_store."""
        import warnings

        client = _make_connected_client(mock_transport)

        mock_result = pb.Result()
        mock_result.Sent = True
        mock_result.EventID = "deprecated-1"
        mock_result.Error = ""
        client._event_sender.send = AsyncMock(return_value=mock_result)

        msg = EventStoreMessage(channel="ch", body=b"deprecated")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            result = await client.send_events_store_message(msg)

        assert result.sent is True


class TestAsyncClientCloseWithoutEventSender:
    """Cover branch 131->134: close() when _event_sender is None."""

    @pytest.mark.asyncio
    async def test_close_when_no_event_sender(self, mock_transport):
        """close() skips event sender close when None."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True
        assert client._event_sender is None

        with patch.object(type(client).__bases__[0], "close", new_callable=AsyncMock):
            await client.close()

        # No error and event sender still None
        assert client._event_sender is None


class TestSubscribeToEventsStreamEndReconnect:
    """Cover lines 660-667: stream ends without cancel -> reconnect in subscribe_to_events."""

    @pytest.mark.asyncio
    async def test_stream_end_reconnect(self, mock_transport):
        """Stream ends (no cancel) -> reconnects with backoff (lines 657-667)."""
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()
        pb_ev = _make_pb_event(event_id="reconnect-ev")

        call_count = [0]

        def mock_subscribe(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return AsyncIteratorMock([])  # Stream ends immediately
            return CancellingAsyncIteratorMock([pb_ev], token)

        mock_transport.subscribe_to_events = mock_subscribe

        sub = EventsSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
        )

        events = []
        with patch("kubemq.pubsub.async_client.asyncio.sleep", new_callable=AsyncMock):
            async for ev in client.subscribe_to_events(sub, cancellation_token=token):
                events.append(ev)

        assert len(events) == 1
        assert call_count[0] == 2


class TestSubscribeToEventsClientClosedError:
    """Cover line 670: KubeMQClientClosedError in subscribe_to_events."""

    @pytest.mark.asyncio
    async def test_client_closed_error_propagates(self, mock_transport):
        """KubeMQClientClosedError -> re-raised (line 669-670)."""
        from kubemq.core.exceptions import KubeMQClientClosedError

        client = _make_connected_client(mock_transport)

        mock_transport.subscribe_to_events = MagicMock(
            return_value=RaisingAsyncIterator([], KubeMQClientClosedError("closed"))
        )

        sub = EventsSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
        )

        with pytest.raises(KubeMQClientClosedError):
            async for _ in client.subscribe_to_events(sub):
                pass


class TestSubscribeToEventsKubeMQErrorWhenCancelled:
    """Cover line 674: KubeMQError when token is already cancelled."""

    @pytest.mark.asyncio
    async def test_kubemq_error_cancelled_raises(self, mock_transport):
        """KubeMQError raised when token is cancelled -> re-raises (line 673-674)."""
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()

        retryable = KubeMQConnectionError("lost", is_retryable=True)
        mock_transport.subscribe_to_events = MagicMock(
            return_value=_CancelAndRaiseIterator(token, retryable)
        )

        sub = EventsSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
        )

        with pytest.raises(KubeMQConnectionError):
            async for _ in client.subscribe_to_events(sub, cancellation_token=token):
                pass


class TestSubscribeToEventsStoreStreamEndWithEvents:
    """Cover the stream end reconnect in subscribe_to_events_store with events."""

    @pytest.mark.asyncio
    async def test_store_stream_end_with_events(self, mock_transport):
        """Stream yields events then ends -> reconnects (lines 994-1004)."""
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()
        pb_ev1 = _make_pb_event(event_id="se-1")
        pb_ev2 = _make_pb_event(event_id="se-2")

        call_count = [0]

        def mock_subscribe(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return AsyncIteratorMock([pb_ev1])  # Yields one, then stream ends
            return CancellingAsyncIteratorMock([pb_ev2], token)

        mock_transport.subscribe_to_events = mock_subscribe

        sub = EventsStoreSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
            events_store_type=EventStoreStartPosition.StartFromNew,
        )

        events = []
        with patch("kubemq.pubsub.async_client.asyncio.sleep", new_callable=AsyncMock):
            async for ev in client.subscribe_to_events_store(sub, cancellation_token=token):
                events.append(ev)

        assert len(events) == 2
        assert call_count[0] == 2


class TestSubscribeWithCallbackStreamEndConcurrent:
    """Cover lines 1220-1230 in concurrent mode: stream ends -> reconnect."""

    @pytest.mark.asyncio
    async def test_callback_concurrent_stream_end_reconnect(self, mock_transport):
        """Concurrent callback: stream ends -> reconnect (lines 1220-1230)."""
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()
        pb_ev = _make_pb_event(event_id="conc-reconnect")

        call_count = [0]

        def mock_subscribe(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return AsyncIteratorMock([])
            return CancellingAsyncIteratorMock([pb_ev], token)

        mock_transport.subscribe_to_events = mock_subscribe

        received = []

        async def cb(event):
            received.append(event)

        with patch("kubemq.pubsub.async_client.asyncio.sleep", new_callable=AsyncMock):
            await client.subscribe_with_callback(
                EventsSubscription(channel="ch", on_receive_event_callback=lambda e: None),
                cb,
                cancellation_token=token,
                max_concurrent_callbacks=3,
            )

        await asyncio.sleep(0.05)
        assert len(received) == 1
        assert call_count[0] == 2


class TestSubscribeStoreWithCallbackStreamEndConcurrent:
    """Cover lines 1443-1453 in concurrent mode: stream ends -> reconnect."""

    @pytest.mark.asyncio
    async def test_store_callback_concurrent_stream_end_reconnect(self, mock_transport):
        """Concurrent store callback: stream ends -> reconnect (lines 1443-1453)."""
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()
        pb_ev = _make_pb_event(event_id="sconc-reconnect")

        call_count = [0]

        def mock_subscribe(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return AsyncIteratorMock([])
            return CancellingAsyncIteratorMock([pb_ev], token)

        mock_transport.subscribe_to_events = mock_subscribe

        received = []

        async def cb(event):
            received.append(event)

        with patch("kubemq.pubsub.async_client.asyncio.sleep", new_callable=AsyncMock):
            await client.subscribe_store_with_callback(
                EventsStoreSubscription(channel="ch", on_receive_event_callback=lambda e: None, events_store_type=EventStoreStartPosition.StartFromNew),
                cb,
                cancellation_token=token,
                max_concurrent_callbacks=3,
            )

        await asyncio.sleep(0.05)
        assert len(received) == 1
        assert call_count[0] == 2


class TestSubscribeToEventsRetryableWithSyncCallback:
    """Cover lines 690-695: retryable error with sync on_error_callback."""

    @pytest.mark.asyncio
    async def test_retryable_sync_callback(self, mock_transport):
        """Retryable error with sync on_error_callback (lines 690-695)."""
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()
        retryable = KubeMQConnectionError("lost", is_retryable=True)
        pb_ev = _make_pb_event()

        mock_transport.subscribe_to_events = CancellingOneShotRetryIterator(retryable, [pb_ev], token)

        errors = []

        def sync_err_cb(msg):
            errors.append(msg)

        sub = EventsSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
            on_error_callback=sync_err_cb,
        )

        events = []
        with patch("kubemq.pubsub.async_client.asyncio.sleep", new_callable=AsyncMock):
            async for ev in client.subscribe_to_events(sub, cancellation_token=token):
                events.append(ev)

        assert len(events) == 1
        assert len(errors) == 1
        assert "Stream broken" in errors[0]


class TestSubscribeToEventsNonRetryableSyncCallback:
    """Cover line 680-681: non-retryable with sync on_error_callback."""

    @pytest.mark.asyncio
    async def test_non_retryable_sync_callback(self, mock_transport):
        """Non-retryable KubeMQError with sync on_error_callback (lines 676-681)."""
        client = _make_connected_client(mock_transport)

        non_retryable = KubeMQError("fatal sync", is_retryable=False)
        mock_transport.subscribe_to_events = MagicMock(
            return_value=RaisingAsyncIterator([], non_retryable)
        )

        errors = []

        def sync_err_cb(msg):
            errors.append(msg)

        sub = EventsSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
            on_error_callback=sync_err_cb,
        )

        with pytest.raises(KubeMQError, match="fatal sync"):
            async for _ in client.subscribe_to_events(sub):
                pass

        assert len(errors) == 1


class TestSubscribeToEventsGenericExceptionSyncCallback:
    """Cover lines 708-712: generic exception with sync on_error_callback."""

    @pytest.mark.asyncio
    async def test_generic_sync_callback(self, mock_transport):
        """Generic exception with sync on_error_callback (lines 706-712)."""
        client = _make_connected_client(mock_transport)

        mock_transport.subscribe_to_events = MagicMock(
            return_value=RaisingAsyncIterator([], RuntimeError("sync gen err"))
        )

        errors = []

        def sync_err_cb(msg):
            errors.append(msg)

        sub = EventsSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
            on_error_callback=sync_err_cb,
        )

        with pytest.raises(RuntimeError, match="sync gen err"):
            async for _ in client.subscribe_to_events(sub):
                pass

        assert len(errors) == 1


# ==============================================================================
# Cover link-append branch: create_link_from_context returns non-None
# ==============================================================================


class TestSubscribeToEventsLinkBranch:
    """Cover line 595: links.append(link) when create_link_from_context returns non-None."""

    @pytest.mark.asyncio
    async def test_link_appended_in_subscribe_to_events(self, mock_transport):
        """create_link_from_context returns a link -> appended (line 595)."""
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(
            return_value=CancellingAsyncIteratorMock([pb_ev], token)
        )

        sub = EventsSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
        )

        with patch("kubemq.pubsub.async_client.create_link_from_context", return_value=MagicMock()):
            events = []
            async for ev in client.subscribe_to_events(sub, cancellation_token=token):
                events.append(ev)

        assert len(events) == 1


class TestSubscribeToEventsStoreLinkBranch:
    """Cover line 929: links.append(link) when create_link_from_context returns non-None."""

    @pytest.mark.asyncio
    async def test_link_appended_in_subscribe_to_events_store(self, mock_transport):
        """create_link_from_context returns a link -> appended (line 929)."""
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(
            return_value=CancellingAsyncIteratorMock([pb_ev], token)
        )

        sub = EventsStoreSubscription(
            channel="ch",
            on_receive_event_callback=lambda e: None,
            events_store_type=EventStoreStartPosition.StartFromNew,
        )

        with patch("kubemq.pubsub.async_client.create_link_from_context", return_value=MagicMock()):
            events = []
            async for ev in client.subscribe_to_events_store(sub, cancellation_token=token):
                events.append(ev)

        assert len(events) == 1


class TestSubscribeWithCallbackLinkBranch:
    """Cover line 1150: links.append(link) in subscribe_with_callback."""

    @pytest.mark.asyncio
    async def test_link_appended_in_subscribe_with_callback(self, mock_transport):
        """create_link_from_context returns a link -> appended (line 1150)."""
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(
            return_value=CancellingAsyncIteratorMock([pb_ev], token)
        )

        received = []

        async def cb(event):
            received.append(event)

        with patch("kubemq.pubsub.async_client.create_link_from_context", return_value=MagicMock()):
            await client.subscribe_with_callback(
                EventsSubscription(channel="ch", on_receive_event_callback=lambda e: None),
                cb,
                cancellation_token=token,
            )

        assert len(received) == 1


class TestSubscribeStoreWithCallbackLinkBranch:
    """Cover line 1370: links.append(link) in subscribe_store_with_callback."""

    @pytest.mark.asyncio
    async def test_link_appended_in_subscribe_store_with_callback(self, mock_transport):
        """create_link_from_context returns a link -> appended (line 1370)."""
        client = _make_connected_client(mock_transport)
        token = AsyncCancellationToken()
        pb_ev = _make_pb_event()
        mock_transport.subscribe_to_events = MagicMock(
            return_value=CancellingAsyncIteratorMock([pb_ev], token)
        )

        received = []

        async def cb(event):
            received.append(event)

        with patch("kubemq.pubsub.async_client.create_link_from_context", return_value=MagicMock()):
            await client.subscribe_store_with_callback(
                EventsStoreSubscription(channel="ch", on_receive_event_callback=lambda e: None, events_store_type=EventStoreStartPosition.StartFromNew),
                cb,
                cancellation_token=token,
            )

        assert len(received) == 1
