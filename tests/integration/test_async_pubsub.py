"""Integration tests for AsyncPubSubClient.

These tests require a running KubeMQ server on localhost:50000.

Run with:
    uv run pytest tests/integration/test_async_pubsub.py -v -m integration
"""

from __future__ import annotations

import asyncio
import contextlib

import pytest

from kubemq.common.async_cancellation_token import AsyncCancellationToken
from kubemq.pubsub import (
    AsyncClient as AsyncPubSubClient,
    EventMessage,
    EventsStoreSubscription,
    EventsSubscription,
    EventStoreMessage,
)
from kubemq.pubsub.events_store_subscription import EventsStoreType

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration

KUBEMQ_ADDRESS = "localhost:50000"


class TestAsyncPubSubClientConnection:
    """Integration tests for connection handling."""

    @pytest.mark.asyncio
    async def test_connect_and_ping(self, unique_client_id):
        """Test basic connection and ping."""
        async with AsyncPubSubClient(
            address=KUBEMQ_ADDRESS,
            client_id=unique_client_id,
        ) as client:
            server_info = await client.ping()

            assert server_info is not None
            assert server_info.host is not None
            assert server_info.version is not None

    @pytest.mark.asyncio
    async def test_connect_disconnect(self, unique_client_id):
        """Test connection lifecycle."""
        client = AsyncPubSubClient(
            address=KUBEMQ_ADDRESS,
            client_id=unique_client_id,
        )

        # Connect
        await client.connect()
        assert client.is_connected

        # Ping
        server_info = await client.ping()
        assert server_info is not None

        # Disconnect
        await client.close()
        assert not client.is_connected


class TestAsyncPubSubClientEvents:
    """Integration tests for fire-and-forget events."""

    @pytest.mark.asyncio
    async def test_send_single_event(self, unique_channel, unique_client_id):
        """Test sending a single event."""
        async with AsyncPubSubClient(
            address=KUBEMQ_ADDRESS,
            client_id=unique_client_id,
        ) as client:
            message = EventMessage(
                channel=unique_channel,
                body=b"test message",
                metadata="test metadata",
            )

            # Should not raise
            await client.send_event(message)

    @pytest.mark.asyncio
    async def test_send_multiple_events(self, unique_channel, unique_client_id):
        """Test sending multiple events."""
        async with AsyncPubSubClient(
            address=KUBEMQ_ADDRESS,
            client_id=unique_client_id,
        ) as client:
            for i in range(10):
                message = EventMessage(
                    channel=unique_channel,
                    body=f"message {i}".encode(),
                )
                await client.send_event(message)

    @pytest.mark.asyncio
    async def test_send_events_batch(self, unique_channel, unique_client_id):
        """Test batch sending events."""
        async with AsyncPubSubClient(
            address=KUBEMQ_ADDRESS,
            client_id=unique_client_id,
        ) as client:
            messages = [
                EventMessage(
                    channel=unique_channel,
                    body=f"batch message {i}".encode(),
                )
                for i in range(100)
            ]

            results = await client.send_events_batch(messages, max_concurrent=10)

            assert len(results) == 100
            # All should succeed
            assert all(r.sent for r in results)


class TestAsyncPubSubClientEventsStore:
    """Integration tests for persistent events store."""

    @pytest.mark.asyncio
    async def test_send_event_store(self, unique_channel, unique_client_id):
        """Test sending an event to store."""
        async with AsyncPubSubClient(
            address=KUBEMQ_ADDRESS,
            client_id=unique_client_id,
        ) as client:
            message = EventStoreMessage(
                channel=unique_channel,
                body=b"persistent message",
                metadata="test metadata",
            )

            result = await client.send_event_store(message)

            assert result.sent is True

    @pytest.mark.asyncio
    async def test_send_events_store_batch(self, unique_channel, unique_client_id):
        """Test batch sending to events store."""
        async with AsyncPubSubClient(
            address=KUBEMQ_ADDRESS,
            client_id=unique_client_id,
        ) as client:
            messages = [
                EventStoreMessage(
                    channel=unique_channel,
                    body=f"batch store message {i}".encode(),
                )
                for i in range(50)
            ]

            results = await client.send_events_store_batch(messages, max_concurrent=10)

            assert len(results) == 50
            assert all(r.sent for r in results)


class TestAsyncPubSubClientSubscription:
    """Integration tests for event subscriptions."""

    @pytest.mark.asyncio
    async def test_subscribe_to_events(self, unique_channel, unique_client_id):
        """Test subscribing to events."""
        received_events = []
        token = AsyncCancellationToken()

        async with AsyncPubSubClient(
            address=KUBEMQ_ADDRESS,
            client_id=f"{unique_client_id}-sub",
        ) as subscriber:
            # Start subscription in background
            async def subscribe():
                subscription = EventsSubscription(
                    channel=unique_channel,
                    group="test-group",
                    on_receive_event_callback=lambda e: received_events.append(e),
                )
                try:
                    async for event in subscriber.subscribe_to_events(
                        subscription, cancellation_token=token
                    ):
                        received_events.append(event)
                        if len(received_events) >= 5:
                            token.cancel()
                except Exception:
                    pass

            sub_task = asyncio.create_task(subscribe())

            # Wait for subscription to be established on the server
            await asyncio.sleep(2)

            # Send some events
            async with AsyncPubSubClient(
                address=KUBEMQ_ADDRESS,
                client_id=f"{unique_client_id}-pub",
            ) as publisher:
                for i in range(5):
                    await publisher.send_event(
                        EventMessage(
                            channel=unique_channel,
                            body=f"event {i}".encode(),
                        )
                    )
                    await asyncio.sleep(0.1)

            # Wait for events to be received
            await asyncio.sleep(2)
            token.cancel()

            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(sub_task, timeout=2)

        assert len(received_events) >= 1

    @pytest.mark.asyncio
    async def test_subscribe_to_events_store(self, unique_channel, unique_client_id):
        """Test subscribing to events store."""
        # First, send some messages
        async with AsyncPubSubClient(
            address=KUBEMQ_ADDRESS,
            client_id=f"{unique_client_id}-pub",
        ) as publisher:
            for i in range(3):
                await publisher.send_event_store(
                    EventStoreMessage(
                        channel=unique_channel,
                        body=f"store event {i}".encode(),
                    )
                )

        # Wait for messages to be stored
        await asyncio.sleep(0.5)

        received_events = []
        token = AsyncCancellationToken()

        async with AsyncPubSubClient(
            address=KUBEMQ_ADDRESS,
            client_id=f"{unique_client_id}-sub",
        ) as subscriber:

            async def subscribe():
                subscription = EventsStoreSubscription(
                    channel=unique_channel,
                    group="test-group",
                    on_receive_event_callback=lambda e: received_events.append(e),
                    events_store_type=EventsStoreType.StartFromFirst,
                )
                try:
                    async for event in subscriber.subscribe_to_events_store(
                        subscription, cancellation_token=token
                    ):
                        received_events.append(event)
                        if len(received_events) >= 3:
                            token.cancel()
                except Exception:
                    pass

            sub_task = asyncio.create_task(subscribe())

            # Wait for events
            await asyncio.sleep(2)
            token.cancel()

            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(sub_task, timeout=2)

        assert len(received_events) >= 1


class TestAsyncPubSubClientCancellation:
    """Integration tests for cancellation handling."""

    @pytest.mark.asyncio
    async def test_cancel_subscription(self, unique_channel, unique_client_id):
        """Test cancelling a subscription."""
        token = AsyncCancellationToken()
        subscription_ended = False

        async with AsyncPubSubClient(
            address=KUBEMQ_ADDRESS,
            client_id=unique_client_id,
        ) as client:

            async def subscribe():
                nonlocal subscription_ended
                subscription = EventsSubscription(
                    channel=unique_channel,
                    group="test-group",
                    on_receive_event_callback=lambda e: None,
                )
                try:
                    async for _ in client.subscribe_to_events(
                        subscription, cancellation_token=token
                    ):
                        pass
                except Exception:
                    pass
                finally:
                    subscription_ended = True

            sub_task = asyncio.create_task(subscribe())

            # Wait for subscription to start
            await asyncio.sleep(0.5)

            # Cancel
            token.cancel()

            # Wait for subscription to end
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(sub_task, timeout=2)

        assert subscription_ended is True
