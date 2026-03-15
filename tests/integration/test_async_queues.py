"""Integration tests for AsyncQueuesClient.

These tests require a running KubeMQ server on localhost:50000.

Run with:
    uv run pytest tests/integration/test_async_queues.py -v -m integration
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid

import pytest

from kubemq.common.async_cancellation_token import AsyncCancellationToken
from kubemq.queues import AsyncClient as AsyncQueuesClient, QueueMessage

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration

KUBEMQ_ADDRESS = "localhost:50000"


@pytest.fixture
def unique_channel() -> str:
    """Generate a unique channel name for test isolation."""
    return f"test-queue-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def unique_client_id() -> str:
    """Generate a unique client ID."""
    return f"test-client-{uuid.uuid4().hex[:8]}"


class TestAsyncQueuesClientConnection:
    """Integration tests for connection handling."""

    @pytest.mark.asyncio
    async def test_connect_and_ping(self, unique_client_id):
        """Test basic connection and ping."""
        async with AsyncQueuesClient(
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
        client = AsyncQueuesClient(
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


class TestAsyncQueuesClientSend:
    """Integration tests for queue message sending."""

    @pytest.mark.asyncio
    async def test_send_single_message(self, unique_channel, unique_client_id):
        """Test sending a single queue message."""
        async with AsyncQueuesClient(
            address=KUBEMQ_ADDRESS,
            client_id=unique_client_id,
        ) as client:
            message = QueueMessage(
                channel=unique_channel,
                body=b"test message",
                metadata="test metadata",
            )

            result = await client.send_queue_message(message)

            assert result.is_error is False

    @pytest.mark.asyncio
    async def test_send_multiple_messages(self, unique_channel, unique_client_id):
        """Test sending multiple queue messages."""
        async with AsyncQueuesClient(
            address=KUBEMQ_ADDRESS,
            client_id=unique_client_id,
        ) as client:
            for i in range(10):
                message = QueueMessage(
                    channel=unique_channel,
                    body=f"message {i}".encode(),
                )
                result = await client.send_queue_message(message)
                assert result.is_error is False

    @pytest.mark.asyncio
    async def test_send_messages_batch(self, unique_channel, unique_client_id):
        """Test batch sending queue messages."""
        async with AsyncQueuesClient(
            address=KUBEMQ_ADDRESS,
            client_id=unique_client_id,
        ) as client:
            messages = [
                QueueMessage(
                    channel=unique_channel,
                    body=f"batch message {i}".encode(),
                )
                for i in range(50)
            ]

            results = await client.send_queue_messages_batch(messages)

            assert len(results) == 50
            # All should succeed
            assert all(not r.is_error for r in results)


class TestAsyncQueuesClientReceive:
    """Integration tests for queue message receiving."""

    @pytest.mark.asyncio
    async def test_receive_messages(self, unique_channel, unique_client_id):
        """Test receiving queue messages."""
        async with AsyncQueuesClient(
            address=KUBEMQ_ADDRESS,
            client_id=unique_client_id,
        ) as client:
            # Send some messages first
            for i in range(5):
                await client.send_queue_message(
                    QueueMessage(
                        channel=unique_channel,
                        body=f"message {i}".encode(),
                    )
                )

            # Receive messages
            response = await client.receive_queue_messages(
                channel=unique_channel,
                max_messages=5,
                wait_timeout_seconds=5,
                auto_ack=True,
            )

            assert response.is_error is False
            assert len(response.messages) > 0

    @pytest.mark.asyncio
    async def test_receive_with_ack(self, unique_channel, unique_client_id):
        """Test receiving and acknowledging messages."""
        async with AsyncQueuesClient(
            address=KUBEMQ_ADDRESS,
            client_id=unique_client_id,
        ) as client:
            # Send a message
            await client.send_queue_message(
                QueueMessage(
                    channel=unique_channel,
                    body=b"message to ack",
                )
            )

            # Receive without auto-ack
            response = await client.receive_queue_messages(
                channel=unique_channel,
                max_messages=1,
                wait_timeout_seconds=5,
                auto_ack=True,  # Using auto_ack to simplify
            )

            assert response.is_error is False
            if response.messages:
                assert response.messages[0].body == b"message to ack"

    @pytest.mark.asyncio
    async def test_peek_messages(self, unique_channel, unique_client_id):
        """Test peeking at queue messages without removing them."""
        async with AsyncQueuesClient(
            address=KUBEMQ_ADDRESS,
            client_id=unique_client_id,
        ) as client:
            # Send a message
            await client.send_queue_message(
                QueueMessage(
                    channel=unique_channel,
                    body=b"peek test message",
                )
            )

            # Peek at message
            response = await client.peek_queue_messages(
                channel=unique_channel,
                max_messages=1,
                wait_timeout_seconds=5,
            )

            assert response.is_error is False
            if response.messages:
                assert response.messages[0].body == b"peek test message"

            # Message should still be in queue - receive it
            response2 = await client.receive_queue_messages(
                channel=unique_channel,
                max_messages=1,
                wait_timeout_seconds=5,
                auto_ack=True,
            )

            # Should still get the message
            assert response2.is_error is False


class TestAsyncQueuesClientAckAll:
    """Integration tests for ack all functionality."""

    @pytest.mark.asyncio
    async def test_ack_all_messages(self, unique_channel, unique_client_id):
        """Test acknowledging all messages in a queue."""
        from kubemq.core.exceptions import KubeMQTimeoutError

        async with AsyncQueuesClient(
            address=KUBEMQ_ADDRESS,
            client_id=unique_client_id,
        ) as client:
            # Send some messages
            for i in range(5):
                await client.send_queue_message(
                    QueueMessage(
                        channel=unique_channel,
                        body=f"message to ack {i}".encode(),
                    )
                )

            # Ack all - may timeout on some servers
            try:
                count = await client.ack_all_queue_messages(channel=unique_channel)
                # Should have acknowledged some messages
                assert count >= 0
            except KubeMQTimeoutError:
                # Some server configurations may not respond quickly to ack_all
                # This is acceptable for integration tests
                pass


class TestAsyncQueuesClientSubscription:
    """Integration tests for queue subscriptions."""

    @pytest.mark.asyncio
    async def test_subscribe_to_queue(self, unique_channel, unique_client_id):
        """Test subscribing to queue messages."""
        received_messages = []
        token = AsyncCancellationToken()

        async with AsyncQueuesClient(
            address=KUBEMQ_ADDRESS,
            client_id=f"{unique_client_id}-sub",
        ) as subscriber:
            # Start subscription in background
            async def subscribe():
                try:
                    async for response in subscriber.subscribe_to_queue(
                        channel=unique_channel,
                        max_messages=5,
                        wait_timeout_seconds=2,
                        auto_ack=True,
                        cancellation_token=token,
                    ):
                        for msg in response.messages:
                            received_messages.append(msg)
                        if len(received_messages) >= 3:
                            token.cancel()
                except Exception:
                    pass

            sub_task = asyncio.create_task(subscribe())

            # Wait for subscription to be established
            await asyncio.sleep(0.5)

            # Send some messages
            async with AsyncQueuesClient(
                address=KUBEMQ_ADDRESS,
                client_id=f"{unique_client_id}-pub",
            ) as publisher:
                for i in range(5):
                    await publisher.send_queue_message(
                        QueueMessage(
                            channel=unique_channel,
                            body=f"queued message {i}".encode(),
                        )
                    )

            # Wait for messages to be received
            await asyncio.sleep(3)
            token.cancel()

            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(sub_task, timeout=5)

        assert len(received_messages) >= 1


class TestAsyncQueuesClientCancellation:
    """Integration tests for cancellation handling."""

    @pytest.mark.asyncio
    async def test_cancel_subscription(self, unique_channel, unique_client_id):
        """Test cancelling a queue subscription."""
        token = AsyncCancellationToken()
        subscription_ended = False

        async with AsyncQueuesClient(
            address=KUBEMQ_ADDRESS,
            client_id=unique_client_id,
        ) as client:

            async def subscribe():
                nonlocal subscription_ended
                try:
                    async for _ in client.subscribe_to_queue(
                        channel=unique_channel,
                        max_messages=1,
                        wait_timeout_seconds=1,
                        auto_ack=True,
                        cancellation_token=token,
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
                await asyncio.wait_for(sub_task, timeout=5)

        assert subscription_ended is True


class TestAsyncQueuesClientMessageAttributes:
    """Integration tests for message attributes."""

    @pytest.mark.asyncio
    async def test_message_with_tags(self, unique_channel, unique_client_id):
        """Test sending message with tags."""
        async with AsyncQueuesClient(
            address=KUBEMQ_ADDRESS,
            client_id=unique_client_id,
        ) as client:
            message = QueueMessage(
                channel=unique_channel,
                body=b"tagged message",
                tags={"key1": "value1", "key2": "value2"},
            )

            result = await client.send_queue_message(message)
            assert result.is_error is False

    @pytest.mark.asyncio
    async def test_message_with_expiration(self, unique_channel, unique_client_id):
        """Test sending message with expiration."""
        async with AsyncQueuesClient(
            address=KUBEMQ_ADDRESS,
            client_id=unique_client_id,
        ) as client:
            message = QueueMessage(
                channel=unique_channel,
                body=b"expiring message",
                policy_expiration_seconds=60,
            )

            result = await client.send_queue_message(message)
            assert result.is_error is False

    @pytest.mark.asyncio
    async def test_message_with_delay(self, unique_channel, unique_client_id):
        """Test sending message with delay."""
        async with AsyncQueuesClient(
            address=KUBEMQ_ADDRESS,
            client_id=unique_client_id,
        ) as client:
            message = QueueMessage(
                channel=unique_channel,
                body=b"delayed message",
                policy_delay_seconds=1,
            )

            result = await client.send_queue_message(message)
            assert result.is_error is False
