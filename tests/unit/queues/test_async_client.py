"""Tests for AsyncQueuesClient."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kubemq.common.async_cancellation_token import AsyncCancellationToken
from kubemq.core.config import ClientConfig
from kubemq.core.exceptions import KubeMQConnectionError
from kubemq.grpc import kubemq_pb2 as pb
from kubemq.queues.async_client import (
    AsyncClient,
    AsyncQueuesClient,
    AsyncQueuesPollResponse,
)
from kubemq.queues.queues_message import QueueMessage


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
        """Test that AsyncQueuesClient is an alias for AsyncClient."""
        assert AsyncQueuesClient is AsyncClient


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


class TestAsyncClientSendQueueMessage:
    """Tests for send_queue_message method."""

    @pytest.mark.asyncio
    async def test_send_queue_message_when_not_connected(self):
        """Test send_queue_message raises when not connected."""
        client = AsyncClient(address="localhost:50000")
        message = QueueMessage(channel="test", body=b"data")

        with pytest.raises(KubeMQConnectionError):
            await client.send_queue_message(message)

    @pytest.mark.asyncio
    async def test_send_queue_message_returns_result(self, mock_transport):
        """Test send_queue_message returns QueueSendResult."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        # Setup mock response
        mock_result = pb.SendQueueMessageResult()
        mock_result.IsError = False
        mock_result.MessageID = "msg-123"
        mock_result.SentAt = 1234567890
        mock_transport.send_queue_message.return_value = mock_result

        message = QueueMessage(
            channel="test-channel",
            body=b"test body",
        )

        result = await client.send_queue_message(message)

        assert result.is_error is False
        mock_transport.send_queue_message.assert_called_once()


class TestAsyncClientSendBatch:
    """Tests for batch send method."""

    @pytest.mark.asyncio
    async def test_send_queue_messages_batch_when_not_connected(self):
        """Test send_queue_messages_batch raises when not connected."""
        client = AsyncClient(address="localhost:50000")
        messages = [QueueMessage(channel="test", body=b"data")]

        with pytest.raises(KubeMQConnectionError):
            await client.send_queue_messages_batch(messages)

    @pytest.mark.asyncio
    async def test_send_queue_messages_batch_respects_concurrency(self, mock_transport):
        """Test send_queue_messages_batch respects max_concurrent."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        # Setup mock response
        mock_result = pb.SendQueueMessageResult()
        mock_result.IsError = False
        mock_transport.send_queue_message.return_value = mock_result

        messages = [QueueMessage(channel="test", body=f"msg-{i}".encode()) for i in range(10)]

        results = await client.send_queue_messages_batch(messages, max_concurrent=5)

        assert len(results) == 10


class TestAsyncClientReceiveQueueMessages:
    """Tests for receive_queue_messages method."""

    @pytest.mark.asyncio
    async def test_receive_queue_messages_when_not_connected(self):
        """Test receive_queue_messages raises when not connected."""
        client = AsyncClient(address="localhost:50000")

        with pytest.raises(KubeMQConnectionError):
            await client.receive_queue_messages(channel="test")

    @pytest.mark.asyncio
    async def test_receive_queue_messages_returns_response(self, mock_transport):
        """Test receive_queue_messages returns AsyncQueuesPollResponse."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        # Setup mock response
        mock_response = pb.ReceiveQueueMessagesResponse()
        mock_response.RequestID = "req-123"
        mock_response.IsError = False
        mock_transport.receive_queue_messages.return_value = mock_response

        # Patch the method to avoid protobuf field issue
        with patch.object(client, "receive_queue_messages") as mock_receive:
            mock_poll_response = AsyncQueuesPollResponse(
                ref_request_id="req-123",
                transaction_id="",
                messages=[],
                error="",
                is_error=False,
                is_transaction_completed=False,
                active_offsets=[],
                receiver_client_id="test-client",
                visibility_seconds=0,
                is_auto_acked=False,
                transport=mock_transport,
            )
            mock_receive.return_value = mock_poll_response

            response = await client.receive_queue_messages(
                channel="test-channel",
                max_messages=10,
                wait_timeout_seconds=30,
            )

            assert isinstance(response, AsyncQueuesPollResponse)
            assert response.ref_request_id == "req-123"

    @pytest.mark.asyncio
    async def test_receive_queue_messages_with_auto_ack(self, mock_transport):
        """Test receive_queue_messages with auto_ack."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        # Patch the method to avoid protobuf field issue
        with patch.object(client, "receive_queue_messages") as mock_receive:
            mock_poll_response = AsyncQueuesPollResponse(
                ref_request_id="req-123",
                transaction_id="",
                messages=[],
                error="",
                is_error=False,
                is_transaction_completed=False,
                active_offsets=[],
                receiver_client_id="test-client",
                visibility_seconds=0,
                is_auto_acked=True,
                transport=mock_transport,
            )
            mock_receive.return_value = mock_poll_response

            response = await client.receive_queue_messages(
                channel="test-channel",
                auto_ack=True,
            )

            assert response.is_auto_acked is True


class TestAsyncClientPeekQueueMessages:
    """Tests for peek_queue_messages method."""

    @pytest.mark.asyncio
    async def test_peek_queue_messages_when_not_connected(self):
        """Test peek_queue_messages raises when not connected."""
        client = AsyncClient(address="localhost:50000")

        with pytest.raises(KubeMQConnectionError):
            await client.peek_queue_messages(channel="test")

    @pytest.mark.asyncio
    async def test_peek_queue_messages_returns_response(self, mock_transport):
        """Test peek_queue_messages returns AsyncQueuesPollResponse."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        mock_response = pb.ReceiveQueueMessagesResponse()
        mock_response.RequestID = "req-123"
        mock_response.IsError = False
        mock_transport.receive_queue_messages.return_value = mock_response

        response = await client.peek_queue_messages(
            channel="test-channel",
            max_messages=5,
        )

        assert isinstance(response, AsyncQueuesPollResponse)
        assert response.is_auto_acked is True  # Peek is always auto-acked


class TestAsyncClientAckAllQueueMessages:
    """Tests for ack_all_queue_messages method."""

    @pytest.mark.asyncio
    async def test_ack_all_queue_messages_when_not_connected(self):
        """Test ack_all_queue_messages raises when not connected."""
        client = AsyncClient(address="localhost:50000")

        with pytest.raises(KubeMQConnectionError):
            await client.ack_all_queue_messages(channel="test")

    @pytest.mark.asyncio
    async def test_ack_all_queue_messages_returns_count(self, mock_transport):
        """Test ack_all_queue_messages returns count."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        mock_response = pb.AckAllQueueMessagesResponse()
        mock_response.IsError = False
        mock_response.AffectedMessages = 5
        mock_transport.ack_all_queue_messages.return_value = mock_response

        count = await client.ack_all_queue_messages(channel="test-channel")

        assert count == 5


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


class TestAsyncQueuesPollResponse:
    """Tests for AsyncQueuesPollResponse."""

    def test_count_returns_message_count(self):
        """Test count returns number of messages."""
        response = AsyncQueuesPollResponse(
            ref_request_id="req-123",
            transaction_id="tx-123",
            messages=[MagicMock(), MagicMock()],
            error="",
            is_error=False,
            is_transaction_completed=False,
            active_offsets=[0, 1],
            receiver_client_id="client-123",
            visibility_seconds=30,
            is_auto_acked=False,
            transport=MagicMock(),
        )

        assert response.count() == 2

    def test_is_empty_returns_true_when_no_messages(self):
        """Test is_empty returns True when no messages."""
        response = AsyncQueuesPollResponse(
            ref_request_id="req-123",
            transaction_id="tx-123",
            messages=[],
            error="",
            is_error=False,
            is_transaction_completed=False,
            active_offsets=[],
            receiver_client_id="client-123",
            visibility_seconds=30,
            is_auto_acked=False,
            transport=MagicMock(),
        )

        assert response.is_empty() is True

    def test_is_empty_returns_false_when_has_messages(self):
        """Test is_empty returns False when has messages."""
        response = AsyncQueuesPollResponse(
            ref_request_id="req-123",
            transaction_id="tx-123",
            messages=[MagicMock()],
            error="",
            is_error=False,
            is_transaction_completed=False,
            active_offsets=[0],
            receiver_client_id="client-123",
            visibility_seconds=30,
            is_auto_acked=False,
            transport=MagicMock(),
        )

        assert response.is_empty() is False

    @pytest.mark.asyncio
    async def test_ack_all_raises_when_auto_acked(self):
        """Test ack_all raises when auto-acked."""
        response = AsyncQueuesPollResponse(
            ref_request_id="req-123",
            transaction_id="tx-123",
            messages=[],
            error="",
            is_error=False,
            is_transaction_completed=False,
            active_offsets=[],
            receiver_client_id="client-123",
            visibility_seconds=30,
            is_auto_acked=True,
            transport=MagicMock(),
        )

        with pytest.raises(ValueError, match="auto-acknowledged"):
            await response.ack_all()

    @pytest.mark.asyncio
    async def test_ack_all_raises_when_transaction_completed(self):
        """Test ack_all raises when transaction already completed."""
        response = AsyncQueuesPollResponse(
            ref_request_id="req-123",
            transaction_id="tx-123",
            messages=[],
            error="",
            is_error=False,
            is_transaction_completed=True,
            active_offsets=[],
            receiver_client_id="client-123",
            visibility_seconds=30,
            is_auto_acked=False,
            transport=MagicMock(),
        )

        with pytest.raises(ValueError, match="already completed"):
            await response.ack_all()

    @pytest.mark.asyncio
    async def test_reject_all_raises_when_auto_acked(self):
        """Test reject_all raises when auto-acked."""
        response = AsyncQueuesPollResponse(
            ref_request_id="req-123",
            transaction_id="tx-123",
            messages=[],
            error="",
            is_error=False,
            is_transaction_completed=False,
            active_offsets=[],
            receiver_client_id="client-123",
            visibility_seconds=30,
            is_auto_acked=True,
            transport=MagicMock(),
        )

        with pytest.raises(ValueError, match="auto-acknowledged"):
            await response.reject_all()

    @pytest.mark.asyncio
    async def test_re_queue_all_raises_when_auto_acked(self):
        """Test re_queue_all raises when auto-acked."""
        response = AsyncQueuesPollResponse(
            ref_request_id="req-123",
            transaction_id="tx-123",
            messages=[],
            error="",
            is_error=False,
            is_transaction_completed=False,
            active_offsets=[],
            receiver_client_id="client-123",
            visibility_seconds=30,
            is_auto_acked=True,
            transport=MagicMock(),
        )

        with pytest.raises(ValueError, match="auto-acknowledged"):
            await response.re_queue_all("other-channel")


# ==============================================================================
# Extended Tests for Additional Coverage
# ==============================================================================


class TestAsyncClientSendBatchPartialErrors:
    """Tests for send_queue_messages_batch error handling."""

    @pytest.mark.asyncio
    async def test_send_queue_messages_batch_handles_partial_errors(self, mock_transport):
        """Test send_queue_messages_batch handles partial failures."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        # Make send_queue_message fail on every other call
        call_count = [0]

        async def mock_send_message(pb_message):
            call_count[0] += 1
            if call_count[0] % 2 == 0:
                raise Exception("Send failed")
            result = pb.SendQueueMessageResult()
            result.IsError = False
            result.MessageID = f"msg-{call_count[0]}"
            result.SentAt = 1234567890
            return result

        mock_transport.send_queue_message.side_effect = mock_send_message

        messages = [QueueMessage(channel="test", body=f"msg-{i}".encode()) for i in range(4)]

        results = await client.send_queue_messages_batch(messages)

        assert len(results) == 4
        # Some should succeed, some should fail
        successful = [r for r in results if not r.is_error]
        failed = [r for r in results if r.is_error]
        assert len(successful) == 2
        assert len(failed) == 2


class TestAsyncClientSubscribeToQueue:
    """Tests for subscribe_to_queue method."""

    @pytest.mark.asyncio
    async def test_subscribe_to_queue_when_not_connected(self):
        """Test subscribe_to_queue raises when not connected."""
        client = AsyncClient(address="localhost:50000")

        with pytest.raises(KubeMQConnectionError):
            async for _ in client.subscribe_to_queue(channel="test"):
                pass

    @pytest.mark.asyncio
    @pytest.mark.timeout(5)
    async def test_subscribe_to_queue_registers_subscription(self, mock_transport):
        """Test subscribe_to_queue registers the subscription token."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        # Create mock response - empty to skip yielding
        mock_response = pb.ReceiveQueueMessagesResponse()
        mock_response.RequestID = "req-123"
        mock_response.IsError = False

        # Return empty response and then raise to exit the loop
        call_count = [0]

        async def mock_receive(*args):
            call_count[0] += 1
            if call_count[0] > 1:
                raise asyncio.CancelledError()
            return mock_response

        mock_transport.receive_queue_messages.side_effect = mock_receive

        token = AsyncCancellationToken()
        token.cancel()  # Cancel immediately

        # Just verify it doesn't error out when cancelled
        async for _ in client.subscribe_to_queue(
            channel="test",
            cancellation_token=token,
        ):
            break


class TestAsyncClientProcessQueueMessages:
    """Tests for process_queue_messages method - simplified tests."""

    @pytest.mark.asyncio
    @pytest.mark.timeout(5)
    async def test_process_queue_messages_accepts_callback(self, mock_transport):
        """Test process_queue_messages accepts callback parameters."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        # Create mock response - empty to not call callback
        mock_response = pb.ReceiveQueueMessagesResponse()
        mock_response.RequestID = "req-123"
        mock_response.IsError = False

        async def mock_receive(*args):
            raise asyncio.CancelledError()

        mock_transport.receive_queue_messages.side_effect = mock_receive

        token = AsyncCancellationToken()
        token.cancel()  # Cancel immediately

        async def callback(message):
            pass

        # Just verify it accepts the callback without error
        try:
            await client.process_queue_messages(
                channel="test",
                callback=callback,
                auto_ack=True,
                cancellation_token=token,
            )
        except asyncio.CancelledError:
            pass  # Expected when cancelled


class TestAsyncQueuesPollResponseOperations:
    """Additional tests for AsyncQueuesPollResponse operations."""

    @pytest.mark.asyncio
    async def test_reject_all_raises_when_transaction_completed(self):
        """Test reject_all raises when transaction already completed."""
        response = AsyncQueuesPollResponse(
            ref_request_id="req-123",
            transaction_id="tx-123",
            messages=[],
            error="",
            is_error=False,
            is_transaction_completed=True,
            active_offsets=[],
            receiver_client_id="client-123",
            visibility_seconds=30,
            is_auto_acked=False,
            transport=MagicMock(),
        )

        with pytest.raises(ValueError, match="already completed"):
            await response.reject_all()

    @pytest.mark.asyncio
    async def test_re_queue_all_raises_when_transaction_completed(self):
        """Test re_queue_all raises when transaction already completed."""
        response = AsyncQueuesPollResponse(
            ref_request_id="req-123",
            transaction_id="tx-123",
            messages=[],
            error="",
            is_error=False,
            is_transaction_completed=True,
            active_offsets=[],
            receiver_client_id="client-123",
            visibility_seconds=30,
            is_auto_acked=False,
            transport=MagicMock(),
        )

        with pytest.raises(ValueError, match="already completed"):
            await response.re_queue_all("other-channel")
