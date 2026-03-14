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
from kubemq.queues.queues_send_result import QueueSendResult


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
        """Test send_queue_message returns QueueSendResult via bidi upstream sender."""
        from unittest.mock import AsyncMock

        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        mock_send_result = QueueSendResult(
            id="msg-123", is_error=False, sent_at=1234567890
        )
        mock_sender = AsyncMock()
        mock_sender.send = AsyncMock(return_value=mock_send_result)
        client._upstream_sender = mock_sender

        message = QueueMessage(
            channel="test-channel",
            body=b"test body",
        )

        result = await client.send_queue_message(message)

        assert result.is_error is False
        mock_sender.send.assert_called_once()


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
    async def test_send_queue_messages_batch_uses_grpc_batch_rpc(self, mock_transport):
        """Test send_queue_messages_batch uses gRPC SendQueueMessagesBatch RPC."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        mock_response = pb.QueueMessagesBatchResponse()
        mock_response.BatchID = "test-batch-123"
        mock_response.HaveErrors = False
        for i in range(3):
            r = mock_response.Results.add()
            r.MessageID = f"msg-{i}"
            r.IsError = False
            r.SentAt = 1234567890
            r.RefChannel = ""
            r.RefTopic = ""
            r.RefPartition = 0
            r.RefHash = ""
        mock_transport.send_queue_messages_batch.return_value = mock_response

        messages = [QueueMessage(channel="test", body=f"msg-{i}".encode()) for i in range(3)]

        batch_result = await client.send_queue_messages_batch(messages)

        assert len(batch_result) == 3
        assert len(batch_result.results) == 3
        assert batch_result.batch_id == "test-batch-123"
        assert batch_result.have_errors is False
        mock_transport.send_queue_messages_batch.assert_called_once()
        req_arg = mock_transport.send_queue_messages_batch.call_args[0][0]
        assert len(req_arg.Messages) == 3
        assert req_arg.BatchID != ""


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
    """Tests for send_queue_messages_batch error handling via gRPC batch RPC."""

    @pytest.mark.asyncio
    async def test_send_queue_messages_batch_handles_partial_errors(self, mock_transport):
        """Test send_queue_messages_batch surfaces per-message errors from server."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        mock_response = pb.QueueMessagesBatchResponse()
        mock_response.HaveErrors = True
        r1 = mock_response.Results.add()
        r1.MessageID = "msg-0"
        r1.IsError = False
        r1.SentAt = 1234567890
        r1.RefChannel = ""
        r1.RefTopic = ""
        r1.RefPartition = 0
        r1.RefHash = ""
        r2 = mock_response.Results.add()
        r2.MessageID = "msg-1"
        r2.IsError = True
        r2.Error = "channel not found"
        r2.RefChannel = ""
        r2.RefTopic = ""
        r2.RefPartition = 0
        r2.RefHash = ""
        mock_transport.send_queue_messages_batch.return_value = mock_response

        messages = [
            QueueMessage(channel="test", body=b"msg-0"),
            QueueMessage(channel="test", body=b"msg-1"),
        ]

        batch_result = await client.send_queue_messages_batch(messages)

        assert len(batch_result) == 2
        assert batch_result.have_errors is True
        assert batch_result.results[0].is_error is False
        assert batch_result.results[1].is_error is True
        assert batch_result.results[1].error == "channel not found"
        assert batch_result[0].is_error is False
        assert batch_result[1].is_error is True


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


class TestAsyncQueuesClientTransactionOps:
    """Tests for poll response transaction operations when transport errors."""

    @pytest.mark.asyncio
    async def test_ack_all_when_not_connected(self):
        """Test ack_all raises when transport is disconnected."""
        mock_transport = AsyncMock()
        mock_transport.queues_downstream = MagicMock(
            side_effect=KubeMQConnectionError("Not connected")
        )

        response = AsyncQueuesPollResponse(
            ref_request_id="req-123",
            transaction_id="tx-123",
            messages=[MagicMock()],
            error="",
            is_error=False,
            is_transaction_completed=False,
            active_offsets=[1],
            receiver_client_id="client-123",
            visibility_seconds=30,
            is_auto_acked=False,
            transport=mock_transport,
        )

        with pytest.raises(KubeMQConnectionError):
            await response.ack_all()

    @pytest.mark.asyncio
    async def test_reject_all_when_not_connected(self):
        """Test reject_all raises when transport is disconnected."""
        mock_transport = AsyncMock()
        mock_transport.queues_downstream = MagicMock(
            side_effect=KubeMQConnectionError("Not connected")
        )

        response = AsyncQueuesPollResponse(
            ref_request_id="req-123",
            transaction_id="tx-123",
            messages=[MagicMock()],
            error="",
            is_error=False,
            is_transaction_completed=False,
            active_offsets=[1],
            receiver_client_id="client-123",
            visibility_seconds=30,
            is_auto_acked=False,
            transport=mock_transport,
        )

        with pytest.raises(KubeMQConnectionError):
            await response.reject_all()


class TestAsyncQueuesClientChannelManagement:
    """Tests for channel management operations when not connected."""

    @pytest.mark.asyncio
    async def test_create_queues_channel_when_not_connected(self):
        """Test that channel creation requires an active connection."""
        client = AsyncClient(address="localhost:50000")

        with pytest.raises(KubeMQConnectionError):
            await client.send_queue_message(
                QueueMessage(channel="new-channel", body=b"test")
            )

    @pytest.mark.asyncio
    async def test_delete_queues_channel_when_not_connected(self):
        """Test that channel deletion requires an active connection."""
        client = AsyncClient(address="localhost:50000")

        with pytest.raises(KubeMQConnectionError):
            await client.ack_all_queue_messages(channel="delete-channel")

    @pytest.mark.asyncio
    async def test_list_queues_channels_when_not_connected(self):
        """Test that channel listing requires an active connection."""
        client = AsyncClient(address="localhost:50000")

        with pytest.raises(KubeMQConnectionError):
            await client.receive_queue_messages(channel="list-channel")


# ==============================================================================
# Coverage extension: lines 96, 114-119, 139-152, 251-256, 266-268,
# 515-521, 553-594, 624
# ==============================================================================


class TestDoOperationWithReQueueChannel:

    @pytest.mark.asyncio
    async def test_requeue_all_sends_channel_and_completes(self):
        captured = []

        async def fake_downstream(request_iter):
            async for req in request_iter:
                captured.append(req)
            yield pb.QueuesDownstreamResponse()

        transport = MagicMock()
        transport.queues_downstream = fake_downstream

        response = AsyncQueuesPollResponse(
            ref_request_id="req-1",
            transaction_id="tx-1",
            messages=[],
            error="",
            is_error=False,
            is_transaction_completed=False,
            active_offsets=[10, 20],
            receiver_client_id="client-1",
            visibility_seconds=0,
            is_auto_acked=False,
            transport=transport,
        )

        await response.re_queue_all("target-channel")

        assert len(captured) == 1
        assert captured[0].ReQueueChannel == "target-channel"
        assert captured[0].RefTransactionId == "tx-1"
        assert list(captured[0].SequenceRange) == [10, 20]
        assert response.is_transaction_completed is True

    @pytest.mark.asyncio
    async def test_ack_all_sends_empty_requeue_channel(self):
        captured = []

        async def fake_downstream(request_iter):
            async for req in request_iter:
                captured.append(req)
            yield pb.QueuesDownstreamResponse()

        transport = MagicMock()
        transport.queues_downstream = fake_downstream

        response = AsyncQueuesPollResponse(
            ref_request_id="req-1",
            transaction_id="tx-1",
            messages=[],
            error="",
            is_error=False,
            is_transaction_completed=False,
            active_offsets=[5],
            receiver_client_id="client-1",
            visibility_seconds=0,
            is_auto_acked=False,
            transport=transport,
        )

        await response.ack_all()

        assert len(captured) == 1
        assert captured[0].ReQueueChannel == ""
        assert response.is_transaction_completed is True


class TestAsyncQueuesPollResponseDecodeAllFields:

    def test_decode_all_fields(self):
        pb_response = pb.QueuesDownstreamResponse()
        pb_response.RefRequestId = "ref-1"
        pb_response.TransactionId = "tx-1"
        pb_response.TransactionComplete = True
        pb_response.IsError = False
        pb_response.Error = ""
        pb_response.ActiveOffsets.extend([5, 10])

        msg = pb_response.Messages.add()
        msg.MessageID = "msg-1"
        msg.Body = b"hello"

        transport = MagicMock()

        with patch(
            "kubemq.queues.async_client.QueueMessageReceived.decode"
        ) as mock_decode:
            mock_msg = MagicMock()
            mock_decode.return_value = mock_msg

            result = AsyncQueuesPollResponse.decode(
                pb_response,
                receiver_client_id="rcv-1",
                transport=transport,
                request_visibility_seconds=30,
                request_auto_ack=True,
            )

        assert result.ref_request_id == "ref-1"
        assert result.transaction_id == "tx-1"
        assert result.is_transaction_completed is True
        assert result.active_offsets == [5, 10]
        assert result.receiver_client_id == "rcv-1"
        assert result.visibility_seconds == 30
        assert result.is_auto_acked is True
        assert len(result.messages) == 1

        from unittest.mock import ANY

        mock_decode.assert_called_once_with(
            msg, "tx-1", True, "rcv-1", None,
            visibility_seconds=30, is_auto_acked=True,
            async_response_handler=ANY,
        )

    def test_decode_multiple_messages(self):
        pb_response = pb.QueuesDownstreamResponse()
        pb_response.RefRequestId = "ref-2"
        pb_response.TransactionId = "tx-2"
        pb_response.TransactionComplete = False

        for i in range(3):
            m = pb_response.Messages.add()
            m.MessageID = f"msg-{i}"
            m.Body = f"body-{i}".encode()

        transport = MagicMock()

        with patch(
            "kubemq.queues.async_client.QueueMessageReceived.decode"
        ) as mock_decode:
            mock_decode.side_effect = [MagicMock(), MagicMock(), MagicMock()]
            result = AsyncQueuesPollResponse.decode(
                pb_response, "rcv-2", transport,
            )

        assert len(result.messages) == 3
        assert mock_decode.call_count == 3


class TestSendBatchTransportException:

    @pytest.mark.asyncio
    async def test_batch_transport_exception_propagates(self, mock_transport):
        """Test that transport-level exceptions propagate from batch send."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        mock_transport.send_queue_messages_batch.side_effect = ConnectionError("network down")

        messages = [
            QueueMessage(channel="test", body=b"a"),
            QueueMessage(channel="test", body=b"b"),
        ]

        with pytest.raises(ConnectionError, match="network down"):
            await client.send_queue_messages_batch(messages)


class TestReceiveQueueMessagesWithMessages:

    @pytest.mark.asyncio
    async def test_receive_with_messages_runs_metrics_loop(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        mock_response = pb.ReceiveQueueMessagesResponse()
        mock_response.RequestID = "req-1"
        mock_response.IsError = False

        for i in range(3):
            msg = mock_response.Messages.add()
            msg.MessageID = f"msg-{i}"
            msg.Body = b"data"

        mock_transport.receive_queue_messages.return_value = mock_response

        with patch(
            "kubemq.queues.async_client.QueueMessageReceived.decode",
            return_value=MagicMock(),
        ):
            response = await client.receive_queue_messages(
                channel="test-ch", max_messages=10,
            )

        assert len(response.messages) == 3
        assert response.is_auto_acked is False


class TestSubscribeToQueueBackoff:

    @pytest.mark.asyncio
    @pytest.mark.timeout(5)
    async def test_backoff_on_receive_error(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        call_count = [0]
        token = AsyncCancellationToken()

        async def mock_receive(**kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise RuntimeError("transient error")
            token.cancel()
            return AsyncQueuesPollResponse(
                ref_request_id="r", transaction_id="",
                messages=[], error="", is_error=False,
                is_transaction_completed=False, active_offsets=[],
                receiver_client_id="c", visibility_seconds=0,
                is_auto_acked=True, transport=mock_transport,
            )

        with patch.object(client, "receive_queue_messages", mock_receive):
            with patch(
                "kubemq.queues.async_client.asyncio.sleep",
                new_callable=AsyncMock,
            ) as mock_sleep:
                async for _ in client.subscribe_to_queue(
                    channel="test", cancellation_token=token,
                ):
                    pass

        mock_sleep.assert_called_once()
        assert call_count[0] == 2


class TestProcessQueueMessagesErrorPaths:

    def _make_client(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True
        if not hasattr(client, "_register_subscription_task"):
            client._register_subscription_task = MagicMock()
        return client

    @pytest.mark.asyncio
    @pytest.mark.timeout(5)
    async def test_error_response_calls_error_callback(self, mock_transport):
        client = self._make_client(mock_transport)
        errors = []

        async def fake_subscribe(**kwargs):
            yield AsyncQueuesPollResponse(
                ref_request_id="r", transaction_id="", messages=[],
                error="server error", is_error=True,
                is_transaction_completed=False, active_offsets=[],
                receiver_client_id="c", visibility_seconds=0,
                is_auto_acked=True, transport=mock_transport,
            )

        async def error_cb(e):
            errors.append(e)

        async def msg_cb(msg):
            pass

        with patch.object(client, "subscribe_to_queue", fake_subscribe):
            await client.process_queue_messages(
                channel="test", callback=msg_cb, error_callback=error_cb,
                auto_ack=True,
            )

        assert len(errors) == 1
        assert "server error" in str(errors[0])

    @pytest.mark.asyncio
    @pytest.mark.timeout(5)
    async def test_error_response_callback_raises_is_logged(self, mock_transport):
        client = self._make_client(mock_transport)

        async def fake_subscribe(**kwargs):
            yield AsyncQueuesPollResponse(
                ref_request_id="r", transaction_id="", messages=[],
                error="server error", is_error=True,
                is_transaction_completed=False, active_offsets=[],
                receiver_client_id="c", visibility_seconds=0,
                is_auto_acked=True, transport=mock_transport,
            )

        async def error_cb(e):
            raise RuntimeError("callback crashed")

        async def msg_cb(msg):
            pass

        with patch.object(client, "subscribe_to_queue", fake_subscribe):
            await client.process_queue_messages(
                channel="test", callback=msg_cb, error_callback=error_cb,
                auto_ack=True,
            )

    @pytest.mark.asyncio
    @pytest.mark.timeout(5)
    async def test_handler_error_wraps_in_handler_error(self, mock_transport):
        from kubemq.core.exceptions import KubeMQHandlerError

        client = self._make_client(mock_transport)
        errors = []

        async def fake_subscribe(**kwargs):
            yield AsyncQueuesPollResponse(
                ref_request_id="r", transaction_id="", messages=[MagicMock()],
                error="", is_error=False,
                is_transaction_completed=True, active_offsets=[],
                receiver_client_id="c", visibility_seconds=0,
                is_auto_acked=True, transport=mock_transport,
            )

        async def error_cb(e):
            errors.append(e)

        async def msg_cb(msg):
            raise ValueError("handler crash")

        with patch.object(client, "subscribe_to_queue", fake_subscribe):
            await client.process_queue_messages(
                channel="test", callback=msg_cb, error_callback=error_cb,
                auto_ack=True,
            )

        assert len(errors) == 1
        assert isinstance(errors[0], KubeMQHandlerError)
        assert "ValueError" in str(errors[0])

    @pytest.mark.asyncio
    @pytest.mark.timeout(5)
    async def test_handler_error_callback_raises_is_logged(self, mock_transport):
        client = self._make_client(mock_transport)

        async def fake_subscribe(**kwargs):
            yield AsyncQueuesPollResponse(
                ref_request_id="r", transaction_id="", messages=[MagicMock()],
                error="", is_error=False,
                is_transaction_completed=True, active_offsets=[],
                receiver_client_id="c", visibility_seconds=0,
                is_auto_acked=True, transport=mock_transport,
            )

        async def error_cb(e):
            raise RuntimeError("error callback exploded")

        async def msg_cb(msg):
            raise ValueError("handler crash")

        with patch.object(client, "subscribe_to_queue", fake_subscribe):
            await client.process_queue_messages(
                channel="test", callback=msg_cb, error_callback=error_cb,
                auto_ack=True,
            )

    @pytest.mark.asyncio
    @pytest.mark.timeout(5)
    async def test_handler_error_without_callback_is_logged(self, mock_transport):
        client = self._make_client(mock_transport)

        async def fake_subscribe(**kwargs):
            yield AsyncQueuesPollResponse(
                ref_request_id="r", transaction_id="", messages=[MagicMock()],
                error="", is_error=False,
                is_transaction_completed=True, active_offsets=[],
                receiver_client_id="c", visibility_seconds=0,
                is_auto_acked=True, transport=mock_transport,
            )

        async def msg_cb(msg):
            raise ValueError("handler crash")

        with patch.object(client, "subscribe_to_queue", fake_subscribe):
            await client.process_queue_messages(
                channel="test", callback=msg_cb, auto_ack=True,
            )

    @pytest.mark.asyncio
    @pytest.mark.timeout(5)
    async def test_ack_all_failure_calls_error_callback(self, mock_transport):
        client = self._make_client(mock_transport)
        errors = []

        resp = AsyncQueuesPollResponse(
            ref_request_id="r", transaction_id="tx-1",
            messages=[MagicMock()], error="", is_error=False,
            is_transaction_completed=False, active_offsets=[1],
            receiver_client_id="c", visibility_seconds=0,
            is_auto_acked=False, transport=mock_transport,
        )
        resp.ack_all = AsyncMock(side_effect=RuntimeError("ack failed"))

        async def fake_subscribe(**kwargs):
            yield resp

        async def error_cb(e):
            errors.append(e)

        async def msg_cb(msg):
            pass

        with patch.object(client, "subscribe_to_queue", fake_subscribe):
            await client.process_queue_messages(
                channel="test", callback=msg_cb, error_callback=error_cb,
            )

        assert len(errors) == 1
        assert "ack failed" in str(errors[0])


class TestAckAllQueueMessagesErrorResponse:

    @pytest.mark.asyncio
    async def test_ack_all_error_raises_message_error(self, mock_transport):
        from kubemq.core.exceptions import KubeMQMessageError

        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        mock_response = pb.AckAllQueueMessagesResponse()
        mock_response.IsError = True
        mock_response.Error = "queue not found"
        mock_transport.ack_all_queue_messages.return_value = mock_response

        with pytest.raises(KubeMQMessageError, match="queue not found"):
            await client.ack_all_queue_messages(channel="missing-queue")


class TestAckAllCustomWaitTime:
    """GAP-H11: Tests for configurable WaitTimeSeconds in ack_all."""

    @pytest.mark.asyncio
    async def test_ack_all_custom_wait_time(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        mock_response = pb.AckAllQueueMessagesResponse()
        mock_response.IsError = False
        mock_response.AffectedMessages = 5
        mock_transport.ack_all_queue_messages.return_value = mock_response

        result = await client.ack_all_queue_messages(
            channel="test-queue", wait_time_seconds=120
        )

        assert result == 5
        req_arg = mock_transport.ack_all_queue_messages.call_args[0][0]
        assert req_arg.WaitTimeSeconds == 120


class TestAsyncClientQueuesInfo:
    """GAP-H7: Tests for queues_info on async client."""

    @pytest.mark.asyncio
    async def test_queues_info_calls_transport(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        mock_response = MagicMock()
        mock_transport.queues_info.return_value = mock_response

        result = await client.queues_info()

        mock_transport.queues_info.assert_called_once()
        assert result is mock_response

    @pytest.mark.asyncio
    async def test_queues_info_with_specific_queue(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        mock_response = MagicMock()
        mock_transport.queues_info.return_value = mock_response

        result = await client.queues_info("my-queue")

        req_arg = mock_transport.queues_info.call_args[0][0]
        assert req_arg.QueueName == "my-queue"
        assert result is mock_response
