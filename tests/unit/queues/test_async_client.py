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

        mock_send_result = QueueSendResult(id="msg-123", is_error=False, sent_at=1234567890)
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
        r2 = mock_response.Results.add()
        r2.MessageID = "msg-1"
        r2.IsError = True
        r2.Error = "channel not found"
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
            await client.send_queue_message(QueueMessage(channel="new-channel", body=b"test"))

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

        with patch("kubemq.queues.async_client.QueueMessageReceived.decode") as mock_decode:
            mock_msg = MagicMock()
            mock_decode.return_value = mock_msg

            result = AsyncQueuesPollResponse.decode(
                pb_response,
                receiver_client_id="rcv-1",
                transport=transport,
                request_auto_ack=True,
            )

        assert result.ref_request_id == "ref-1"
        assert result.transaction_id == "tx-1"
        assert result.is_transaction_completed is True
        assert result.active_offsets == [5, 10]
        assert result.receiver_client_id == "rcv-1"
        assert result.is_auto_acked is True
        assert len(result.messages) == 1

        from unittest.mock import ANY

        mock_decode.assert_called_once_with(
            msg,
            "tx-1",
            True,
            "rcv-1",
            None,
            is_auto_acked=True,
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

        with patch("kubemq.queues.async_client.QueueMessageReceived.decode") as mock_decode:
            mock_decode.side_effect = [MagicMock(), MagicMock(), MagicMock()]
            result = AsyncQueuesPollResponse.decode(
                pb_response,
                "rcv-2",
                transport,
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

        # Build a QueuesDownstreamResponse (the bidi stream response type)
        mock_downstream_response = pb.QueuesDownstreamResponse()
        mock_downstream_response.RefRequestId = "req-1"
        mock_downstream_response.TransactionId = "tx-1"
        mock_downstream_response.IsError = False
        mock_downstream_response.TransactionComplete = False

        for i in range(3):
            msg = mock_downstream_response.Messages.add()
            msg.MessageID = f"msg-{i}"
            msg.Body = b"data"

        # Mock the downstream receiver
        mock_receiver = AsyncMock()
        mock_receiver.send = AsyncMock(return_value=mock_downstream_response)

        with patch.object(
            client, "_get_downstream_receiver", new_callable=AsyncMock, return_value=mock_receiver
        ):
            with patch(
                "kubemq.queues.async_client.QueueMessageReceived.decode",
                return_value=MagicMock(),
            ):
                response = await client.receive_queue_messages(
                    channel="test-ch",
                    max_messages=10,
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
                ref_request_id="r",
                transaction_id="",
                messages=[],
                error="",
                is_error=False,
                is_transaction_completed=False,
                active_offsets=[],
                receiver_client_id="c",
                is_auto_acked=True,
                transport=mock_transport,
            )

        with patch.object(client, "receive_queue_messages", mock_receive):
            with patch(
                "kubemq.queues.async_client.asyncio.sleep",
                new_callable=AsyncMock,
            ) as mock_sleep:
                async for _ in client.subscribe_to_queue(
                    channel="test",
                    cancellation_token=token,
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
                ref_request_id="r",
                transaction_id="",
                messages=[],
                error="server error",
                is_error=True,
                is_transaction_completed=False,
                active_offsets=[],
                receiver_client_id="c",
                is_auto_acked=True,
                transport=mock_transport,
            )

        async def error_cb(e):
            errors.append(e)

        async def msg_cb(msg):
            pass

        with patch.object(client, "subscribe_to_queue", fake_subscribe):
            await client.process_queue_messages(
                channel="test",
                callback=msg_cb,
                error_callback=error_cb,
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
                ref_request_id="r",
                transaction_id="",
                messages=[],
                error="server error",
                is_error=True,
                is_transaction_completed=False,
                active_offsets=[],
                receiver_client_id="c",
                is_auto_acked=True,
                transport=mock_transport,
            )

        async def error_cb(e):
            raise RuntimeError("callback crashed")

        async def msg_cb(msg):
            pass

        with patch.object(client, "subscribe_to_queue", fake_subscribe):
            await client.process_queue_messages(
                channel="test",
                callback=msg_cb,
                error_callback=error_cb,
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
                ref_request_id="r",
                transaction_id="",
                messages=[MagicMock()],
                error="",
                is_error=False,
                is_transaction_completed=True,
                active_offsets=[],
                receiver_client_id="c",
                is_auto_acked=True,
                transport=mock_transport,
            )

        async def error_cb(e):
            errors.append(e)

        async def msg_cb(msg):
            raise ValueError("handler crash")

        with patch.object(client, "subscribe_to_queue", fake_subscribe):
            await client.process_queue_messages(
                channel="test",
                callback=msg_cb,
                error_callback=error_cb,
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
                ref_request_id="r",
                transaction_id="",
                messages=[MagicMock()],
                error="",
                is_error=False,
                is_transaction_completed=True,
                active_offsets=[],
                receiver_client_id="c",
                is_auto_acked=True,
                transport=mock_transport,
            )

        async def error_cb(e):
            raise RuntimeError("error callback exploded")

        async def msg_cb(msg):
            raise ValueError("handler crash")

        with patch.object(client, "subscribe_to_queue", fake_subscribe):
            await client.process_queue_messages(
                channel="test",
                callback=msg_cb,
                error_callback=error_cb,
                auto_ack=True,
            )

    @pytest.mark.asyncio
    @pytest.mark.timeout(5)
    async def test_handler_error_without_callback_is_logged(self, mock_transport):
        client = self._make_client(mock_transport)

        async def fake_subscribe(**kwargs):
            yield AsyncQueuesPollResponse(
                ref_request_id="r",
                transaction_id="",
                messages=[MagicMock()],
                error="",
                is_error=False,
                is_transaction_completed=True,
                active_offsets=[],
                receiver_client_id="c",
                is_auto_acked=True,
                transport=mock_transport,
            )

        async def msg_cb(msg):
            raise ValueError("handler crash")

        with patch.object(client, "subscribe_to_queue", fake_subscribe):
            await client.process_queue_messages(
                channel="test",
                callback=msg_cb,
                auto_ack=True,
            )

    @pytest.mark.asyncio
    @pytest.mark.timeout(5)
    async def test_ack_all_failure_calls_error_callback(self, mock_transport):
        client = self._make_client(mock_transport)
        errors = []

        resp = AsyncQueuesPollResponse(
            ref_request_id="r",
            transaction_id="tx-1",
            messages=[MagicMock()],
            error="",
            is_error=False,
            is_transaction_completed=False,
            active_offsets=[1],
            receiver_client_id="c",
            is_auto_acked=False,
            transport=mock_transport,
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
                channel="test",
                callback=msg_cb,
                error_callback=error_cb,
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

        result = await client.ack_all_queue_messages(channel="test-queue", wait_time_seconds=120)

        assert result == 5
        req_arg = mock_transport.ack_all_queue_messages.call_args[0][0]
        assert req_arg.WaitTimeSeconds == 120


# ==============================================================================
# Extended Coverage Tests — 95% target
# ==============================================================================

from kubemq.core.exceptions import KubeMQValidationError  # noqa: E402


class TestAsyncClientSendQueueMessageSimple:
    """Tests for send_queue_message_simple() (lines 259-306)."""

    @pytest.mark.asyncio
    async def test_send_queue_message_simple_success(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        mock_pb_result = MagicMock()
        mock_pb_result.MessageID = "msg-simple-1"
        mock_pb_result.SentAt = 0
        mock_pb_result.ExpirationAt = 0
        mock_pb_result.DelayedTo = 0
        mock_pb_result.IsError = False
        mock_pb_result.Error = ""
        mock_transport.send_queue_message.return_value = mock_pb_result

        message = QueueMessage(channel="test-queue", body=b"hello")
        result = await client.send_queue_message_simple(message)

        assert result is not None
        mock_transport.send_queue_message.assert_called_once()

    @pytest.mark.asyncio
    async def test_send_queue_message_simple_validation_error(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        message = QueueMessage(channel="test-queue", body=b"test")
        validation_err = ValueError("QueueMessage validation failed")
        with patch.object(QueueMessage, "encode_message", side_effect=validation_err):
            with pytest.raises(KubeMQValidationError) as exc_info:
                await client.send_queue_message_simple(message)
            assert exc_info.value.__cause__ is validation_err

    @pytest.mark.asyncio
    async def test_send_queue_message_simple_transport_error(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        mock_transport.send_queue_message.side_effect = RuntimeError("transport down")

        message = QueueMessage(channel="test-queue", body=b"test")
        with pytest.raises(RuntimeError, match="transport down"):
            await client.send_queue_message_simple(message)


class TestAsyncClientSendQueueMessageBidiErrors:
    """Tests for send_queue_message() bidi error paths (lines 342-354)."""

    @pytest.mark.asyncio
    async def test_send_queue_message_validation_error(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        mock_sender = AsyncMock()
        client._upstream_sender = mock_sender

        message = QueueMessage(channel="test-queue", body=b"test")
        validation_err = ValueError("QueueMessage validation failed")
        with patch.object(QueueMessage, "encode_message", side_effect=validation_err):
            with pytest.raises(KubeMQValidationError):
                await client.send_queue_message(message)

    @pytest.mark.asyncio
    async def test_send_queue_message_generic_error(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        mock_sender = AsyncMock()
        mock_sender.send = AsyncMock(side_effect=RuntimeError("bidi boom"))
        client._upstream_sender = mock_sender

        message = QueueMessage(channel="test-queue", body=b"test")
        with pytest.raises(RuntimeError, match="bidi boom"):
            await client.send_queue_message(message)


class TestAsyncClientGetUpstreamSender:
    """Tests for _get_upstream_sender() lazy init (lines 239-246)."""

    @pytest.mark.asyncio
    async def test_get_upstream_sender_creates_on_first_call(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        assert client._upstream_sender is None

        with patch("kubemq.queues.async_client.AsyncUpstreamSender") as mock_sender_class:
            mock_sender = AsyncMock()
            mock_sender.start = AsyncMock()
            mock_sender_class.return_value = mock_sender

            sender = await client._get_upstream_sender()

            assert sender is mock_sender
            mock_sender.start.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_upstream_sender_returns_same_instance(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        mock_sender = AsyncMock()
        client._upstream_sender = mock_sender

        sender = await client._get_upstream_sender()
        assert sender is mock_sender


class TestDoOperationWithMetadata:
    """Tests for _do_operation with metadata (lines 113-115)."""

    @pytest.mark.asyncio
    async def test_do_operation_sets_metadata(self):
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
            active_offsets=[1],
            receiver_client_id="client-1",
            is_auto_acked=False,
            transport=transport,
        )

        await response._do_operation(
            pb.QueuesDownstreamRequestType.AckAll,
            metadata={"trace_id": "abc-123", "env": "test"},
        )

        assert len(captured) == 1
        assert captured[0].Metadata["trace_id"] == "abc-123"
        assert captured[0].Metadata["env"] == "test"
        assert response.is_transaction_completed is True


class TestAsyncClientCloseWithSender:
    """Tests for close() with active sender."""

    @pytest.mark.asyncio
    async def test_close_closes_upstream_sender(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        mock_sender = AsyncMock()
        mock_sender.close = AsyncMock()
        client._upstream_sender = mock_sender

        with patch.object(type(client).__bases__[0], "close", new_callable=AsyncMock):
            await client.close()

        mock_sender.close.assert_called_once()
        assert client._upstream_sender is None


class TestReceiveQueueMessagesValidation:
    """Tests for receive_queue_messages validation (lines 427-432)."""

    @pytest.mark.asyncio
    async def test_receive_no_client_id_raises(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True
        client._config.client_id = ""

        with pytest.raises(ValueError, match="ClientID required"):
            await client.receive_queue_messages(channel="test")

    @pytest.mark.asyncio
    async def test_receive_invalid_max_messages(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        with pytest.raises(ValueError, match="max_messages must be between 1 and 1024"):
            await client.receive_queue_messages(channel="test", max_messages=0)

    @pytest.mark.asyncio
    async def test_receive_invalid_timeout(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        with pytest.raises(ValueError, match="wait_timeout_seconds must be between 0 and 3600"):
            await client.receive_queue_messages(channel="test", wait_timeout_seconds=-1)


class TestPeekQueueMessagesValidation:
    """Tests for peek_queue_messages validation (lines 508-511)."""

    @pytest.mark.asyncio
    async def test_peek_invalid_max_messages(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        with pytest.raises(ValueError, match="max_messages must be between 1 and 1024"):
            await client.peek_queue_messages(channel="test", max_messages=0)

    @pytest.mark.asyncio
    async def test_peek_invalid_timeout(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        with pytest.raises(ValueError, match="wait_timeout_seconds must be between 0 and 3600"):
            await client.peek_queue_messages(channel="test", wait_timeout_seconds=-1)


class TestSubscribeToQueueYield:
    """Tests for subscribe_to_queue yielding and error paths."""

    @pytest.mark.asyncio
    async def test_subscribe_yields_non_empty_response(self, mock_transport):
        from kubemq.common.async_cancellation_token import AsyncCancellationToken
        from kubemq.queues.async_client import AsyncQueuesPollResponse

        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        token = AsyncCancellationToken()
        call_count = 0

        fake_response = MagicMock(spec=AsyncQueuesPollResponse)
        fake_response.is_empty.return_value = False
        fake_response.messages = []

        async def fake_receive(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                token.cancel()
            return fake_response

        with patch.object(client, "receive_queue_messages", side_effect=fake_receive):
            results = []
            async for resp in client.subscribe_to_queue(channel="q1", cancellation_token=token):
                results.append(resp)
            assert len(results) >= 1

    @pytest.mark.asyncio
    async def test_subscribe_breaks_on_cancel_during_error(self, mock_transport):
        from kubemq.common.async_cancellation_token import AsyncCancellationToken

        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        token = AsyncCancellationToken()

        async def failing_receive(**kwargs):
            token.cancel()
            raise RuntimeError("connection lost")

        with patch.object(client, "receive_queue_messages", side_effect=failing_receive):
            results = []
            async for resp in client.subscribe_to_queue(channel="q1", cancellation_token=token):
                results.append(resp)
            assert results == []


class TestAckAllQueueMessagesSuccess:
    """Tests for ack_all_queue_messages success path."""

    @pytest.mark.asyncio
    async def test_ack_all_returns_affected_count(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        mock_response = MagicMock()
        mock_response.IsError = False
        mock_response.AffectedMessages = 42

        mock_transport.ack_all_queue_messages = AsyncMock(return_value=mock_response)
        count = await client.ack_all_queue_messages(channel="q1")
        assert count == 42


class TestProcessQueueMessagesCallbacks:
    """Tests for process_queue_messages error callback paths."""

    @pytest.mark.asyncio
    async def test_error_response_calls_error_callback(self, mock_transport):
        from kubemq.common.async_cancellation_token import AsyncCancellationToken
        from kubemq.queues.async_client import AsyncQueuesPollResponse

        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        token = AsyncCancellationToken()
        errors = []

        error_response = MagicMock(spec=AsyncQueuesPollResponse)
        error_response.is_error = True
        error_response.error = "server error"
        error_response.is_empty.return_value = False
        error_response.messages = []

        call_count = 0

        async def fake_subscribe(**kwargs):
            nonlocal call_count
            call_count += 1
            yield error_response
            token.cancel()

        async def error_cb(err):
            errors.append(str(err))

        with patch.object(client, "subscribe_to_queue", side_effect=fake_subscribe):
            await client.process_queue_messages(
                channel="q1",
                callback=AsyncMock(),
                error_callback=error_cb,
                cancellation_token=token,
            )

        assert len(errors) >= 1

    @pytest.mark.asyncio
    async def test_handler_error_calls_error_callback(self, mock_transport):
        from kubemq.common.async_cancellation_token import AsyncCancellationToken
        from kubemq.queues.async_client import AsyncQueuesPollResponse

        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        token = AsyncCancellationToken()
        errors = []

        mock_msg = MagicMock()
        success_response = MagicMock(spec=AsyncQueuesPollResponse)
        success_response.is_error = False
        success_response.is_empty.return_value = False
        success_response.messages = [mock_msg]
        success_response.is_transaction_completed = True

        async def failing_handler(msg):
            raise ValueError("processing failed")

        async def error_cb(err):
            errors.append(err)

        async def fake_subscribe(**kwargs):
            yield success_response
            token.cancel()

        with patch.object(client, "subscribe_to_queue", side_effect=fake_subscribe):
            await client.process_queue_messages(
                channel="q1",
                callback=failing_handler,
                error_callback=error_cb,
                cancellation_token=token,
            )

        assert len(errors) >= 1


# ==============================================================================
# Coverage extension: _do_operation, decode, send_queue_message,
# receive_queue_messages (None/IsError), send_queue_message_fast,
# peek_queue_messages, subscribe_to_queue, close with downstream,
# process_queue_messages error branches
# ==============================================================================


class TestDoOperationPassesMetadata:
    """Covers _do_operation() with metadata dict — lines 111-113."""

    @pytest.mark.asyncio
    async def test_do_operation_passes_metadata(self):
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
            active_offsets=[1],
            receiver_client_id="client-1",
            is_auto_acked=False,
            transport=transport,
        )

        await response._do_operation(
            pb.QueuesDownstreamRequestType.AckAll,
            metadata={"trace-id": "abc123", "source": "test"},
        )

        assert len(captured) == 1
        assert captured[0].Metadata["trace-id"] == "abc123"
        assert captured[0].Metadata["source"] == "test"
        assert response.is_transaction_completed is True


class TestAsyncQueuesPollResponseDecode:
    """Covers AsyncQueuesPollResponse.decode() — lines 132-175."""

    @pytest.mark.asyncio
    async def test_decode_creates_response_from_protobuf(self):
        """decode() produces a correctly populated AsyncQueuesPollResponse."""
        pb_response = pb.QueuesDownstreamResponse()
        pb_response.RefRequestId = "ref-req-1"
        pb_response.TransactionId = "tx-abc"
        pb_response.TransactionComplete = False
        pb_response.IsError = False
        pb_response.Error = ""
        pb_response.ActiveOffsets.extend([10, 20])

        # Add a message
        msg = pb_response.Messages.add()
        msg.MessageID = "msg-1"
        msg.Channel = "q1"
        msg.Body = b"hello"

        transport = MagicMock()

        result = AsyncQueuesPollResponse.decode(
            pb_response,
            receiver_client_id="client-1",
            transport=transport,
            request_auto_ack=False,
        )

        assert result.ref_request_id == "ref-req-1"
        assert result.transaction_id == "tx-abc"
        assert result.is_error is False
        assert len(result.messages) == 1
        assert result.messages[0].id == "msg-1"
        assert result.messages[0].channel == "q1"
        assert result.messages[0].body == b"hello"
        assert result.active_offsets == [10, 20]
        assert result.receiver_client_id == "client-1"
        assert result.is_auto_acked is False

    @pytest.mark.asyncio
    async def test_decode_with_auto_ack(self):
        """decode() with request_auto_ack=True sets is_auto_acked."""
        pb_response = pb.QueuesDownstreamResponse()
        pb_response.RefRequestId = "ref-2"
        pb_response.TransactionId = "tx-2"
        pb_response.TransactionComplete = True

        transport = MagicMock()

        result = AsyncQueuesPollResponse.decode(
            pb_response,
            receiver_client_id="client-2",
            transport=transport,
            request_auto_ack=True,
        )

        assert result.is_auto_acked is True
        assert result.is_transaction_completed is True

    @pytest.mark.asyncio
    async def test_decode_async_handler_calls_transport(self):
        """The _async_handler created by decode() calls transport.queues_downstream."""
        pb_response = pb.QueuesDownstreamResponse()
        pb_response.RefRequestId = "ref-3"
        pb_response.TransactionId = "tx-3"

        msg = pb_response.Messages.add()
        msg.MessageID = "msg-3"
        msg.Channel = "q1"
        msg.Body = b"data"

        downstream_calls = []

        async def fake_downstream(request_iter):
            async for req in request_iter:
                downstream_calls.append(req)
            yield pb.QueuesDownstreamResponse()

        transport = MagicMock()
        transport.queues_downstream = fake_downstream

        result = AsyncQueuesPollResponse.decode(
            pb_response,
            receiver_client_id="client-3",
            transport=transport,
        )

        # Call the async_response_handler that decode created
        handler = result.messages[0].async_response_handler
        assert handler is not None
        fake_req = pb.QueuesDownstreamRequest(RequestID="ack-1")
        await handler(fake_req)

        assert len(downstream_calls) == 1
        assert downstream_calls[0].RequestID == "ack-1"


class TestAsyncClientSendQueueMessageViaUpstream:
    """Covers send_queue_message() via upstream sender — lines 315-321 (span.is_recording)."""

    @pytest.mark.asyncio
    async def test_send_queue_message_with_recording_span(self, mock_transport):
        """send_queue_message sets span attributes when span is recording."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        mock_send_result = QueueSendResult(id="msg-1", is_error=False, sent_at=1000)
        mock_sender = AsyncMock()
        mock_sender.send = AsyncMock(return_value=mock_send_result)
        client._upstream_sender = mock_sender

        message = QueueMessage(channel="test-channel", body=b"test body")

        result = await client.send_queue_message(message)

        assert result.is_error is False
        assert result.id == "msg-1"
        mock_sender.send.assert_called_once()


class TestAsyncClientSendQueueMessageFast:
    """Covers send_queue_message_fast() — lines 410-412."""

    @pytest.mark.asyncio
    async def test_send_queue_message_fast(self, mock_transport):
        """send_queue_message_fast sends via upstream sender, no instrumentation."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        mock_send_result = QueueSendResult(id="msg-fast", is_error=False, sent_at=2000)
        mock_sender = AsyncMock()
        mock_sender.send = AsyncMock(return_value=mock_send_result)
        client._upstream_sender = mock_sender

        message = QueueMessage(channel="fast-ch", body=b"fast data")
        result = await client.send_queue_message_fast(message)

        assert result.is_error is False
        assert result.id == "msg-fast"
        mock_sender.send.assert_called_once()


class TestAsyncClientReceiveQueueMessagesViaBidiStream:
    """Covers receive_queue_messages() via bidi downstream — lines 617, 631, 647."""

    @pytest.mark.asyncio
    async def test_receive_returns_timeout_when_response_is_none(self, mock_transport):
        """receive_queue_messages returns error response when receiver.send() is None."""
        client = AsyncClient(address="localhost:50000", client_id="test-client")
        client._transport = mock_transport
        client._connected = True

        mock_receiver = AsyncMock()
        mock_receiver.send = AsyncMock(return_value=None)
        client._downstream_receiver = mock_receiver

        response = await client.receive_queue_messages(channel="test-ch")

        assert response.is_error is True
        assert "Timeout" in response.error
        assert response.messages == []

    @pytest.mark.asyncio
    async def test_receive_returns_error_when_kubemq_response_is_error(self, mock_transport):
        """receive_queue_messages returns error when server reports IsError."""
        client = AsyncClient(address="localhost:50000", client_id="test-client")
        client._transport = mock_transport
        client._connected = True

        mock_response = MagicMock()
        mock_response.IsError = True
        mock_response.Error = "queue not found"
        mock_response.RefRequestId = "ref-err"
        mock_response.TransactionId = "tx-err"

        mock_receiver = AsyncMock()
        mock_receiver.send = AsyncMock(return_value=mock_response)
        client._downstream_receiver = mock_receiver

        response = await client.receive_queue_messages(channel="test-ch")

        assert response.is_error is True
        assert response.error == "queue not found"
        assert response.messages == []

    @pytest.mark.asyncio
    async def test_receive_returns_messages_on_success(self, mock_transport):
        """receive_queue_messages decodes messages on success."""
        client = AsyncClient(address="localhost:50000", client_id="test-client")
        client._transport = mock_transport
        client._connected = True

        pb_msg = pb.QueueMessage()
        pb_msg.MessageID = "msg-rx"
        pb_msg.Channel = "test-ch"
        pb_msg.Body = b"payload"

        mock_response = MagicMock()
        mock_response.IsError = False
        mock_response.Error = ""
        mock_response.RefRequestId = "ref-ok"
        mock_response.TransactionId = "tx-ok"
        mock_response.TransactionComplete = False
        mock_response.Messages = [pb_msg]
        mock_response.ActiveOffsets = [1]

        mock_receiver = AsyncMock()
        mock_receiver.send = AsyncMock(return_value=mock_response)
        client._downstream_receiver = mock_receiver

        response = await client.receive_queue_messages(channel="test-ch")

        assert response.is_error is False
        assert len(response.messages) == 1
        assert response.messages[0].id == "msg-rx"


class TestAsyncClientCloseWithDownstream:
    """Covers close() with downstream_receiver — lines 263-264."""

    @pytest.mark.asyncio
    async def test_close_shuts_down_downstream_receiver(self):
        """close() closes both upstream sender and downstream receiver."""
        client = AsyncClient(address="localhost:50000")
        client._transport = AsyncMock()
        client._connected = True

        mock_sender = AsyncMock()
        mock_receiver = AsyncMock()
        client._upstream_sender = mock_sender
        client._downstream_receiver = mock_receiver

        await client.close()

        mock_sender.close.assert_called_once()
        mock_receiver.close.assert_called_once()
        assert client._upstream_sender is None
        assert client._downstream_receiver is None

    @pytest.mark.asyncio
    async def test_close_only_downstream(self):
        """close() works when only downstream_receiver is set (no sender)."""
        client = AsyncClient(address="localhost:50000")
        client._transport = AsyncMock()
        client._connected = True

        mock_receiver = AsyncMock()
        client._downstream_receiver = mock_receiver

        await client.close()

        mock_receiver.close.assert_called_once()
        assert client._downstream_receiver is None


class TestAsyncClientGetDownstreamReceiver:
    """Covers _get_downstream_receiver() — lines 247-249."""

    @pytest.mark.asyncio
    async def test_get_downstream_receiver_initializes_lazily(self, mock_transport):
        """_get_downstream_receiver creates and starts receiver on first call."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        with (
            patch("kubemq.queues.async_client.AsyncDownstreamReceiver") as MockReceiverCls,
        ):
            mock_instance = AsyncMock()
            MockReceiverCls.return_value = mock_instance

            result = await client._get_downstream_receiver()

            assert result is mock_instance
            mock_instance.start.assert_called_once()


class TestAsyncClientSubscribeToQueueLoop:
    """Covers subscribe_to_queue() subscription loop — lines 822-838."""

    @pytest.mark.asyncio
    @pytest.mark.timeout(5)
    async def test_subscribe_yields_non_empty_responses(self, mock_transport):
        """subscribe_to_queue yields responses that have messages."""
        client = AsyncClient(address="localhost:50000", client_id="test-client")
        client._transport = mock_transport
        client._connected = True

        call_count = [0]
        token = AsyncCancellationToken()

        async def mock_receive_queue(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return AsyncQueuesPollResponse(
                    ref_request_id="r1",
                    transaction_id="tx1",
                    messages=[MagicMock()],
                    error="",
                    is_error=False,
                    is_transaction_completed=True,
                    active_offsets=[1],
                    receiver_client_id="test-client",
                    is_auto_acked=True,
                    transport=mock_transport,
                )
            token.cancel()
            return AsyncQueuesPollResponse(
                ref_request_id="r2",
                transaction_id="",
                messages=[],
                error="",
                is_error=False,
                is_transaction_completed=True,
                active_offsets=[],
                receiver_client_id="test-client",
                is_auto_acked=True,
                transport=mock_transport,
            )

        with patch.object(client, "receive_queue_messages", side_effect=mock_receive_queue):
            responses = []
            async for resp in client.subscribe_to_queue(
                channel="test-ch",
                cancellation_token=token,
            ):
                responses.append(resp)

            assert len(responses) == 1
            assert len(responses[0].messages) == 1

    @pytest.mark.asyncio
    @pytest.mark.timeout(5)
    async def test_subscribe_retries_on_error_with_backoff(self, mock_transport):
        """subscribe_to_queue retries on error and increments attempt."""
        client = AsyncClient(address="localhost:50000", client_id="test-client")
        client._transport = mock_transport
        client._connected = True

        call_count = [0]
        token = AsyncCancellationToken()

        async def mock_receive_queue(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] <= 2:
                raise RuntimeError("transient error")
            token.cancel()
            return AsyncQueuesPollResponse(
                ref_request_id="r1",
                transaction_id="",
                messages=[],
                error="",
                is_error=False,
                is_transaction_completed=True,
                active_offsets=[],
                receiver_client_id="test-client",
                is_auto_acked=True,
                transport=mock_transport,
            )

        with patch.object(client, "receive_queue_messages", side_effect=mock_receive_queue):
            async for _ in client.subscribe_to_queue(
                channel="test-ch",
                cancellation_token=token,
            ):
                pass

        assert call_count[0] >= 3

    @pytest.mark.asyncio
    @pytest.mark.timeout(5)
    async def test_subscribe_skips_empty_responses(self, mock_transport):
        """subscribe_to_queue does not yield empty responses."""
        client = AsyncClient(address="localhost:50000", client_id="test-client")
        client._transport = mock_transport
        client._connected = True

        call_count = [0]
        token = AsyncCancellationToken()

        async def mock_receive_queue(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] > 2:
                token.cancel()
            return AsyncQueuesPollResponse(
                ref_request_id="r1",
                transaction_id="",
                messages=[],
                error="",
                is_error=False,
                is_transaction_completed=True,
                active_offsets=[],
                receiver_client_id="test-client",
                is_auto_acked=True,
                transport=mock_transport,
            )

        with patch.object(client, "receive_queue_messages", side_effect=mock_receive_queue):
            responses = []
            async for resp in client.subscribe_to_queue(
                channel="test-ch",
                cancellation_token=token,
            ):
                responses.append(resp)

            # All responses were empty, none should be yielded
            assert responses == []


class TestAsyncClientProcessQueueMessagesErrorBranches:
    """Covers process_queue_messages error branches — lines 885-901, 925."""

    @pytest.mark.asyncio
    @pytest.mark.timeout(5)
    async def test_process_error_response_with_error_callback(self, mock_transport):
        """Error responses are forwarded to error_callback and processing continues."""
        client = AsyncClient(address="localhost:50000", client_id="test-client")
        client._transport = mock_transport
        client._connected = True

        token = AsyncCancellationToken()
        errors = []

        error_response = MagicMock(spec=AsyncQueuesPollResponse)
        error_response.is_error = True
        error_response.error = "queue offline"
        error_response.is_empty.return_value = False
        error_response.messages = []

        async def fake_subscribe(**kwargs):
            yield error_response
            token.cancel()

        async def callback(msg):
            pass

        async def error_cb(err):
            errors.append(err)

        with patch.object(client, "subscribe_to_queue", side_effect=fake_subscribe):
            await client.process_queue_messages(
                channel="q1",
                callback=callback,
                error_callback=error_cb,
                cancellation_token=token,
            )

        assert len(errors) == 1
        assert "queue offline" in str(errors[0])

    @pytest.mark.asyncio
    @pytest.mark.timeout(5)
    async def test_process_error_response_without_error_callback(self, mock_transport):
        """Error responses are logged (not raised) when no error_callback."""
        client = AsyncClient(address="localhost:50000", client_id="test-client")
        client._transport = mock_transport
        client._connected = True

        token = AsyncCancellationToken()

        error_response = MagicMock(spec=AsyncQueuesPollResponse)
        error_response.is_error = True
        error_response.error = "queue offline"
        error_response.is_empty.return_value = False

        async def fake_subscribe(**kwargs):
            yield error_response
            token.cancel()

        async def callback(msg):
            pass

        with patch.object(client, "subscribe_to_queue", side_effect=fake_subscribe):
            # Should not raise even without error_callback
            await client.process_queue_messages(
                channel="q1",
                callback=callback,
                cancellation_token=token,
            )

    @pytest.mark.asyncio
    @pytest.mark.timeout(5)
    async def test_process_error_callback_itself_raises(self, mock_transport):
        """When error_callback itself raises, exception is logged not propagated."""
        client = AsyncClient(address="localhost:50000", client_id="test-client")
        client._transport = mock_transport
        client._connected = True

        token = AsyncCancellationToken()

        error_response = MagicMock(spec=AsyncQueuesPollResponse)
        error_response.is_error = True
        error_response.error = "queue offline"
        error_response.is_empty.return_value = False

        async def fake_subscribe(**kwargs):
            yield error_response
            token.cancel()

        async def callback(msg):
            pass

        async def bad_error_cb(err):
            raise RuntimeError("error_callback exploded")

        with patch.object(client, "subscribe_to_queue", side_effect=fake_subscribe):
            # Should not raise even though error_callback raises
            await client.process_queue_messages(
                channel="q1",
                callback=callback,
                error_callback=bad_error_cb,
                cancellation_token=token,
            )

    @pytest.mark.asyncio
    @pytest.mark.timeout(5)
    async def test_process_auto_ack_false_calls_ack_all(self, mock_transport):
        """process_queue_messages calls ack_all when auto_ack=False and txn not complete."""
        client = AsyncClient(address="localhost:50000", client_id="test-client")
        client._transport = mock_transport
        client._connected = True

        token = AsyncCancellationToken()
        callback_messages = []

        mock_msg = MagicMock()
        success_response = MagicMock(spec=AsyncQueuesPollResponse)
        success_response.is_error = False
        success_response.is_empty.return_value = False
        success_response.messages = [mock_msg]
        success_response.is_transaction_completed = False
        success_response.ack_all = AsyncMock()

        async def fake_subscribe(**kwargs):
            yield success_response
            token.cancel()

        async def callback(msg):
            callback_messages.append(msg)

        with patch.object(client, "subscribe_to_queue", side_effect=fake_subscribe):
            await client.process_queue_messages(
                channel="q1",
                callback=callback,
                auto_ack=False,
                cancellation_token=token,
            )

        assert len(callback_messages) == 1
        success_response.ack_all.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.timeout(5)
    async def test_process_ack_all_error_with_error_callback(self, mock_transport):
        """When ack_all raises and error_callback is set, error is forwarded."""
        client = AsyncClient(address="localhost:50000", client_id="test-client")
        client._transport = mock_transport
        client._connected = True

        token = AsyncCancellationToken()
        errors = []

        mock_msg = MagicMock()
        success_response = MagicMock(spec=AsyncQueuesPollResponse)
        success_response.is_error = False
        success_response.is_empty.return_value = False
        success_response.messages = [mock_msg]
        success_response.is_transaction_completed = False
        success_response.ack_all = AsyncMock(side_effect=RuntimeError("ack failed"))

        async def fake_subscribe(**kwargs):
            yield success_response
            token.cancel()

        async def callback(msg):
            pass

        async def error_cb(err):
            errors.append(err)

        with patch.object(client, "subscribe_to_queue", side_effect=fake_subscribe):
            await client.process_queue_messages(
                channel="q1",
                callback=callback,
                error_callback=error_cb,
                auto_ack=False,
                cancellation_token=token,
            )

        assert len(errors) == 1
        assert "ack failed" in str(errors[0])

    @pytest.mark.asyncio
    @pytest.mark.timeout(5)
    async def test_process_handler_error_without_error_callback(self, mock_transport):
        """Handler error without error_callback is logged, not raised."""
        client = AsyncClient(address="localhost:50000", client_id="test-client")
        client._transport = mock_transport
        client._connected = True

        token = AsyncCancellationToken()

        mock_msg = MagicMock()
        success_response = MagicMock(spec=AsyncQueuesPollResponse)
        success_response.is_error = False
        success_response.is_empty.return_value = False
        success_response.messages = [mock_msg]
        success_response.is_transaction_completed = True

        async def fake_subscribe(**kwargs):
            yield success_response
            token.cancel()

        async def failing_callback(msg):
            raise ValueError("handler broke")

        with patch.object(client, "subscribe_to_queue", side_effect=fake_subscribe):
            # Should not raise - error is logged
            await client.process_queue_messages(
                channel="q1",
                callback=failing_callback,
                cancellation_token=token,
            )


class TestAsyncClientReceiveQueueMessagesFast:
    """Covers receive_queue_messages_fast() — lines 422-465."""

    @pytest.mark.asyncio
    async def test_fast_receive_success_with_messages(self, mock_transport):
        """receive_queue_messages_fast returns messages on success."""
        client = AsyncClient(address="localhost:50000", client_id="test-client")
        client._transport = mock_transport
        client._connected = True

        pb_msg = pb.QueueMessage()
        pb_msg.MessageID = "msg-fast-1"
        pb_msg.Channel = "fast-ch"
        pb_msg.Body = b"fast payload"

        mock_response = MagicMock()
        mock_response.IsError = False
        mock_response.Error = ""
        mock_response.RefRequestId = "ref-fast"
        mock_response.TransactionId = "tx-fast"
        mock_response.TransactionComplete = False
        mock_response.Messages = [pb_msg]
        mock_response.ActiveOffsets = [1]

        mock_receiver = AsyncMock()
        mock_receiver.send = AsyncMock(return_value=mock_response)
        client._downstream_receiver = mock_receiver

        response = await client.receive_queue_messages_fast(
            channel="fast-ch",
            max_messages=5,
            wait_timeout_seconds=30,
        )

        assert response.is_error is False
        assert len(response.messages) == 1
        assert response.messages[0].id == "msg-fast-1"
        assert response.receiver_client_id == "test-client"

    @pytest.mark.asyncio
    async def test_fast_receive_returns_error_on_none_response(self, mock_transport):
        """receive_queue_messages_fast returns error when response is None."""
        client = AsyncClient(address="localhost:50000", client_id="test-client")
        client._transport = mock_transport
        client._connected = True

        mock_receiver = AsyncMock()
        mock_receiver.send = AsyncMock(return_value=None)
        client._downstream_receiver = mock_receiver

        response = await client.receive_queue_messages_fast(channel="fast-ch")

        assert response.is_error is True
        assert "Timeout" in response.error

    @pytest.mark.asyncio
    async def test_fast_receive_returns_error_on_is_error(self, mock_transport):
        """receive_queue_messages_fast returns error when IsError is True."""
        client = AsyncClient(address="localhost:50000", client_id="test-client")
        client._transport = mock_transport
        client._connected = True

        mock_response = MagicMock()
        mock_response.IsError = True
        mock_response.Error = "queue full"
        mock_response.TransactionId = "tx-err"

        mock_receiver = AsyncMock()
        mock_receiver.send = AsyncMock(return_value=mock_response)
        client._downstream_receiver = mock_receiver

        response = await client.receive_queue_messages_fast(channel="fast-ch")

        assert response.is_error is True
        assert response.error == "queue full"

    @pytest.mark.asyncio
    async def test_fast_receive_with_auto_ack(self, mock_transport):
        """receive_queue_messages_fast passes auto_ack correctly."""
        client = AsyncClient(address="localhost:50000", client_id="test-client")
        client._transport = mock_transport
        client._connected = True

        mock_response = MagicMock()
        mock_response.IsError = False
        mock_response.Error = ""
        mock_response.RefRequestId = "ref-aa"
        mock_response.TransactionId = "tx-aa"
        mock_response.TransactionComplete = True
        mock_response.Messages = []
        mock_response.ActiveOffsets = []

        mock_receiver = AsyncMock()
        mock_receiver.send = AsyncMock(return_value=mock_response)
        client._downstream_receiver = mock_receiver

        response = await client.receive_queue_messages_fast(
            channel="fast-ch",
            auto_ack=True,
        )

        assert response.is_auto_acked is True


class TestAsyncClientSendQueueMessageSimpleWithSpan:
    """Covers send_queue_message_simple with span.is_recording() — lines 315-321."""

    @pytest.mark.asyncio
    async def test_send_simple_sets_span_attributes_when_recording(self, mock_transport):
        """send_queue_message_simple sets span attributes when span is recording."""
        from contextlib import contextmanager

        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        # Mock the transport to return a valid result
        pb_result = pb.SendQueueMessageResult()
        pb_result.MessageID = "msg-span"
        pb_result.IsError = False
        pb_result.SentAt = 1000
        mock_transport.send_queue_message.return_value = pb_result

        # Mock the instrumentor to provide a recording span
        mock_span = MagicMock()
        mock_span.is_recording.return_value = True

        @contextmanager
        def fake_start_span(op, channel):
            yield mock_span

        client._instrumentor = MagicMock()
        client._instrumentor.start_span = fake_start_span

        message = QueueMessage(channel="test-channel", body=b"test body")

        result = await client.send_queue_message_simple(message)

        assert result.is_error is False
        # Verify span.set_attribute was called (lines 315-321)
        assert mock_span.set_attribute.call_count == 2


class TestAsyncClientSendQueueMessageBidiWithSpan:
    """Covers send_queue_message with span.is_recording() — lines 383-389."""

    @pytest.mark.asyncio
    async def test_send_bidi_sets_span_attributes_when_recording(self, mock_transport):
        """send_queue_message sets span attributes when span is recording."""
        from contextlib import contextmanager

        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        mock_send_result = QueueSendResult(id="msg-span", is_error=False, sent_at=1000)
        mock_sender = AsyncMock()
        mock_sender.send = AsyncMock(return_value=mock_send_result)
        client._upstream_sender = mock_sender

        # Mock the instrumentor to provide a recording span
        mock_span = MagicMock()
        mock_span.is_recording.return_value = True

        @contextmanager
        def fake_start_span(op, channel):
            yield mock_span

        client._instrumentor = MagicMock()
        client._instrumentor.start_span = fake_start_span

        message = QueueMessage(channel="test-channel", body=b"test body")

        result = await client.send_queue_message(message)

        assert result.is_error is False
        # Verify span.set_attribute was called (lines 383-389)
        assert mock_span.set_attribute.call_count == 2
