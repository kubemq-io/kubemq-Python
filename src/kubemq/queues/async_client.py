"""Native async Queues client for KubeMQ."""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import (
    TYPE_CHECKING,
    Any,
)


from kubemq._internal.retry import BackoffCalculator
from kubemq._internal.telemetry import KubeMQTagsCarrier, error_code_to_error_type
from kubemq.common.async_cancellation_token import AsyncCancellationToken
from kubemq.core.client import NativeAsyncBaseClient
from kubemq.core.config import ClientConfig
from kubemq.core.exceptions import KubeMQHandlerError, KubeMQMessageError, KubeMQValidationError
from kubemq.grpc import kubemq_pb2 as pb
from kubemq.queues.async_downstream_receiver import AsyncDownstreamReceiver
from kubemq.queues.async_upstream_sender import AsyncUpstreamSender
from kubemq.queues.queues_message import QueueMessage
from kubemq.queues.queues_message_received import QueueMessageReceived
from kubemq.queues.queues_send_result import QueueBatchSendResult, QueueSendResult

if TYPE_CHECKING:
    from kubemq.transport.async_transport import AsyncTransport

_logger = logging.getLogger("kubemq.queues.async_client")

# Type aliases for callbacks
AsyncMessageCallback = Callable[[QueueMessageReceived], Awaitable[None]]
AsyncErrorCallback = Callable[[Exception], Awaitable[None]]


class AsyncQueuesPollResponse:
    """Async response from polling messages from a queue.

    This is a simplified async version of QueuesPollResponse that provides
    async methods for acknowledging, rejecting, and re-queuing messages.
    """

    def __init__(
        self,
        ref_request_id: str,
        transaction_id: str,
        messages: list[QueueMessageReceived],
        error: str,
        is_error: bool,
        is_transaction_completed: bool,
        active_offsets: list[int],
        receiver_client_id: str,
        is_auto_acked: bool,
        transport: AsyncTransport,
    ) -> None:
        self.ref_request_id = ref_request_id
        self.transaction_id = transaction_id
        self.messages = messages
        self.error = error
        self.is_error = is_error
        self.is_transaction_completed = is_transaction_completed
        self.active_offsets = active_offsets
        self.receiver_client_id = receiver_client_id
        self.is_auto_acked = is_auto_acked
        self._transport = transport

    async def ack_all(self) -> None:
        """Acknowledge all messages in the response."""
        if self.is_auto_acked:
            raise ValueError("Messages are auto-acknowledged")
        if self.is_transaction_completed:
            raise ValueError("Transaction is already completed")

        await self._do_operation(pb.QueuesDownstreamRequestType.AckAll)

    async def reject_all(self) -> None:
        """Reject all messages in the response."""
        if self.is_auto_acked:
            raise ValueError("Messages are auto-acknowledged")
        if self.is_transaction_completed:
            raise ValueError("Transaction is already completed")

        await self._do_operation(pb.QueuesDownstreamRequestType.NAckAll)

    async def re_queue_all(self, channel: str) -> None:
        """Re-queue all messages to another channel."""
        if self.is_auto_acked:
            raise ValueError("Messages are auto-acknowledged")
        if self.is_transaction_completed:
            raise ValueError("Transaction is already completed")

        await self._do_operation(pb.QueuesDownstreamRequestType.ReQueueAll, channel)

    async def _do_operation(
        self,
        request_type: int,
        re_queue_channel: str = "",
        metadata: dict[str, str] | None = None,
    ) -> None:
        """Perform an operation on all messages."""
        request = pb.QueuesDownstreamRequest()
        request.RequestID = str(uuid.uuid4())
        request.ClientID = self.receiver_client_id
        request.RequestTypeData = request_type  # type: ignore[assignment]
        request.ReQueueChannel = re_queue_channel
        request.RefTransactionId = self.transaction_id
        request.SequenceRange.extend(self.active_offsets)
        if metadata:
            for k, v in metadata.items():
                request.Metadata[k] = v

        # Use the transport's receive method with a single request
        async def single_request() -> AsyncIterator[pb.QueuesDownstreamRequest]:
            yield request

        async for _ in self._transport.queues_downstream(single_request()):
            break  # Only need one response

        self.is_transaction_completed = True

    def count(self) -> int:
        """Get the number of messages in the response."""
        return len(self.messages)

    def is_empty(self) -> bool:
        """Check if the response contains no messages."""
        return len(self.messages) == 0

    @classmethod
    def decode(
        cls,
        response: pb.QueuesDownstreamResponse,
        receiver_client_id: str,
        transport: AsyncTransport,
        request_auto_ack: bool = False,
    ) -> AsyncQueuesPollResponse:
        """Create an AsyncQueuesPollResponse from a protobuf response."""

        async def _async_handler(req: pb.QueuesDownstreamRequest) -> None:
            """Send a per-message ack/reject/requeue via a short-lived downstream stream."""

            async def _single() -> AsyncIterator[pb.QueuesDownstreamRequest]:
                yield req

            async for _ in transport.queues_downstream(_single()):
                break

        messages = [
            QueueMessageReceived.decode(
                message,
                response.TransactionId,
                response.TransactionComplete,
                receiver_client_id,
                None,
                is_auto_acked=request_auto_ack,
                async_response_handler=_async_handler,
            )
            for message in response.Messages
        ]

        return cls(
            ref_request_id=response.RefRequestId,
            transaction_id=response.TransactionId,
            messages=messages,
            error=response.Error,
            is_error=response.IsError,
            is_transaction_completed=response.TransactionComplete,
            active_offsets=list(response.ActiveOffsets),
            receiver_client_id=receiver_client_id,
            is_auto_acked=request_auto_ack,
            transport=transport,
        )


class AsyncClient(NativeAsyncBaseClient):
    """Native async Queues client.

    Provides queue message sending and receiving with transaction support
    using native gRPC async.

    Example:
        async with AsyncClient(address="localhost:50000") as client:
            # Send message
            result = await client.send_queue_message(QueueMessage(
                channel="tasks",
                body=b"work item",
            ))

            # Receive messages
            response = await client.receive_queue_messages(
                channel="tasks",
                max_messages=10,
            )
            for msg in response.messages:
                print(msg.body)
            await response.ack_all()

    Thread Safety:
        Safe to share across asyncio tasks within a single event loop.
    """

    def __init__(
        self,
        address: str = "",
        client_id: str | None = None,
        auth_token: str | None = None,
        config: ClientConfig | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the async Queues client.

        Note: The client is NOT connected after initialization.
        Use `await client.connect()` or `async with client:` to connect.

        Args:
            address: KubeMQ server address (host:port)
            client_id: Client identifier (defaults to hostname)
            auth_token: Authentication token
            config: Pre-built ClientConfig object (overrides other params)
            **kwargs: Additional configuration options
        """
        super().__init__(
            address=address,
            client_id=client_id,
            auth_token=auth_token,
            config=config,
            **kwargs,
        )
        self._upstream_sender: AsyncUpstreamSender | None = None
        self._downstream_receiver: AsyncDownstreamReceiver | None = None

    async def _get_upstream_sender(self) -> AsyncUpstreamSender:
        """Lazily initialize the bidirectional upstream stream sender."""
        if self._upstream_sender is None:
            self._ensure_connected()
            self._upstream_sender = AsyncUpstreamSender(self._pick_pool_transport())
            await self._upstream_sender.start()
        return self._upstream_sender

    async def _get_downstream_receiver(self) -> AsyncDownstreamReceiver:
        """Lazily initialize the persistent downstream bidi stream."""
        if self._downstream_receiver is None:
            self._ensure_connected()
            self._downstream_receiver = AsyncDownstreamReceiver(self._pick_pool_transport())
            await self._downstream_receiver.start()
        return self._downstream_receiver

    async def close(self) -> None:
        """Close the client and its senders/receivers.

        Releases all bidirectional streams and transport resources.

        See Also:
            :meth:`QueuesClient.close`: Sync counterpart.
        """
        if self._upstream_sender is not None:
            await self._upstream_sender.close()
            self._upstream_sender = None
        if self._downstream_receiver is not None:
            await self._downstream_receiver.close()
            self._downstream_receiver = None
        await super().close()

    # =========================================================================
    # Send Operations
    # =========================================================================

    async def send_queue_message_simple(
        self,
        message: QueueMessage,
    ) -> QueueSendResult:
        """Send a single queue message via unary SendQueueMessage RPC.

        Uses the unary ``SendQueueMessage`` RPC for single-shot delivery.

        Args:
            message: The queue message to send.

        Returns:
            QueueSendResult: Contains ``is_error`` (bool), ``error``
            (error description), ``message_id`` (server-assigned ID),
            ``sent_at`` (timestamp), and ``expired_at`` (expiration time).

        Raises:
            KubeMQValidationError: If the message fails validation (e.g.,
                empty channel, body exceeds ``max_send_size``).
            KubeMQConnectionError: If the server is unreachable or the
                connection is lost.
            KubeMQAuthenticationError: If the auth token is invalid or
                expired, or the client lacks permission for the channel.
            KubeMQTimeoutError: If the operation exceeds the server deadline.
            KubeMQClientClosedError: If the client has already been closed.

        See Also:
            :meth:`QueuesClient.send_queue_message_simple`: Sync counterpart.
            :meth:`send_queue_message`: Streaming variant with higher
                throughput.
            :meth:`receive_queue_messages`: Consume messages from a queue.
        """
        self._validate_message_size(message.body)
        start = time.perf_counter()
        error_type_val = None
        with self._instrumentor.start_span("send", message.channel) as span:
            try:
                self._ensure_connected()
                assert self._transport is not None
                pb_message = message.encode_message(self._config.client_id or "")
                tags_dict = dict(pb_message.Tags)
                KubeMQTagsCarrier(tags_dict).inject()
                pb_message.Tags.update(tags_dict)
                if span.is_recording():
                    from kubemq._internal.semconv import (
                        MESSAGING_MESSAGE_BODY_SIZE,
                        MESSAGING_MESSAGE_ID,
                    )

                    span.set_attribute(MESSAGING_MESSAGE_ID, message.id)
                    span.set_attribute(MESSAGING_MESSAGE_BODY_SIZE, len(message.body))
                result = await self._transport.send_queue_message(pb_message)
                self._instrumentor._metrics.record_sent_message("send", message.channel)
                return QueueSendResult.decode(result)
            except (ValueError, TypeError) as e:
                error_type_val = "validation"
                self._instrumentor.record_error(span, e, error_type_val)
                raise KubeMQValidationError(str(e), is_retryable=False) from e
            except Exception as e:
                error_type_val = error_code_to_error_type(getattr(e, "code", None))
                self._instrumentor.record_error(span, e, error_type_val)
                raise
            finally:
                duration = time.perf_counter() - start
                self._instrumentor._metrics.record_operation_duration(
                    duration, "send", message.channel, error_type_val
                )

    async def send_queue_message(
        self,
        message: QueueMessage,
    ) -> QueueSendResult:
        """Send a single queue message via bidirectional upstream stream.

        Uses ``QueuesUpstream`` bidi RPC for high-throughput delivery.

        Args:
            message: The queue message to send.

        Returns:
            QueueSendResult: Contains ``is_error`` (bool), ``error``
            (error description), ``message_id`` (server-assigned ID),
            ``sent_at`` (timestamp), and ``expired_at`` (expiration time).

        Raises:
            KubeMQValidationError: If the message fails validation (e.g.,
                empty channel, body exceeds ``max_send_size``).
            KubeMQConnectionError: If the server is unreachable or the
                connection is lost.
            KubeMQAuthenticationError: If the auth token is invalid or
                expired, or the client lacks permission for the channel.
            KubeMQTimeoutError: If the operation exceeds the server deadline.
            KubeMQClientClosedError: If the client has already been closed.

        See Also:
            :meth:`QueuesClient.send_queue_message`: Sync counterpart.
            :class:`~kubemq.queues.queues_message.QueueMessage`:
                Message type for queue operations.
            :meth:`receive_queue_messages`: Consume messages from a queue.
            :meth:`send_queue_messages_batch`: Send multiple messages
                atomically.
        """
        self._validate_message_size(message.body)
        start = time.perf_counter()
        error_type_val = None
        with self._instrumentor.start_span("send", message.channel) as span:
            try:
                pb_message = message.encode_message(self._config.client_id or "")
                tags_dict = dict(pb_message.Tags)
                KubeMQTagsCarrier(tags_dict).inject()
                pb_message.Tags.update(tags_dict)
                if span.is_recording():
                    from kubemq._internal.semconv import (
                        MESSAGING_MESSAGE_BODY_SIZE,
                        MESSAGING_MESSAGE_ID,
                    )

                    span.set_attribute(MESSAGING_MESSAGE_ID, message.id)
                    span.set_attribute(MESSAGING_MESSAGE_BODY_SIZE, len(message.body))
                sender = await self._get_upstream_sender()
                result = await sender.send(pb_message)
                self._instrumentor._metrics.record_sent_message("send", message.channel)
                return result
            except (ValueError, TypeError) as e:
                error_type_val = "validation"
                self._instrumentor.record_error(span, e, error_type_val)
                raise KubeMQValidationError(str(e), is_retryable=False) from e
            except Exception as e:
                error_type_val = error_code_to_error_type(getattr(e, "code", None))
                self._instrumentor.record_error(span, e, error_type_val)
                raise
            finally:
                duration = time.perf_counter() - start
                self._instrumentor._metrics.record_operation_duration(
                    duration, "send", message.channel, error_type_val
                )

    async def send_queue_message_fast(self, message: QueueMessage) -> QueueSendResult:
        """Send queue message — fast path, no instrumentation."""
        sender = await self._get_upstream_sender()
        pb_message = message.encode_message(self._config.client_id or "")
        return await sender.send(pb_message)

    async def receive_queue_messages_fast(
        self,
        channel: str,
        max_messages: int = 1,
        wait_timeout_seconds: int = 60,
        auto_ack: bool = False,
    ) -> AsyncQueuesPollResponse:
        """Receive queue messages — fast path, no instrumentation."""
        receiver = await self._get_downstream_receiver()
        client_id = self._config.client_id or ""

        request = pb.QueuesDownstreamRequest()
        request.RequestID = str(uuid.uuid4())
        request.ClientID = client_id
        request.Channel = channel
        request.MaxItems = max_messages
        request.WaitTimeout = wait_timeout_seconds * 1000
        request.AutoAck = auto_ack
        request.RequestTypeData = pb.QueuesDownstreamRequestType.Get

        kubemq_response = await receiver.send(request)
        if kubemq_response is None or kubemq_response.IsError:
            return AsyncQueuesPollResponse(
                ref_request_id=request.RequestID,
                transaction_id=kubemq_response.TransactionId if kubemq_response else "",
                messages=[],
                error=kubemq_response.Error if kubemq_response else "Timeout",
                is_error=True,
                is_transaction_completed=True,
                active_offsets=[],
                receiver_client_id=client_id,
                is_auto_acked=auto_ack,
                transport=self._transport,
            )

        async def _async_handler(req: pb.QueuesDownstreamRequest) -> None:
            await receiver.send_without_response(req)

        messages = [
            QueueMessageReceived.decode(
                message,
                kubemq_response.TransactionId,
                kubemq_response.TransactionComplete,
                client_id,
                None,
                is_auto_acked=auto_ack,
                async_response_handler=_async_handler,
            )
            for message in kubemq_response.Messages
        ]

        return AsyncQueuesPollResponse(
            ref_request_id=kubemq_response.RefRequestId,
            transaction_id=kubemq_response.TransactionId,
            messages=messages,
            error=kubemq_response.Error,
            is_error=kubemq_response.IsError,
            is_transaction_completed=kubemq_response.TransactionComplete,
            active_offsets=list(kubemq_response.ActiveOffsets),
            receiver_client_id=client_id,
            is_auto_acked=auto_ack,
            transport=self._transport,
        )

    async def send_queue_messages_batch(
        self,
        messages: list[QueueMessage],
    ) -> QueueBatchSendResult:
        """Send multiple queue messages as a server-side batch.

        Uses the gRPC ``SendQueueMessagesBatch`` RPC for atomic batch tracking
        with ``BatchID`` correlation and aggregate ``HaveErrors`` flag.

        Args:
            messages: List of messages to send. Each message must have a
                valid ``channel`` and ``body``.

        Returns:
            QueueBatchSendResult: Contains ``batch_id`` (correlation ID for
            the batch), ``have_errors`` (bool indicating if any message
            failed), and ``results`` (list of per-message
            :class:`QueueSendResult` objects).

        Raises:
            KubeMQValidationError: If any message fails validation (e.g.,
                empty channel, body exceeds ``max_send_size``).
            KubeMQConnectionError: If the server is unreachable or the
                connection is lost.
            KubeMQAuthenticationError: If the auth token is invalid or
                expired, or the client lacks permission for the channel.
            KubeMQTimeoutError: If the operation exceeds the server deadline.
            KubeMQClientClosedError: If the client has already been closed.

        See Also:
            :meth:`QueuesClient.send_queue_messages_batch`: Sync counterpart.
            :meth:`send_queue_message`: Send a single queue message.
        """
        self._ensure_connected()
        assert self._transport is not None

        batch_id = str(uuid.uuid4())
        client_id = self._config.client_id or ""

        batch_request = pb.QueueMessagesBatchRequest()
        batch_request.BatchID = batch_id
        for msg in messages:
            self._validate_message_size(msg.body)
            pb_msg = msg.encode_message(client_id)
            tags_dict = dict(pb_msg.Tags)
            KubeMQTagsCarrier(tags_dict).inject()
            pb_msg.Tags.update(tags_dict)
            batch_request.Messages.append(pb_msg)

        batch_response = await self._transport.send_queue_messages_batch(batch_request)

        results: list[QueueSendResult] = []
        for pb_result in batch_response.Results:
            results.append(QueueSendResult.decode(pb_result))

        return QueueBatchSendResult(
            batch_id=batch_response.BatchID,
            results=results,
            have_errors=batch_response.HaveErrors,
        )

    # =========================================================================
    # Receive Operations
    # =========================================================================

    async def receive_queue_messages(
        self,
        channel: str,
        max_messages: int = 1,
        wait_timeout_seconds: int = 60,
        auto_ack: bool = False,
    ) -> AsyncQueuesPollResponse:
        """Receive messages from queue (polling).

        Long-polls the server for up to ``wait_timeout_seconds``. If
        ``auto_ack`` is ``False``, each message must be explicitly
        acknowledged, rejected, or re-queued.

        Args:
            channel: Queue channel to receive from.
            max_messages: Maximum number of messages to receive (1–1024).
            wait_timeout_seconds: Timeout in seconds to wait for messages
                (0–3600).
            auto_ack: If True, messages are auto-acknowledged after receive.

        Returns:
            AsyncQueuesPollResponse: Contains ``messages`` (list of received
            queue messages, each supporting ``.ack()``, ``.nack()``,
            and ``.requeue()``), ``is_error`` (bool), ``error`` (error
            description), and async methods ``ack_all()``, ``reject_all()``,
            ``re_queue_all()``.

        Raises:
            ValueError: If ``max_messages`` is not between 1 and 1024 or
                ``wait_timeout_seconds`` is not between 0 and 3600.
            KubeMQConnectionError: If the server is unreachable or the
                connection is lost.
            KubeMQAuthenticationError: If the auth token is invalid or
                expired, or the client lacks permission for the channel.
            KubeMQTimeoutError: If the operation exceeds the server deadline.
            KubeMQClientClosedError: If the client has already been closed.

        Note:
            The simple receive RPC doesn't support auto_ack at the protocol level.
            If auto_ack is True, messages are acknowledged after being received.

        See Also:
            :meth:`QueuesClient.receive_queue_messages`: Sync counterpart.
            :meth:`send_queue_message`: Send messages to a queue.
            :meth:`peek_queue_messages`: Peek at waiting messages without
                consuming.
            :meth:`subscribe_to_queue`: Stream messages continuously.
        """
        if not self._config.client_id:
            raise ValueError("ClientID required for downstream operations")
        if max_messages < 1 or max_messages > 1024:
            raise ValueError("max_messages must be between 1 and 1024")
        if wait_timeout_seconds < 0 or wait_timeout_seconds > 3600:
            raise ValueError("wait_timeout_seconds must be between 0 and 3600")
        start = time.perf_counter()
        error_type_val = None
        with self._instrumentor.start_span("receive", channel) as span:
            try:
                receiver = await self._get_downstream_receiver()
                client_id = self._config.client_id or ""

                # Use QueuesDownstreamRequest (bidi stream) instead of unary RPC.
                # This gives us a TransactionId from the server for ack/nack.
                request = pb.QueuesDownstreamRequest()
                request.RequestID = str(uuid.uuid4())
                request.ClientID = client_id
                request.Channel = channel
                request.MaxItems = max_messages
                request.WaitTimeout = wait_timeout_seconds * 1000
                request.AutoAck = auto_ack
                request.RequestTypeData = pb.QueuesDownstreamRequestType.Get

                kubemq_response = await receiver.send(request)
                if kubemq_response is None:
                    return AsyncQueuesPollResponse(
                        ref_request_id=request.RequestID,
                        transaction_id="",
                        messages=[],
                        error="Timeout waiting for response",
                        is_error=True,
                        is_transaction_completed=True,
                        active_offsets=[],
                        receiver_client_id=client_id,
                        is_auto_acked=auto_ack,
                        transport=self._transport,
                    )

                if kubemq_response.IsError:
                    return AsyncQueuesPollResponse(
                        ref_request_id=kubemq_response.RefRequestId,
                        transaction_id=kubemq_response.TransactionId,
                        messages=[],
                        error=kubemq_response.Error,
                        is_error=True,
                        is_transaction_completed=True,
                        active_offsets=[],
                        receiver_client_id=client_id,
                        is_auto_acked=auto_ack,
                        transport=self._transport,
                    )

                # Build per-message async handler that sends ack/nack on the
                # SAME persistent downstream stream (preserves TransactionId)
                async def _async_handler(req: pb.QueuesDownstreamRequest) -> None:
                    await receiver.send_without_response(req)

                messages = [
                    QueueMessageReceived.decode(
                        message,
                        kubemq_response.TransactionId,
                        kubemq_response.TransactionComplete,
                        client_id,
                        None,
                        is_auto_acked=auto_ack,
                        async_response_handler=_async_handler,
                    )
                    for message in kubemq_response.Messages
                ]

                for _ in messages:
                    self._instrumentor._metrics.record_consumed_message("receive", channel)

                poll_response = AsyncQueuesPollResponse(
                    ref_request_id=kubemq_response.RefRequestId,
                    transaction_id=kubemq_response.TransactionId,
                    messages=messages,
                    error=kubemq_response.Error,
                    is_error=kubemq_response.IsError,
                    is_transaction_completed=kubemq_response.TransactionComplete,
                    active_offsets=list(kubemq_response.ActiveOffsets),
                    receiver_client_id=client_id,
                    is_auto_acked=auto_ack,
                    transport=self._transport,
                )

                return poll_response
            except Exception as e:
                error_type_val = error_code_to_error_type(getattr(e, "code", None))
                self._instrumentor.record_error(span, e, error_type_val)
                raise
            finally:
                duration = time.perf_counter() - start
                self._instrumentor._metrics.record_operation_duration(
                    duration, "receive", channel, error_type_val
                )

    async def peek_queue_messages(
        self,
        channel: str,
        max_messages: int = 1,
        wait_timeout_seconds: int = 60,
    ) -> AsyncQueuesPollResponse:
        """Peek at messages without removing them from the queue.

        Retrieves messages that are currently waiting in the queue without
        consuming them. Messages remain available for other consumers.

        Args:
            channel: The name of the queue channel.
            max_messages: Maximum number of messages to retrieve (1–1024).
            wait_timeout_seconds: Maximum time to wait for messages in
                seconds (0–3600).

        Returns:
            AsyncQueuesPollResponse: Contains ``messages`` (list of peeked
            messages), ``is_error`` (bool), ``error`` (error description).
            Messages are auto-acknowledged (peek-only, no consume).

        Raises:
            ValueError: If ``max_messages`` is not between 1 and 1024 or
                ``wait_timeout_seconds`` is not between 0 and 3600.
            KubeMQConnectionError: If the server is unreachable or the
                connection is lost.
            KubeMQAuthenticationError: If the auth token is invalid or the
                client lacks permission for the channel.
            KubeMQTimeoutError: If the operation exceeds the server deadline.
            KubeMQClientClosedError: If the client has already been closed.

        See Also:
            :meth:`QueuesClient.waiting`: Sync counterpart.
            :meth:`receive_queue_messages`: Consume messages with
                acknowledgment support.
        """
        if max_messages < 1 or max_messages > 1024:
            raise ValueError("max_messages must be between 1 and 1024")
        if wait_timeout_seconds < 0 or wait_timeout_seconds > 3600:
            raise ValueError("wait_timeout_seconds must be between 0 and 3600")
        self._ensure_connected()
        assert self._transport is not None
        client_id = self._config.client_id or ""

        request = pb.ReceiveQueueMessagesRequest()
        request.RequestID = str(uuid.uuid4())
        request.ClientID = client_id
        request.Channel = channel
        request.MaxNumberOfMessages = max_messages
        request.WaitTimeSeconds = wait_timeout_seconds
        request.IsPeak = True

        response = await self._transport.receive_queue_messages(request)

        messages = [
            QueueMessageReceived.decode(
                msg,
                "",
                True,
                client_id,
                None,
                is_auto_acked=True,
            )
            for msg in response.Messages
        ]

        return AsyncQueuesPollResponse(
            ref_request_id=response.RequestID,
            transaction_id="",
            messages=messages,
            error=response.Error,
            is_error=response.IsError,
            is_transaction_completed=True,
            active_offsets=[],
            receiver_client_id=client_id,
            is_auto_acked=True,
            transport=self._transport,
        )

    # =========================================================================
    # Streaming Operations
    # =========================================================================

    async def subscribe_to_queue(
        self,
        channel: str,
        max_messages: int = 1,
        wait_timeout_seconds: int = 60,
        auto_ack: bool = False,
        cancellation_token: AsyncCancellationToken | None = None,
    ) -> AsyncIterator[AsyncQueuesPollResponse]:
        """Subscribe to queue messages with exponential backoff on errors.

        This method continuously polls for messages and yields responses.
        Transient errors trigger exponential backoff; successful receives
        reset the backoff counter.

        Args:
            channel: Queue channel to subscribe to.
            max_messages: Maximum messages per poll (1–1024).
            wait_timeout_seconds: Wait timeout per poll (0–3600).
            auto_ack: If True, messages are auto-acknowledged.
            cancellation_token: Optional token to cancel subscription.

        Yields:
            AsyncQueuesPollResponse: Response for each poll batch containing
            received messages.

        Raises:
            KubeMQConnectionError: If the server is unreachable or the
                connection is lost.
            KubeMQAuthenticationError: If the auth token is invalid or the
                client lacks permission for the channel.
            KubeMQTimeoutError: If the operation exceeds the server deadline.
            KubeMQClientClosedError: If the client has already been closed.

        See Also:
            :meth:`receive_queue_messages`: Single poll for messages.
            :meth:`process_queue_messages`: Process messages with a callback.
        """
        self._ensure_connected()

        token = cancellation_token or AsyncCancellationToken()
        await self._register_subscription(token)

        backoff = BackoffCalculator(self._config.retry_policy)
        attempt = 0

        try:
            while not token.is_cancelled:
                try:
                    response = await self.receive_queue_messages(
                        channel=channel,
                        max_messages=max_messages,
                        wait_timeout_seconds=wait_timeout_seconds,
                        auto_ack=auto_ack,
                    )

                    attempt = 0
                    if not response.is_empty():
                        yield response

                except Exception as e:
                    if token.is_cancelled:
                        break
                    _logger.warning("Error receiving messages: %s", e)
                    delay = backoff.delay_seconds(attempt)
                    attempt += 1
                    await asyncio.sleep(delay)

        finally:
            await self._unregister_subscription(token)

    async def process_queue_messages(
        self,
        channel: str,
        callback: AsyncMessageCallback,
        error_callback: AsyncErrorCallback | None = None,
        max_messages: int = 1,
        wait_timeout_seconds: int = 60,
        auto_ack: bool = False,
        cancellation_token: AsyncCancellationToken | None = None,
    ) -> None:
        """Process queue messages with an async callback.

        Per-message handler errors are isolated and reported via error_callback
        without terminating the processing loop.

        Args:
            channel: Queue channel to process from.
            callback: Async callback for each message.
            error_callback: Optional async callback for errors.
            max_messages: Maximum messages per poll (1–1024).
            wait_timeout_seconds: Wait timeout per poll (0–3600).
            auto_ack: If True, messages are auto-acknowledged.
            cancellation_token: Optional token to cancel processing.

        Returns:
            None. Runs until cancelled via ``cancellation_token``.

        Raises:
            KubeMQConnectionError: If the server is unreachable or the
                connection is lost.
            KubeMQAuthenticationError: If the auth token is invalid or the
                client lacks permission for the channel.
            KubeMQTimeoutError: If the operation exceeds the server deadline.
            KubeMQClientClosedError: If the client has already been closed.

        See Also:
            :meth:`subscribe_to_queue`: Low-level streaming subscription.
            :meth:`receive_queue_messages`: Single poll for messages.
        """
        token = cancellation_token or AsyncCancellationToken()

        current_task = asyncio.current_task()
        if current_task is not None:
            self._register_subscription_task(current_task)

        async for response in self.subscribe_to_queue(
            channel=channel,
            max_messages=max_messages,
            wait_timeout_seconds=wait_timeout_seconds,
            auto_ack=auto_ack,
            cancellation_token=token,
        ):
            if response.is_error:
                if error_callback:
                    try:
                        await error_callback(Exception(response.error))
                    except Exception:
                        _logger.exception("Error in error_callback itself")
                continue

            for message in response.messages:
                try:
                    await callback(message)
                except Exception as handler_err:
                    handler_error = KubeMQHandlerError(
                        f"Message handler raised {type(handler_err).__name__}: {handler_err}",
                        cause=handler_err,
                        operation="MessageHandler",
                        channel=channel,
                    )
                    if error_callback:
                        try:
                            await error_callback(handler_error)
                        except Exception:
                            _logger.exception("Error in error_callback itself")
                    else:
                        _logger.error("Unhandled handler error: %s", handler_error)

            if not auto_ack and not response.is_transaction_completed:
                try:
                    await response.ack_all()
                except Exception as e:
                    if error_callback:
                        await error_callback(e)

    # =========================================================================
    # Queue Management
    # =========================================================================

    async def ack_all_queue_messages(
        self,
        channel: str,
        wait_time_seconds: int = 60,
    ) -> int:
        """Acknowledge all messages in a queue.

        Args:
            channel: Queue channel to ack all messages.
            wait_time_seconds: How long the server should wait for messages
                to ack (in seconds).

        Returns:
            int: The number of messages that were acknowledged.

        Raises:
            KubeMQMessageError: If the server returns an error response
                (e.g., no messages to acknowledge).
            KubeMQConnectionError: If the server is unreachable or the
                connection is lost.
            KubeMQAuthenticationError: If the auth token is invalid or the
                client lacks permission for the channel.
            KubeMQTimeoutError: If the operation exceeds the server deadline.
            KubeMQClientClosedError: If the client has already been closed.

        See Also:
            :meth:`QueuesClient.ack_all_queue_messages`: Sync counterpart.
        """
        self._ensure_connected()
        assert self._transport is not None

        request = pb.AckAllQueueMessagesRequest()
        request.RequestID = str(uuid.uuid4())
        request.ClientID = self._config.client_id or ""
        request.Channel = channel
        request.WaitTimeSeconds = wait_time_seconds

        response = await self._transport.ack_all_queue_messages(request)

        if response.IsError:
            raise KubeMQMessageError(
                response.Error,
                operation="AckAllQueueMessages",
                channel=channel,
            )

        return response.AffectedMessages


# Alias for backward compatibility and clearer naming
AsyncQueuesClient = AsyncClient
