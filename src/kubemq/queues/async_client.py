"""Native async Queues client for KubeMQ."""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections.abc import AsyncIterator, Awaitable
from typing import (
    TYPE_CHECKING,
    Callable,
)

from kubemq._internal.retry import BackoffCalculator
from kubemq._internal.telemetry import KubeMQTagsCarrier, error_code_to_error_type
from kubemq.common.async_cancellation_token import AsyncCancellationToken
from kubemq.core.client import NativeAsyncBaseClient
from kubemq.core.config import ClientConfig
from pydantic import ValidationError

from kubemq.core.exceptions import KubeMQHandlerError, KubeMQMessageError, KubeMQValidationError
from kubemq.grpc import kubemq_pb2 as pb
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
        visibility_seconds: int,
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
        self.visibility_seconds = visibility_seconds
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
        request_visibility_seconds: int = 0,
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
                visibility_seconds=request_visibility_seconds,
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
            visibility_seconds=request_visibility_seconds,
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
        **kwargs,
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

    async def _get_upstream_sender(self) -> AsyncUpstreamSender:
        """Lazily initialize the bidirectional upstream stream sender."""
        if self._upstream_sender is None:
            self._ensure_connected()
            assert self._transport is not None
            self._upstream_sender = AsyncUpstreamSender(self._transport)
            await self._upstream_sender.start()
        return self._upstream_sender

    async def close(self) -> None:
        """Close the client and its upstream sender."""
        if self._upstream_sender is not None:
            await self._upstream_sender.close()
            self._upstream_sender = None
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
            message: The queue message to send

        Returns:
            QueueSendResult with send confirmation
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
            except ValidationError as e:
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
            message: The queue message to send

        Returns:
            QueueSendResult with send confirmation
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
            except ValidationError as e:
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

    async def send_queue_messages_batch(
        self,
        messages: list[QueueMessage],
    ) -> QueueBatchSendResult:
        """Send multiple queue messages as a server-side batch.

        Uses the gRPC ``SendQueueMessagesBatch`` RPC for atomic batch tracking
        with ``BatchID`` correlation and aggregate ``HaveErrors`` flag.

        Args:
            messages: List of messages to send.

        Returns:
            QueueBatchSendResult with batch_id, have_errors, and per-message results.
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
        visibility_seconds: int = 0,
    ) -> AsyncQueuesPollResponse:
        """Receive messages from queue (polling).

        Args:
            channel: Queue channel to receive from
            max_messages: Maximum messages to receive
            wait_timeout_seconds: How long to wait for messages
            auto_ack: If True, messages are auto-acknowledged after receive
            visibility_seconds: Visibility timeout for messages

        Returns:
            AsyncQueuesPollResponse with received messages

        Note:
            The simple receive RPC doesn't support auto_ack at the protocol level.
            If auto_ack is True, messages are acknowledged after being received.
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
                self._ensure_connected()
                assert self._transport is not None
                client_id = self._config.client_id or ""

                request = pb.ReceiveQueueMessagesRequest()
                request.RequestID = str(uuid.uuid4())
                request.ClientID = client_id
                request.Channel = channel
                request.MaxNumberOfMessages = max_messages
                request.WaitTimeSeconds = wait_timeout_seconds
                request.IsPeak = False

                response = await self._transport.receive_queue_messages(request)

                messages = [
                    QueueMessageReceived.decode(
                        msg,
                        "",
                        True,
                        client_id,
                        None,
                        visibility_seconds=visibility_seconds,
                        is_auto_acked=auto_ack,
                    )
                    for msg in response.Messages
                ]

                for _ in messages:
                    self._instrumentor._metrics.record_consumed_message("receive", channel)

                poll_response = AsyncQueuesPollResponse(
                    ref_request_id=response.RequestID,
                    transaction_id="",
                    messages=messages,
                    error=response.Error,
                    is_error=response.IsError,
                    is_transaction_completed=auto_ack,
                    active_offsets=[],
                    receiver_client_id=client_id,
                    visibility_seconds=visibility_seconds,
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

        Args:
            channel: Queue channel to peek from
            max_messages: Maximum messages to peek
            wait_timeout_seconds: How long to wait for messages

        Returns:
            AsyncQueuesPollResponse with peeked messages
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
                visibility_seconds=0,
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
            visibility_seconds=0,
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
        visibility_seconds: int = 0,
        cancellation_token: AsyncCancellationToken | None = None,
    ) -> AsyncIterator[AsyncQueuesPollResponse]:
        """Subscribe to queue messages with exponential backoff on errors.

        This method continuously polls for messages and yields responses.
        Transient errors trigger exponential backoff; successful receives
        reset the backoff counter.

        Args:
            channel: Queue channel to subscribe to
            max_messages: Maximum messages per poll
            wait_timeout_seconds: Wait timeout per poll
            auto_ack: If True, messages are auto-acknowledged
            visibility_seconds: Visibility timeout for messages
            cancellation_token: Optional token to cancel subscription

        Yields:
            AsyncQueuesPollResponse for each poll batch
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
                        visibility_seconds=visibility_seconds,
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
            channel: Queue channel to process from
            callback: Async callback for each message
            error_callback: Optional async callback for errors
            max_messages: Maximum messages per poll
            wait_timeout_seconds: Wait timeout per poll
            auto_ack: If True, messages are auto-acknowledged
            cancellation_token: Optional token to cancel processing
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
            channel: Queue channel to ack all messages
            wait_time_seconds: How long the server should wait for messages to ack

        Returns:
            Number of messages acknowledged
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
