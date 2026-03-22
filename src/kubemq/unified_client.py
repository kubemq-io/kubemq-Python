"""Unified synchronous client for all KubeMQ operations.

Provides PubSub (events/events store), Commands & Queries, and Queues
through a single ``Client`` instance with one gRPC connection.
"""

from __future__ import annotations

import threading
import time
import uuid
from typing import Any

import grpc
import dataclasses

from kubemq._internal.retry import BackoffCalculator
from kubemq._internal.telemetry import (
    KubeMQTagsCarrier,
    create_link_from_context,
    error_code_to_error_type,
    serialize_span_to_bytes,
)
from kubemq.common.cancellation_token import CancellationToken
from kubemq.common.channel_stats import CQChannel, PubSubChannel, QueuesChannel
from kubemq.common.helpers import decode_grpc_error
from kubemq.common.requests import (
    create_channel_request,
    delete_channel_request,
    list_cq_channels,
    list_pubsub_channels,
    list_queues_channels,
)
from kubemq.core.client import BaseClient
from kubemq.core.config import ClientConfig
from kubemq.core.exceptions import (
    KubeMQError,
    KubeMQHandlerError,
    KubeMQStreamBrokenError,
    KubeMQValidationError,
    from_grpc_error as convert_grpc_error,
)

# Message types (stable names)
from kubemq.cq.command_message import CommandMessage
from kubemq.cq.command_message_received import CommandReceived
from kubemq.cq.command_response_message import CommandResponse
from kubemq.cq.commands_subscription import CommandsSubscription
from kubemq.cq.queries_subscription import QueriesSubscription
from kubemq.cq.query_message import QueryMessage
from kubemq.cq.query_message_received import QueryReceived
from kubemq.cq.query_response_message import QueryResponse
from kubemq.grpc import (
    QueuesDownstreamRequest,
    QueuesDownstreamRequestType,
    ReceiveQueueMessagesRequest,
)
from kubemq.pubsub.event_message import EventMessage
from kubemq.pubsub.event_message_received import EventReceived
from kubemq.pubsub.event_send_result import EventStoreResult
from kubemq.pubsub.event_sender import EventSender
from kubemq.pubsub.event_store_message import EventStoreMessage
from kubemq.pubsub.event_store_message_received import EventStoreReceived
from kubemq.pubsub.events_store_subscription import EventsStoreSubscription, EventStoreStartPosition
from kubemq.pubsub.events_subscription import EventsSubscription
from kubemq.queues.downstream_receiver import DownstreamReceiver
from kubemq.queues.queues_message import QueueMessage
from kubemq.queues.queues_messages_waiting_pulled import (
    QueueMessagesPulled,
    QueueMessagesWaiting,
    QueueMessageWaitingPulled,
)
from kubemq.queues.queues_poll_response import QueuesPollResponse
from kubemq.queues.queues_send_result import QueueBatchSendResult, QueueSendResult
from kubemq.queues.upstream_sender import UpstreamSender

# =========================================================================
# PY-14 / PY-15: Stream handles
# =========================================================================


class EventStreamHandle:
    """Handle returned by :meth:`Client.send_event_stream` for streaming events."""

    def __init__(self, sender: EventSender, instrumentor: Any, client_id: str) -> None:
        self._sender = sender
        self._instrumentor = instrumentor
        self._client_id = client_id
        self._closed = False

    def send(self, message: EventMessage) -> None:
        """Send an event through the persistent stream."""
        if self._closed:
            raise KubeMQError("Stream handle is closed")
        pb_event = message.encode(self._client_id)
        self._sender.send(pb_event)
        self._instrumentor._metrics.record_sent_message("publish", message.channel)

    def close(self) -> None:
        """Close the event stream handle."""
        self._closed = True


class EventStoreStreamHandle:
    """Handle returned by :meth:`Client.send_event_store_stream` for streaming event-store messages."""

    def __init__(self, sender: EventSender, instrumentor: Any, client_id: str) -> None:
        self._sender = sender
        self._instrumentor = instrumentor
        self._client_id = client_id
        self._closed = False

    def send(self, message: EventStoreMessage) -> EventStoreResult:
        """Send an event-store message through the persistent stream."""
        if self._closed:
            raise KubeMQError("Stream handle is closed")
        pb_event = message.encode(self._client_id)
        result = self._sender.send(pb_event)
        self._instrumentor._metrics.record_sent_message("publish", message.channel)
        if result is not None:
            return EventStoreResult().decode(result)
        return EventStoreResult()

    def close(self) -> None:
        """Close the event-store stream handle."""
        self._closed = True


# =========================================================================
# Unified synchronous client
# =========================================================================


class Client(BaseClient):
    """Unified synchronous client for all KubeMQ operations.

    Consolidates PubSub, Commands & Queries, and Queues into a single
    client with one gRPC connection.  Supports ``with`` statement
    (PY-02) and exposes a ``state`` property (PY-03).

    Example::

        with Client(address="localhost:50000") as client:
            client.send_event(EventMessage(channel="events", body=b"hi"))

            resp = client.send_command(CommandMessage(
                channel="cmds", body=b"do", timeout_in_seconds=30,
            ))

            result = client.send_queue_message(QueueMessage(
                channel="tasks", body=b"work",
            ))
    """

    def __init__(
        self,
        address: str = "",
        client_id: str | None = None,
        auth_token: str | None = None,
        config: ClientConfig | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            address=address,
            client_id=client_id,
            auth_token=auth_token,
            config=config,
            **kwargs,
        )
        self._event_sender: EventSender | None = None
        self._sender_lock = threading.Lock()

        self._upstream_sender: UpstreamSender | None = None
        self._downstream_receiver: DownstreamReceiver | None = None
        self._upstream_sender_lock = threading.Lock()
        self._downstream_receiver_lock = threading.Lock()

        self.connection = self._config.to_legacy_connection()

    # -----------------------------------------------------------------
    # PY-03: state property
    # -----------------------------------------------------------------

    @property
    def state(self) -> str:
        """Current connection state: ``'connected'``, ``'disconnected'``, or ``'closed'``."""
        if self._closed:
            return "closed"
        if self.is_connected:
            return "connected"
        return "disconnected"

    # -----------------------------------------------------------------
    # Resource lifecycle
    # -----------------------------------------------------------------

    def _cleanup_resources(self) -> None:
        self._event_sender = None
        if self._upstream_sender is not None:
            self._upstream_sender.close()
            self._upstream_sender = None
        if self._downstream_receiver is not None:
            self._downstream_receiver.close()
            self._downstream_receiver = None

    def _get_event_sender(self) -> EventSender:
        self._ensure_connected()
        assert self._transport is not None
        with self._sender_lock:
            if self._event_sender is None:
                self._event_sender = EventSender(
                    self._transport,
                    self._shutdown_event,
                    self._logger,
                    self.connection,
                    max_queue_size=self._config.max_send_queue_size,
                )
            return self._event_sender

    def _get_upstream_sender(self) -> UpstreamSender:
        self._ensure_connected()
        assert self._transport is not None
        with self._upstream_sender_lock:
            if self._upstream_sender is None:
                self._upstream_sender = UpstreamSender(
                    self._transport,
                    self._logger,
                    self.connection,
                    send_timeout=2.0,
                    max_queue_size=self._config.max_send_queue_size,
                )
            return self._upstream_sender

    def _get_downstream_receiver(self) -> DownstreamReceiver:
        self._ensure_connected()
        assert self._transport is not None
        with self._downstream_receiver_lock:
            if self._downstream_receiver is None:
                self._downstream_receiver = DownstreamReceiver(
                    self._transport,
                    self._logger,
                    self.connection,
                    max_queue_size=self._config.max_send_queue_size,
                )
            return self._downstream_receiver

    # =================================================================
    # PubSub – Event Operations
    # =================================================================

    def send_event(self, message: EventMessage) -> None:
        """Send an event via unary ``SendEvent`` RPC (PY-11: kept)."""
        self._validate_message_size(message.body)
        self._ensure_connected()
        assert self._transport is not None
        start = time.perf_counter()
        error_type_val = None
        with self._instrumentor.start_span("publish", message.channel) as span:
            try:
                pb_event = message.encode(self._config.client_id or "")
                tags_dict = dict(pb_event.Tags)
                KubeMQTagsCarrier(tags_dict).inject()
                pb_event.Tags.update(tags_dict)
                if span.is_recording():
                    from kubemq._internal.semconv import (
                        MESSAGING_MESSAGE_BODY_SIZE,
                        MESSAGING_MESSAGE_ID,
                    )

                    span.set_attribute(MESSAGING_MESSAGE_ID, message.id)
                    span.set_attribute(MESSAGING_MESSAGE_BODY_SIZE, len(message.body))
                result = self._transport.kubemq_client().SendEvent(pb_event)
                self._instrumentor._metrics.record_sent_message("publish", message.channel)
                if result and not result.Sent and result.Error:
                    raise KubeMQError(result.Error)
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
                    duration, "publish", message.channel, error_type_val
                )

    def send_event_store(self, message: EventStoreMessage) -> EventStoreResult:
        """Send a persistent event-store message (renamed from ``publish_event_store``)."""
        self._validate_message_size(message.body)
        start = time.perf_counter()
        error_type_val = None
        with self._instrumentor.start_span("publish", message.channel) as span:
            try:
                pb_event = message.encode(self._config.client_id or "")
                tags_dict = dict(pb_event.Tags)
                KubeMQTagsCarrier(tags_dict).inject()
                pb_event.Tags.update(tags_dict)
                if span.is_recording():
                    from kubemq._internal.semconv import (
                        MESSAGING_MESSAGE_BODY_SIZE,
                        MESSAGING_MESSAGE_ID,
                    )

                    span.set_attribute(MESSAGING_MESSAGE_ID, message.id)
                    span.set_attribute(MESSAGING_MESSAGE_BODY_SIZE, len(message.body))
                sender = self._get_event_sender()
                result = sender.send(pb_event)
                self._instrumentor._metrics.record_sent_message("publish", message.channel)
                if result is not None:
                    return EventStoreResult().decode(result)
                return EventStoreResult()
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
                    duration, "publish", message.channel, error_type_val
                )

    def send_event_stream(self) -> EventStreamHandle:
        """Return a persistent stream handle for sending events (PY-14)."""
        sender = self._get_event_sender()
        return EventStreamHandle(sender, self._instrumentor, self._config.client_id or "")

    def send_event_store_stream(self) -> EventStoreStreamHandle:
        """Return a persistent stream handle for sending event-store messages (PY-15)."""
        sender = self._get_event_sender()
        return EventStoreStreamHandle(sender, self._instrumentor, self._config.client_id or "")

    # =================================================================
    # PubSub – Subscriptions
    # =================================================================

    def subscribe_to_events(
        self, subscription: EventsSubscription, cancel: CancellationToken | None = None
    ) -> None:
        """Subscribe to events on a channel (background thread)."""
        self._ensure_connected()
        assert self._transport is not None
        if cancel is None:
            cancel = CancellationToken()
        transport = self._transport
        client_id = self._config.client_id or ""
        args = (
            lambda: transport.kubemq_client().SubscribeToEvents(subscription.encode(client_id)),
            lambda msg: subscription.raise_on_receive_message(EventReceived().decode(msg)),
            lambda err: subscription.raise_on_error(err),
            cancel.event,
            subscription.channel,
        )
        thread = threading.Thread(target=self._subscribe_loop, args=args, daemon=True)
        self._register_subscription_thread(thread)
        thread.start()

    def subscribe_to_events_store(
        self, subscription: EventsStoreSubscription, cancel: CancellationToken | None = None
    ) -> None:
        """Subscribe to events store on a channel (background thread)."""
        self._ensure_connected()
        assert self._transport is not None
        if cancel is None:
            cancel = CancellationToken()
        transport = self._transport
        client_id = self._config.client_id or ""
        last_seq = [0]

        def _make_stream() -> Any:
            sub = subscription
            if last_seq[0] > 0:
                sub = dataclasses.replace(
                    subscription,
                    events_store_type=EventStoreStartPosition.StartAtSequence,
                    events_store_sequence_value=last_seq[0] + 1,
                )
            return transport.kubemq_client().SubscribeToEvents(sub.encode(client_id))

        def _decode(msg: Any) -> None:
            received = EventStoreReceived().decode(msg)
            if received.sequence > 0:
                last_seq[0] = received.sequence
            subscription.raise_on_receive_message(received)

        args = (
            _make_stream,
            _decode,
            lambda err: subscription.raise_on_error(err),
            cancel.event,
            subscription.channel,
        )
        thread = threading.Thread(target=self._subscribe_loop, args=args, daemon=True)
        self._register_subscription_thread(thread)
        thread.start()

    # =================================================================
    # CQ – Command & Query Operations  (PY-13: split response methods)
    # =================================================================

    def send_command(self, message: CommandMessage) -> CommandResponse:
        """Send a command and wait for response."""
        self._validate_message_size(message.body)
        start = time.perf_counter()
        error_type_val = None
        with self._instrumentor.start_span("send", message.channel) as span:
            try:
                self._ensure_connected()
                assert self._transport is not None
                span_bytes = serialize_span_to_bytes()
                pb_req = message.encode(self._config.client_id or "", span=span_bytes)
                tags_dict = dict(pb_req.Tags)
                KubeMQTagsCarrier(tags_dict).inject()
                pb_req.Tags.update(tags_dict)
                if span.is_recording():
                    from kubemq._internal.semconv import (
                        MESSAGING_MESSAGE_BODY_SIZE,
                        MESSAGING_MESSAGE_ID,
                    )

                    span.set_attribute(MESSAGING_MESSAGE_ID, message.id)
                    span.set_attribute(MESSAGING_MESSAGE_BODY_SIZE, len(message.body))
                response = self._transport.kubemq_client().SendRequest(pb_req)
                self._instrumentor._metrics.record_sent_message("send", message.channel)
                return CommandResponse().decode(response)
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

    def send_query(self, message: QueryMessage) -> QueryResponse:
        """Send a query and wait for response."""
        self._validate_message_size(message.body)
        start = time.perf_counter()
        error_type_val = None
        with self._instrumentor.start_span("send", message.channel) as span:
            try:
                self._ensure_connected()
                assert self._transport is not None
                span_bytes = serialize_span_to_bytes()
                pb_req = message.encode(self._config.client_id or "", span=span_bytes)
                tags_dict = dict(pb_req.Tags)
                KubeMQTagsCarrier(tags_dict).inject()
                pb_req.Tags.update(tags_dict)
                if span.is_recording():
                    from kubemq._internal.semconv import (
                        MESSAGING_MESSAGE_BODY_SIZE,
                        MESSAGING_MESSAGE_ID,
                    )

                    span.set_attribute(MESSAGING_MESSAGE_ID, message.id)
                    span.set_attribute(MESSAGING_MESSAGE_BODY_SIZE, len(message.body))
                response = self._transport.kubemq_client().SendRequest(pb_req)
                self._instrumentor._metrics.record_sent_message("send", message.channel)
                return QueryResponse().decode(response)
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

    def send_command_response(self, response: CommandResponse) -> None:
        """Send a response to a command request (PY-13)."""
        self._send_cq_response(response)

    def send_query_response(self, response: QueryResponse) -> None:
        """Send a response to a query request (PY-13)."""
        self._send_cq_response(response)

    def send_response_message(self, message: CommandResponse | QueryResponse) -> None:
        """Send a command/query response (backward-compatible combined method)."""
        self._send_cq_response(message)

    def _send_cq_response(self, message: CommandResponse | QueryResponse) -> None:
        self._ensure_connected()
        assert self._transport is not None
        channel = getattr(message, "reply_channel", "") or ""
        start = time.perf_counter()
        error_type_val = None
        with self._instrumentor.start_span("settle", channel) as span:
            try:
                pb_response = message.encode(self._config.client_id or "")
                tags_dict = dict(pb_response.Tags)
                KubeMQTagsCarrier(tags_dict).inject()
                pb_response.Tags.update(tags_dict)
                self._transport.kubemq_client().SendResponse(pb_response)
                self._instrumentor._metrics.record_sent_message("settle", channel)
            except Exception as e:
                error_type_val = error_code_to_error_type(getattr(e, "code", None))
                self._instrumentor.record_error(span, e, error_type_val)
                raise
            finally:
                duration = time.perf_counter() - start
                self._instrumentor._metrics.record_operation_duration(
                    duration, "settle", channel, error_type_val
                )

    # =================================================================
    # CQ – Subscriptions
    # =================================================================

    def subscribe_to_commands(
        self, subscription: CommandsSubscription, cancel: CancellationToken | None = None
    ) -> None:
        """Subscribe to commands on a channel (background thread)."""
        self._ensure_connected()
        assert self._transport is not None
        if cancel is None:
            cancel = CancellationToken()
        transport = self._transport
        client_id = self._config.client_id or ""
        args = (
            lambda: transport.kubemq_client().SubscribeToRequests(subscription.encode(client_id)),
            lambda msg: subscription.raise_on_receive_message(CommandReceived().decode(msg)),
            lambda err: subscription.raise_on_error(err),
            cancel.event,
            subscription.channel,
        )
        thread = threading.Thread(target=self._subscribe_loop, args=args, daemon=True)
        self._register_subscription_thread(thread)
        thread.start()

    def subscribe_to_queries(
        self, subscription: QueriesSubscription, cancel: CancellationToken | None = None
    ) -> None:
        """Subscribe to queries on a channel (background thread)."""
        self._ensure_connected()
        assert self._transport is not None
        if cancel is None:
            cancel = CancellationToken()
        transport = self._transport
        client_id = self._config.client_id or ""
        args = (
            lambda: transport.kubemq_client().SubscribeToRequests(subscription.encode(client_id)),
            lambda msg: subscription.raise_on_receive_message(QueryReceived().decode(msg)),
            lambda err: subscription.raise_on_error(err),
            cancel.event,
            subscription.channel,
        )
        thread = threading.Thread(target=self._subscribe_loop, args=args, daemon=True)
        self._register_subscription_thread(thread)
        thread.start()

    # =================================================================
    # Queue Operations
    # =================================================================

    def send_queue_message(self, message: QueueMessage) -> QueueSendResult:
        """Send a message to a queue via streaming upstream."""
        self._validate_message_size(message.body)
        start = time.perf_counter()
        error_type_val = None
        with self._instrumentor.start_span("send", message.channel) as span:
            try:
                sender = self._get_upstream_sender()
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
                result = sender.send(pb_message)
                self._instrumentor._metrics.record_sent_message("send", message.channel)
                if result is None:
                    return QueueSendResult(is_error=True, error="Send failed - no response")
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

    def send_queue_message_simple(self, message: QueueMessage) -> QueueSendResult:
        """Send a single queue message via unary ``SendQueueMessage`` RPC."""
        self._validate_message_size(message.body)
        self._ensure_connected()
        assert self._transport is not None
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
                result = self._transport.kubemq_client().SendQueueMessage(pb_message)
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

    def send_queue_messages_batch(self, messages: list[QueueMessage]) -> QueueBatchSendResult:
        """Send multiple queue messages as a server-side batch."""
        self._ensure_connected()
        assert self._transport is not None
        from kubemq.grpc import QueueMessagesBatchRequest

        batch_id = str(uuid.uuid4())
        client_id = self._config.client_id or ""
        batch_request = QueueMessagesBatchRequest()
        batch_request.BatchID = batch_id
        for msg in messages:
            self._validate_message_size(msg.body)
            pb_msg = msg.encode_message(client_id)
            tags_dict = dict(pb_msg.Tags)
            KubeMQTagsCarrier(tags_dict).inject()
            pb_msg.Tags.update(tags_dict)
            batch_request.Messages.append(pb_msg)
        batch_response = self._transport.kubemq_client().SendQueueMessagesBatch(batch_request)
        results = [QueueSendResult.decode(r) for r in batch_response.Results]
        return QueueBatchSendResult(
            batch_id=batch_response.BatchID,
            results=results,
            have_errors=batch_response.HaveErrors,
        )

    def receive_queue_messages(
        self,
        channel: str | None = None,
        max_messages: int = 1,
        wait_timeout_in_seconds: int = 60,
        auto_ack: bool = False,
        metadata: dict[str, str] | None = None,
    ) -> QueuesPollResponse:
        """Receive messages from a queue channel."""
        if not self._config.client_id:
            raise ValueError("ClientID required for downstream operations")
        if max_messages < 1 or max_messages > 1024:
            raise ValueError("max_messages must be between 1 and 1024")
        if wait_timeout_in_seconds < 0 or wait_timeout_in_seconds > 3600:
            raise ValueError("wait_timeout_in_seconds must be between 0 and 3600")
        ch = channel or ""
        start = time.perf_counter()
        error_type_val = None
        with self._instrumentor.start_span("receive", ch) as span:
            try:
                receiver = self._get_downstream_receiver()
                client_id = self._config.client_id or ""
                request = QueuesDownstreamRequest()
                request.RequestID = str(uuid.uuid4())
                request.ClientID = client_id
                request.Channel = ch
                request.MaxItems = max_messages
                request.WaitTimeout = wait_timeout_in_seconds * 1000
                request.AutoAck = auto_ack
                request.RequestTypeData = QueuesDownstreamRequestType.Get
                if metadata:
                    for k, v in metadata.items():
                        request.Metadata[k] = v
                kubemq_response = receiver.send(request)
                if kubemq_response is None:
                    return QueuesPollResponse()
                response = QueuesPollResponse().decode(
                    response=kubemq_response,
                    receiver_client_id=client_id,
                    response_handler=receiver.send_without_response,
                    request_auto_ack=auto_ack,
                )
                if response.messages:
                    for _ in response.messages:
                        self._instrumentor._metrics.record_consumed_message("receive", ch)
                return response
            except Exception as e:
                error_type_val = error_code_to_error_type(getattr(e, "code", None))
                self._instrumentor.record_error(span, e, error_type_val)
                raise
            finally:
                duration = time.perf_counter() - start
                self._instrumentor._metrics.record_operation_duration(
                    duration, "receive", ch, error_type_val
                )

    def peek_queue_messages(
        self, channel: str, max_messages: int, wait_timeout_in_seconds: int
    ) -> QueueMessagesWaiting:
        """Peek at waiting messages without consuming them (renamed from ``waiting()``)."""
        if channel is None:
            raise ValueError("channel cannot be None.")
        if max_messages < 1 or max_messages > 1024:
            raise ValueError("max_messages must be between 1 and 1024.")
        if wait_timeout_in_seconds < 1 or wait_timeout_in_seconds > 3600:
            raise ValueError("wait_timeout_in_seconds must be between 1 and 3600.")
        self._ensure_connected()
        assert self._transport is not None
        client_id = self._config.client_id or ""
        request = ReceiveQueueMessagesRequest(
            RequestID=str(uuid.uuid4()),
            ClientID=client_id,
            Channel=channel,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_timeout_in_seconds,
            IsPeak=True,
        )
        response = self._transport.kubemq_client().ReceiveQueueMessages(request)
        waiting_messages = QueueMessagesWaiting(
            is_error=response.IsError,
            error=response.Error,
            messages_received=getattr(response, "MessagesReceived", 0),
            messages_expired=getattr(response, "MessagesExpired", 0),
            is_peak=getattr(response, "IsPeak", True),
        )
        if response.Messages:
            for msg in response.Messages:
                waiting_messages.messages.append(QueueMessageWaitingPulled.decode(msg, client_id))
        return waiting_messages

    def pull(
        self, channel: str, max_messages: int, wait_timeout_in_seconds: int
    ) -> QueueMessagesPulled:
        """Pull messages from a queue (retrieve and remove)."""
        if channel is None:
            raise ValueError("channel cannot be None.")
        if max_messages < 1 or max_messages > 1024:
            raise ValueError("max_messages must be between 1 and 1024.")
        if wait_timeout_in_seconds < 1 or wait_timeout_in_seconds > 3600:
            raise ValueError("wait_timeout_in_seconds must be between 1 and 3600.")
        self._ensure_connected()
        assert self._transport is not None
        client_id = self._config.client_id or ""
        request = ReceiveQueueMessagesRequest(
            RequestID=str(uuid.uuid4()),
            ClientID=client_id,
            Channel=channel,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_timeout_in_seconds,
            IsPeak=False,
        )
        response = self._transport.kubemq_client().ReceiveQueueMessages(request)
        pulled = QueueMessagesPulled(
            is_error=response.IsError,
            error=response.Error,
            messages_received=getattr(response, "MessagesReceived", 0),
            messages_expired=getattr(response, "MessagesExpired", 0),
            is_peak=getattr(response, "IsPeak", False),
        )
        if response.Messages:
            for msg in response.Messages:
                pulled.messages.append(QueueMessageWaitingPulled.decode(msg, client_id))
        return pulled

    def ack_all_queue_messages(self, channel: str, wait_time_seconds: int = 60) -> int:
        """Acknowledge all messages in a queue."""
        self._ensure_connected()
        assert self._transport is not None
        from kubemq.core.exceptions import KubeMQMessageError
        from kubemq.grpc import AckAllQueueMessagesRequest

        request = AckAllQueueMessagesRequest()
        request.RequestID = str(uuid.uuid4())
        request.ClientID = self._config.client_id or ""
        request.Channel = channel
        request.WaitTimeSeconds = wait_time_seconds
        response = self._transport.kubemq_client().AckAllQueueMessages(request)
        if response.IsError:
            raise KubeMQMessageError(
                response.Error, operation="AckAllQueueMessages", channel=channel
            )
        return int(response.AffectedMessages)

    # =================================================================
    # PY-34: Generic Channel Management
    # =================================================================

    def create_channel(self, name: str, channel_type: str) -> bool:
        """Create a channel.  *channel_type*: ``'events'``, ``'events_store'``, ``'commands'``, ``'queries'``, or ``'queues'``."""
        return create_channel_request(self._transport, self._config.client_id, name, channel_type)

    def delete_channel(self, name: str, channel_type: str) -> bool:
        """Delete a channel of the specified type."""
        return delete_channel_request(self._transport, self._config.client_id, name, channel_type)

    def list_channels(self, channel_type: str, search: str = "") -> list:
        """List channels of the specified type."""
        if channel_type in ("events", "events_store"):
            return list_pubsub_channels(
                self._transport, self._config.client_id, channel_type, search
            )
        if channel_type in ("commands", "queries"):
            return list_cq_channels(self._transport, self._config.client_id, channel_type, search)
        if channel_type == "queues":
            return list_queues_channels(self._transport, self._config.client_id, search)
        raise ValueError(
            f"Unknown channel_type '{channel_type}'. "
            "Use 'events', 'events_store', 'commands', 'queries', or 'queues'."
        )

    # =================================================================
    # PY-35: Typed Convenience Channel Methods
    # =================================================================

    def create_events_channel(self, channel: str) -> bool | None:
        """Create an events channel."""
        return create_channel_request(self._transport, self._config.client_id, channel, "events")

    def create_events_store_channel(self, channel: str) -> bool | None:
        """Create an events-store channel."""
        return create_channel_request(
            self._transport, self._config.client_id, channel, "events_store"
        )

    def delete_events_channel(self, channel: str) -> bool | None:
        """Delete an events channel."""
        return delete_channel_request(self._transport, self._config.client_id, channel, "events")

    def delete_events_store_channel(self, channel: str) -> bool | None:
        """Delete an events-store channel."""
        return delete_channel_request(
            self._transport, self._config.client_id, channel, "events_store"
        )

    def list_events_channels(self, channel_search: str = "") -> list[PubSubChannel]:
        """List events channels, optionally filtered by *channel_search*."""
        return list_pubsub_channels(
            self._transport, self._config.client_id, "events", channel_search
        )

    def list_events_store_channels(self, channel_search: str = "") -> list[PubSubChannel]:
        """List events-store channels, optionally filtered by *channel_search*."""
        return list_pubsub_channels(
            self._transport, self._config.client_id, "events_store", channel_search
        )

    def create_commands_channel(self, channel: str) -> bool | None:
        """Create a commands channel."""
        return create_channel_request(self._transport, self._config.client_id, channel, "commands")

    def create_queries_channel(self, channel: str) -> bool | None:
        """Create a queries channel."""
        return create_channel_request(self._transport, self._config.client_id, channel, "queries")

    def delete_commands_channel(self, channel: str) -> bool | None:
        """Delete a commands channel."""
        return delete_channel_request(self._transport, self._config.client_id, channel, "commands")

    def delete_queries_channel(self, channel: str) -> bool | None:
        """Delete a queries channel."""
        return delete_channel_request(self._transport, self._config.client_id, channel, "queries")

    def list_commands_channels(self, channel_search: str = "") -> list[CQChannel]:
        """List commands channels, optionally filtered by *channel_search*."""
        return list_cq_channels(self._transport, self._config.client_id, "commands", channel_search)

    def list_queries_channels(self, channel_search: str = "") -> list[CQChannel]:
        """List queries channels, optionally filtered by *channel_search*."""
        return list_cq_channels(self._transport, self._config.client_id, "queries", channel_search)

    def create_queues_channel(self, channel: str) -> bool | None:
        """Create a queues channel."""
        return create_channel_request(self._transport, self._config.client_id, channel, "queues")

    def delete_queues_channel(self, channel: str) -> bool | None:
        """Delete a queues channel."""
        return delete_channel_request(self._transport, self._config.client_id, channel, "queues")

    def list_queues_channels(self, channel_search: str = "") -> list[QueuesChannel]:
        """List queues channels, optionally filtered by *channel_search*."""
        return list_queues_channels(self._transport, self._config.client_id, channel_search)

    # =================================================================
    # Shared subscription infrastructure
    # =================================================================

    def _subscribe_loop(
        self,
        stream_callable: Any,
        decode_callable: Any,
        error_callable: Any,
        cancel_token: threading.Event,
        channel: str = "",
    ) -> None:
        """Background subscription loop with exponential backoff on stream breaks."""
        backoff = BackoffCalculator(self._config.retry_policy)
        attempt = 0

        while not cancel_token.is_set() and not self._shutdown_event.is_set():
            try:
                response = stream_callable()

                def _cancel_watcher(resp: Any = response) -> None:
                    while not cancel_token.is_set() and not self._shutdown_event.is_set():
                        cancel_token.wait(timeout=0.5)
                    try:
                        resp.cancel()
                    except Exception:
                        pass

                watcher = threading.Thread(target=_cancel_watcher, daemon=True)
                watcher.start()

                for message in response:
                    if cancel_token.is_set():
                        break
                    attempt = 0
                    start = time.perf_counter()
                    error_type_val = None
                    tags_dict = dict(message.Tags) if hasattr(message, "Tags") else {}
                    carrier = KubeMQTagsCarrier(tags_dict)
                    parent_ctx = carrier.extract()
                    links = []
                    link = create_link_from_context(parent_ctx)
                    if link is not None:
                        links.append(link)
                    with self._instrumentor.start_span(
                        "process", channel, links=links or None
                    ) as span:
                        try:
                            decode_callable(message)
                            self._instrumentor._metrics.record_consumed_message("process", channel)
                        except Exception as handler_err:
                            error_type_val = error_code_to_error_type(
                                getattr(handler_err, "code", None)
                            )
                            self._instrumentor.record_error(span, handler_err, error_type_val)
                            handler_error = KubeMQHandlerError(
                                f"Message handler raised {type(handler_err).__name__}: {handler_err}",
                                cause=handler_err,
                                operation="MessageHandler",
                            )
                            error_callable(str(handler_error))
                        finally:
                            duration = time.perf_counter() - start
                            self._instrumentor._metrics.record_operation_duration(
                                duration, "process", channel, error_type_val
                            )
            except grpc.RpcError as e:
                sdk_error = convert_grpc_error(e, operation="Subscribe")
                stream_error = KubeMQStreamBrokenError(
                    f"Stream broken: {sdk_error.message}",
                    operation=sdk_error.operation,
                    channel=sdk_error.channel,
                    cause=e,
                )
                error_callable(str(stream_error))
                if not sdk_error.is_retryable:
                    break
                delay = backoff.delay_seconds(attempt)
                attempt += 1
                cancel_token.wait(timeout=delay)
                continue
            except Exception as e:
                error_callable(decode_grpc_error(e))
                delay = backoff.delay_seconds(attempt)
                attempt += 1
                cancel_token.wait(timeout=delay)
                continue
