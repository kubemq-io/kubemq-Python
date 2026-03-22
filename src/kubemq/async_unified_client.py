"""Unified asynchronous client for all KubeMQ operations.

Provides PubSub (events/events store), Commands & Queries, and Queues
through a single ``AsyncClient`` instance with one native-async gRPC connection.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections.abc import AsyncIterator
from typing import Any

import dataclasses

from kubemq._internal.retry import BackoffCalculator
from kubemq._internal.telemetry import (
    KubeMQTagsCarrier,
    create_link_from_context,
    error_code_to_error_type,
    serialize_span_to_bytes,
)
from kubemq.common.async_cancellation_token import AsyncCancellationToken
from kubemq.common.channel_stats import CQChannel, PubSubChannel, QueuesChannel
from kubemq.core.client import NativeAsyncBaseClient
from kubemq.core.config import ClientConfig
from kubemq.core.exceptions import (
    KubeMQError,
    KubeMQHandlerError,
    KubeMQMessageError,
    KubeMQValidationError,
)

# Stable message types
from kubemq.cq.command_message import CommandMessage
from kubemq.cq.command_message_received import CommandReceived
from kubemq.cq.command_response_message import CommandResponse
from kubemq.cq.commands_subscription import CommandsSubscription
from kubemq.cq.queries_subscription import QueriesSubscription
from kubemq.cq.query_message import QueryMessage
from kubemq.cq.query_message_received import QueryReceived
from kubemq.cq.query_response_message import QueryResponse
from kubemq.grpc import kubemq_pb2 as pb
from kubemq.pubsub.async_event_sender import AsyncEventSender
from kubemq.pubsub.event_message import EventMessage
from kubemq.pubsub.event_message_received import EventReceived
from kubemq.pubsub.event_send_result import EventStoreResult
from kubemq.pubsub.event_store_message import EventStoreMessage
from kubemq.pubsub.event_store_message_received import EventStoreReceived
from kubemq.pubsub.events_store_subscription import EventsStoreSubscription, EventStoreStartPosition
from kubemq.pubsub.events_subscription import EventsSubscription

# Re-use the async poll response from the queues async client
from kubemq.queues.async_client import AsyncQueuesPollResponse
from kubemq.queues.async_upstream_sender import AsyncUpstreamSender
from kubemq.queues.queues_message import QueueMessage
from kubemq.queues.queues_message_received import QueueMessageReceived
from kubemq.queues.queues_send_result import QueueBatchSendResult, QueueSendResult

_logger = logging.getLogger("kubemq.unified.async_client")

_REQUESTS_CHANNEL = "kubemq.cluster.internal.requests"


# =========================================================================
# PY-14 / PY-15: Async stream handles
# =========================================================================


class AsyncEventStreamHandle:
    """Handle returned by :meth:`AsyncClient.send_event_stream` for streaming events."""

    def __init__(self, sender: AsyncEventSender, instrumentor: Any, client_id: str) -> None:
        self._sender = sender
        self._instrumentor = instrumentor
        self._client_id = client_id
        self._closed = False

    async def send(self, message: EventMessage) -> None:
        """Send an event through the persistent stream."""
        if self._closed:
            raise KubeMQError("Stream handle is closed")
        pb_event = message.encode(self._client_id)
        await self._sender.send(pb_event)
        self._instrumentor._metrics.record_sent_message("publish", message.channel)

    def close(self) -> None:
        """Close the event stream handle."""
        self._closed = True


class AsyncEventStoreStreamHandle:
    """Handle returned by :meth:`AsyncClient.send_event_store_stream` for streaming event-store messages."""

    def __init__(self, sender: AsyncEventSender, instrumentor: Any, client_id: str) -> None:
        self._sender = sender
        self._instrumentor = instrumentor
        self._client_id = client_id
        self._closed = False

    async def send(self, message: EventStoreMessage) -> EventStoreResult:
        """Send an event-store message through the persistent stream."""
        if self._closed:
            raise KubeMQError("Stream handle is closed")
        pb_event = message.encode(self._client_id)
        result = await self._sender.send(pb_event)
        self._instrumentor._metrics.record_sent_message("publish", message.channel)
        return EventStoreResult.decode(result) if result else EventStoreResult()

    def close(self) -> None:
        """Close the event-store stream handle."""
        self._closed = True


# =========================================================================
# Unified async client
# =========================================================================


class AsyncClient(NativeAsyncBaseClient):
    """Unified native-async client for all KubeMQ operations.

    Consolidates PubSub, Commands & Queries, and Queues into a single
    async client with one native-async gRPC connection.
    Supports ``async with`` (PY-02) and exposes a ``state`` property (PY-03).

    Example::

        async with AsyncClient(address="localhost:50000") as client:
            await client.send_event(EventMessage(channel="events", body=b"hi"))

            resp = await client.send_command(CommandMessage(
                channel="cmds", body=b"do", timeout_in_seconds=30,
            ))

            result = await client.send_queue_message(QueueMessage(
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
        self._event_sender: AsyncEventSender | None = None
        self._upstream_sender: AsyncUpstreamSender | None = None

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

    async def _get_event_sender(self) -> AsyncEventSender:
        if self._event_sender is None:
            self._ensure_connected()
            assert self._transport is not None
            self._event_sender = AsyncEventSender(self._transport)
            await self._event_sender.start()
        return self._event_sender

    async def _get_upstream_sender(self) -> AsyncUpstreamSender:
        if self._upstream_sender is None:
            self._ensure_connected()
            assert self._transport is not None
            self._upstream_sender = AsyncUpstreamSender(self._transport)
            await self._upstream_sender.start()
        return self._upstream_sender

    async def close(self) -> None:
        """Close the client and release all resources."""
        if self._event_sender is not None:
            await self._event_sender.close()
            self._event_sender = None
        if self._upstream_sender is not None:
            await self._upstream_sender.close()
            self._upstream_sender = None
        await super().close()

    # =================================================================
    # PubSub – Event Operations
    # =================================================================

    async def send_event(self, message: EventMessage) -> None:
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
                result = await self._transport.send_event(pb_event)
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

    async def send_event_store(self, message: EventStoreMessage) -> EventStoreResult:
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
                sender = await self._get_event_sender()
                result = await sender.send(pb_event)
                self._instrumentor._metrics.record_sent_message("publish", message.channel)
                return EventStoreResult.decode(result) if result else EventStoreResult()
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

    async def send_event_stream(self) -> AsyncEventStreamHandle:
        """Return a persistent async stream handle for sending events (PY-14)."""
        sender = await self._get_event_sender()
        return AsyncEventStreamHandle(sender, self._instrumentor, self._config.client_id or "")

    async def send_event_store_stream(self) -> AsyncEventStoreStreamHandle:
        """Return a persistent async stream handle for sending event-store messages (PY-15)."""
        sender = await self._get_event_sender()
        return AsyncEventStoreStreamHandle(sender, self._instrumentor, self._config.client_id or "")

    # =================================================================
    # PubSub – Subscriptions
    # =================================================================

    async def subscribe_to_events(
        self,
        subscription: EventsSubscription,
        cancellation_token: AsyncCancellationToken | None = None,
    ) -> AsyncIterator[EventReceived]:
        """Subscribe to events with automatic stream reconnection."""
        self._ensure_connected()
        assert self._transport is not None

        token = cancellation_token or AsyncCancellationToken()
        await self._register_subscription(token)

        backoff = BackoffCalculator(self._config.retry_policy)
        attempt = 0

        try:
            while not token.is_cancelled:
                try:
                    request = subscription.encode(self._config.client_id or "")
                    async for pb_event in self._transport.subscribe_to_events(request, token):
                        attempt = 0
                        start = time.perf_counter()
                        error_type_val = None
                        tags_dict = dict(pb_event.Tags) if hasattr(pb_event, "Tags") else {}
                        carrier = KubeMQTagsCarrier(tags_dict)
                        parent_ctx = carrier.extract()
                        links = []
                        link = create_link_from_context(parent_ctx)
                        if link is not None:
                            links.append(link)
                        with self._instrumentor.start_span(
                            "process", subscription.channel, links=links or None
                        ) as span:
                            try:
                                event = EventReceived.decode(pb_event)
                                if subscription.on_receive_event_callback is not None:
                                    try:
                                        if asyncio.iscoroutinefunction(
                                            subscription.on_receive_event_callback
                                        ):
                                            await subscription.on_receive_event_callback(event)
                                        else:
                                            subscription.on_receive_event_callback(event)
                                    except Exception as handler_err:
                                        handler_error = KubeMQHandlerError(
                                            f"Message handler raised {type(handler_err).__name__}: {handler_err}",
                                            cause=handler_err,
                                            operation="MessageHandler",
                                        )
                                        if subscription.on_error_callback:
                                            try:
                                                if asyncio.iscoroutinefunction(
                                                    subscription.on_error_callback
                                                ):
                                                    await subscription.on_error_callback(
                                                        str(handler_error)
                                                    )
                                                else:
                                                    subscription.on_error_callback(
                                                        str(handler_error)
                                                    )
                                            except Exception:
                                                _logger.exception(
                                                    "Error in on_error callback itself"
                                                )
                                self._instrumentor._metrics.record_consumed_message(
                                    "process", subscription.channel
                                )
                            except Exception as proc_err:
                                error_type_val = error_code_to_error_type(
                                    getattr(proc_err, "code", None)
                                )
                                self._instrumentor.record_error(span, proc_err, error_type_val)
                                raise
                            finally:
                                duration = time.perf_counter() - start
                                self._instrumentor._metrics.record_operation_duration(
                                    duration, "process", subscription.channel, error_type_val
                                )

                        yield event
                    break

                except KubeMQError as e:
                    if not e.is_retryable or token.is_cancelled:
                        raise
                    delay = backoff.delay_seconds(attempt)
                    attempt += 1
                    await asyncio.sleep(delay)

                except Exception:
                    raise
        finally:
            await self._unregister_subscription(token)

    async def subscribe_to_events_store(
        self,
        subscription: EventsStoreSubscription,
        cancellation_token: AsyncCancellationToken | None = None,
    ) -> AsyncIterator[EventStoreReceived]:
        """Subscribe to events store with automatic stream reconnection."""
        self._ensure_connected()
        assert self._transport is not None

        token = cancellation_token or AsyncCancellationToken()
        await self._register_subscription(token)

        backoff = BackoffCalculator(self._config.retry_policy)
        attempt = 0
        last_sequence = 0

        try:
            while not token.is_cancelled:
                try:
                    if last_sequence > 0:
                        resume_sub = dataclasses.replace(
                            subscription,
                            events_store_type=EventStoreStartPosition.StartAtSequence,
                            events_store_sequence_value=last_sequence + 1,
                        )
                        request = resume_sub.encode(self._config.client_id or "")
                    else:
                        request = subscription.encode(self._config.client_id or "")

                    async for pb_event in self._transport.subscribe_to_events(request, token):
                        attempt = 0
                        start = time.perf_counter()
                        error_type_val = None
                        tags_dict = dict(pb_event.Tags) if hasattr(pb_event, "Tags") else {}
                        carrier = KubeMQTagsCarrier(tags_dict)
                        parent_ctx = carrier.extract()
                        links = []
                        link = create_link_from_context(parent_ctx)
                        if link is not None:
                            links.append(link)
                        with self._instrumentor.start_span(
                            "process", subscription.channel, links=links or None
                        ) as span:
                            try:
                                event = EventStoreReceived.decode(pb_event)
                                if event.sequence > 0:
                                    last_sequence = event.sequence
                                if subscription.on_receive_event_callback is not None:
                                    try:
                                        if asyncio.iscoroutinefunction(
                                            subscription.on_receive_event_callback
                                        ):
                                            await subscription.on_receive_event_callback(event)
                                        else:
                                            subscription.on_receive_event_callback(event)
                                    except Exception as handler_err:
                                        handler_error = KubeMQHandlerError(
                                            f"Message handler raised {type(handler_err).__name__}: {handler_err}",
                                            cause=handler_err,
                                            operation="MessageHandler",
                                        )
                                        if subscription.on_error_callback:
                                            try:
                                                if asyncio.iscoroutinefunction(
                                                    subscription.on_error_callback
                                                ):
                                                    await subscription.on_error_callback(
                                                        str(handler_error)
                                                    )
                                                else:
                                                    subscription.on_error_callback(
                                                        str(handler_error)
                                                    )
                                            except Exception:
                                                _logger.exception(
                                                    "Error in on_error callback itself"
                                                )
                                self._instrumentor._metrics.record_consumed_message(
                                    "process", subscription.channel
                                )
                            except Exception as proc_err:
                                error_type_val = error_code_to_error_type(
                                    getattr(proc_err, "code", None)
                                )
                                self._instrumentor.record_error(span, proc_err, error_type_val)
                                raise
                            finally:
                                duration = time.perf_counter() - start
                                self._instrumentor._metrics.record_operation_duration(
                                    duration, "process", subscription.channel, error_type_val
                                )

                        yield event
                    break

                except KubeMQError as e:
                    if not e.is_retryable or token.is_cancelled:
                        raise
                    delay = backoff.delay_seconds(attempt)
                    attempt += 1
                    await asyncio.sleep(delay)

                except Exception:
                    raise
        finally:
            await self._unregister_subscription(token)

    # =================================================================
    # CQ – Command & Query Operations  (PY-13: split response methods)
    # =================================================================

    async def send_command(self, message: CommandMessage) -> CommandResponse:
        """Send a command and wait for response."""
        self._validate_message_size(message.body)
        start = time.perf_counter()
        error_type_val = None
        with self._instrumentor.start_span("send", message.channel) as span:
            try:
                self._ensure_connected()
                assert self._transport is not None
                span_bytes = serialize_span_to_bytes()
                pb_request = message.encode(self._config.client_id or "", span=span_bytes)
                tags_dict = dict(pb_request.Tags)
                KubeMQTagsCarrier(tags_dict).inject()
                pb_request.Tags.update(tags_dict)
                if span.is_recording():
                    from kubemq._internal.semconv import (
                        MESSAGING_MESSAGE_BODY_SIZE,
                        MESSAGING_MESSAGE_ID,
                    )

                    span.set_attribute(MESSAGING_MESSAGE_ID, message.id)
                    span.set_attribute(MESSAGING_MESSAGE_BODY_SIZE, len(message.body))
                response = await self._retry_executor.execute(
                    "SendCommand",
                    self._transport.send_request,
                    pb_request,
                    timeout_seconds=message.timeout_in_seconds,
                    channel=message.channel,
                )
                self._instrumentor._metrics.record_sent_message("send", message.channel)
                return CommandResponse.decode(response)
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

    async def send_query(self, message: QueryMessage) -> QueryResponse:
        """Send a query and wait for response."""
        self._validate_message_size(message.body)
        start = time.perf_counter()
        error_type_val = None
        with self._instrumentor.start_span("send", message.channel) as span:
            try:
                self._ensure_connected()
                assert self._transport is not None
                span_bytes = serialize_span_to_bytes()
                pb_request = message.encode(self._config.client_id or "", span=span_bytes)
                tags_dict = dict(pb_request.Tags)
                KubeMQTagsCarrier(tags_dict).inject()
                pb_request.Tags.update(tags_dict)
                if span.is_recording():
                    from kubemq._internal.semconv import (
                        MESSAGING_MESSAGE_BODY_SIZE,
                        MESSAGING_MESSAGE_ID,
                    )

                    span.set_attribute(MESSAGING_MESSAGE_ID, message.id)
                    span.set_attribute(MESSAGING_MESSAGE_BODY_SIZE, len(message.body))
                response = await self._retry_executor.execute(
                    "SendQuery",
                    self._transport.send_request,
                    pb_request,
                    timeout_seconds=message.timeout_in_seconds,
                    channel=message.channel,
                )
                self._instrumentor._metrics.record_sent_message("send", message.channel)
                return QueryResponse.decode(response)
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

    async def send_command_response(self, response: CommandResponse) -> None:
        """Send a response to a command request (PY-13)."""
        await self._send_cq_response(response)

    async def send_query_response(self, response: QueryResponse) -> None:
        """Send a response to a query request (PY-13)."""
        await self._send_cq_response(response)

    async def send_response_message(self, message: CommandResponse | QueryResponse) -> None:
        """Send a command/query response (backward-compatible combined method)."""
        await self._send_cq_response(message)

    async def _send_cq_response(self, message: CommandResponse | QueryResponse) -> None:
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
                await self._retry_executor.execute(
                    "SendResponse",
                    self._transport.send_response,
                    pb_response,
                    channel=channel,
                )
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

    async def subscribe_to_commands(
        self,
        subscription: CommandsSubscription,
        cancellation_token: AsyncCancellationToken | None = None,
    ) -> AsyncIterator[CommandReceived]:
        """Subscribe to commands as an async iterator."""
        self._ensure_connected()
        assert self._transport is not None

        token = cancellation_token or AsyncCancellationToken()
        await self._register_subscription(token)

        try:
            request = subscription.encode(self._config.client_id or "")
            async for pb_request in self._transport.subscribe_to_requests(request, token):
                start = time.perf_counter()
                error_type_val = None
                tags_dict = dict(pb_request.Tags) if hasattr(pb_request, "Tags") else {}
                carrier = KubeMQTagsCarrier(tags_dict)
                parent_ctx = carrier.extract()
                links = []
                link = create_link_from_context(parent_ctx)
                if link is not None:
                    links.append(link)
                with self._instrumentor.start_span(
                    "process", subscription.channel, links=links or None
                ) as span:
                    try:
                        command = CommandReceived.decode(pb_request)
                        if subscription.on_receive_command_callback is not None:
                            if asyncio.iscoroutinefunction(
                                subscription.on_receive_command_callback
                            ):
                                await subscription.on_receive_command_callback(command)
                            else:
                                subscription.on_receive_command_callback(command)
                        self._instrumentor._metrics.record_consumed_message(
                            "process", subscription.channel
                        )
                    except Exception as proc_err:
                        error_type_val = error_code_to_error_type(getattr(proc_err, "code", None))
                        self._instrumentor.record_error(span, proc_err, error_type_val)
                        raise
                    finally:
                        duration = time.perf_counter() - start
                        self._instrumentor._metrics.record_operation_duration(
                            duration, "process", subscription.channel, error_type_val
                        )
                yield command

        except Exception as e:
            if subscription.on_error_callback:
                error_msg = str(e)
                if asyncio.iscoroutinefunction(subscription.on_error_callback):
                    await subscription.on_error_callback(error_msg)
                else:
                    subscription.on_error_callback(error_msg)
            raise
        finally:
            await self._unregister_subscription(token)

    async def subscribe_to_queries(
        self,
        subscription: QueriesSubscription,
        cancellation_token: AsyncCancellationToken | None = None,
    ) -> AsyncIterator[QueryReceived]:
        """Subscribe to queries as an async iterator."""
        self._ensure_connected()
        assert self._transport is not None

        token = cancellation_token or AsyncCancellationToken()
        await self._register_subscription(token)

        try:
            request = subscription.encode(self._config.client_id or "")
            async for pb_request in self._transport.subscribe_to_requests(request, token):
                start = time.perf_counter()
                error_type_val = None
                tags_dict = dict(pb_request.Tags) if hasattr(pb_request, "Tags") else {}
                carrier = KubeMQTagsCarrier(tags_dict)
                parent_ctx = carrier.extract()
                links = []
                link = create_link_from_context(parent_ctx)
                if link is not None:
                    links.append(link)
                with self._instrumentor.start_span(
                    "process", subscription.channel, links=links or None
                ) as span:
                    try:
                        query = QueryReceived.decode(pb_request)
                        if subscription.on_receive_query_callback is not None:
                            if asyncio.iscoroutinefunction(subscription.on_receive_query_callback):
                                await subscription.on_receive_query_callback(query)
                            else:
                                subscription.on_receive_query_callback(query)
                        self._instrumentor._metrics.record_consumed_message(
                            "process", subscription.channel
                        )
                    except Exception as proc_err:
                        error_type_val = error_code_to_error_type(getattr(proc_err, "code", None))
                        self._instrumentor.record_error(span, proc_err, error_type_val)
                        raise
                    finally:
                        duration = time.perf_counter() - start
                        self._instrumentor._metrics.record_operation_duration(
                            duration, "process", subscription.channel, error_type_val
                        )
                yield query

        except Exception as e:
            if subscription.on_error_callback:
                error_msg = str(e)
                if asyncio.iscoroutinefunction(subscription.on_error_callback):
                    await subscription.on_error_callback(error_msg)
                else:
                    subscription.on_error_callback(error_msg)
            raise
        finally:
            await self._unregister_subscription(token)

    # =================================================================
    # Queue Operations
    # =================================================================

    async def send_queue_message(self, message: QueueMessage) -> QueueSendResult:
        """Send a message to a queue via bidirectional upstream stream."""
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

    async def send_queue_message_simple(self, message: QueueMessage) -> QueueSendResult:
        """Send a single queue message via unary ``SendQueueMessage`` RPC."""
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

    async def send_queue_messages_batch(self, messages: list[QueueMessage]) -> QueueBatchSendResult:
        """Send multiple queue messages as a server-side batch."""
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
        results = [QueueSendResult.decode(r) for r in batch_response.Results]
        return QueueBatchSendResult(
            batch_id=batch_response.BatchID,
            results=results,
            have_errors=batch_response.HaveErrors,
        )

    async def receive_queue_messages(
        self,
        channel: str,
        max_messages: int = 1,
        wait_timeout_seconds: int = 60,
        auto_ack: bool = False,
    ) -> AsyncQueuesPollResponse:
        """Receive messages from a queue (polling)."""
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
                        is_auto_acked=auto_ack,
                    )
                    for msg in response.Messages
                ]
                for _ in messages:
                    self._instrumentor._metrics.record_consumed_message("receive", channel)

                return AsyncQueuesPollResponse(
                    ref_request_id=response.RequestID,
                    transaction_id="",
                    messages=messages,
                    error=response.Error,
                    is_error=response.IsError,
                    is_transaction_completed=auto_ack,
                    active_offsets=[],
                    receiver_client_id=client_id,
                    is_auto_acked=auto_ack,
                    transport=self._transport,
                )
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
        """Peek at messages without removing them (renamed from ``waiting()``)."""
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
            QueueMessageReceived.decode(msg, "", True, client_id, None, is_auto_acked=True)
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

    async def ack_all_queue_messages(self, channel: str, wait_time_seconds: int = 60) -> int:
        """Acknowledge all messages in a queue."""
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
                response.Error, operation="AckAllQueueMessages", channel=channel
            )
        return response.AffectedMessages

    # =================================================================
    # PY-34: Generic Channel Management (async, uses transport directly)
    # =================================================================

    async def create_channel(self, name: str, channel_type: str) -> bool:
        """Create a channel.  *channel_type*: ``'events'``, ``'events_store'``, ``'commands'``, ``'queries'``, or ``'queues'``."""
        self._ensure_connected()
        assert self._transport is not None
        from kubemq.common.exceptions import CreateChannelError
        from kubemq.grpc import Request

        request = Request(
            RequestID=str(uuid.uuid4()),
            RequestTypeData=2,  # type: ignore[arg-type]
            Metadata="create-channel",
            Channel=_REQUESTS_CHANNEL,
            ClientID=self._config.client_id,
            Tags={
                "channel_type": channel_type,
                "channel": name,
                "client_id": self._config.client_id or "",
            },
            Timeout=10 * 1000,
        )
        response = await self._transport.send_request(request)
        if response and response.Executed:
            return True
        if response and response.Error:
            raise CreateChannelError(response.Error)
        return False

    async def delete_channel(self, name: str, channel_type: str) -> bool:
        """Delete a channel of the specified type."""
        self._ensure_connected()
        assert self._transport is not None
        from kubemq.common.exceptions import DeleteChannelError
        from kubemq.grpc import Request

        request = Request(
            RequestID=str(uuid.uuid4()),
            RequestTypeData=2,  # type: ignore[arg-type]
            Metadata="delete-channel",
            Channel=_REQUESTS_CHANNEL,
            ClientID=self._config.client_id,
            Tags={
                "channel_type": channel_type,
                "channel": name,
                "client_id": self._config.client_id or "",
            },
            Timeout=10 * 1000,
        )
        response = await self._transport.send_request(request)
        if response and response.Executed:
            return True
        if response and response.Error:
            raise DeleteChannelError(response.Error)
        return False

    async def list_channels(self, channel_type: str, search: str = "") -> list:
        """List channels of the specified type."""
        self._ensure_connected()
        assert self._transport is not None
        from kubemq.common.channel_stats import (
            decode_cq_channel_list,
            decode_pub_sub_channel_list,
            decode_queues_channel_list,
        )
        from kubemq.common.exceptions import ListChannelsError
        from kubemq.grpc import Request

        if channel_type in ("events", "events_store"):
            decode_fn = decode_pub_sub_channel_list
        elif channel_type in ("commands", "queries"):
            decode_fn = decode_cq_channel_list
        elif channel_type == "queues":
            decode_fn = decode_queues_channel_list
        else:
            raise ValueError(
                f"Unknown channel_type '{channel_type}'. "
                "Use 'events', 'events_store', 'commands', 'queries', or 'queues'."
            )

        request = Request(
            RequestID=str(uuid.uuid4()),
            RequestTypeData=2,  # type: ignore[arg-type]
            Metadata="list-channels",
            Channel=_REQUESTS_CHANNEL,
            ClientID=self._config.client_id,
            Tags={"channel_type": channel_type, "channel_search": search},
            Timeout=10 * 1000,
        )
        response = await self._transport.send_request(request)
        if response and response.Executed:
            return decode_fn(response.Body)
        if response and response.Error:
            raise ListChannelsError(response.Error)
        return []

    # =================================================================
    # PY-35: Typed Convenience Channel Methods
    # =================================================================

    async def create_events_channel(self, channel: str) -> bool:
        """Create an events channel."""
        return await self.create_channel(channel, "events")

    async def create_events_store_channel(self, channel: str) -> bool:
        """Create an events-store channel."""
        return await self.create_channel(channel, "events_store")

    async def delete_events_channel(self, channel: str) -> bool:
        """Delete an events channel."""
        return await self.delete_channel(channel, "events")

    async def delete_events_store_channel(self, channel: str) -> bool:
        """Delete an events-store channel."""
        return await self.delete_channel(channel, "events_store")

    async def list_events_channels(self, channel_search: str = "") -> list[PubSubChannel]:
        """List events channels, optionally filtered by *channel_search*."""
        return await self.list_channels("events", channel_search)

    async def list_events_store_channels(self, channel_search: str = "") -> list[PubSubChannel]:
        """List events-store channels, optionally filtered by *channel_search*."""
        return await self.list_channels("events_store", channel_search)

    async def create_commands_channel(self, channel: str) -> bool:
        """Create a commands channel."""
        return await self.create_channel(channel, "commands")

    async def create_queries_channel(self, channel: str) -> bool:
        """Create a queries channel."""
        return await self.create_channel(channel, "queries")

    async def delete_commands_channel(self, channel: str) -> bool:
        """Delete a commands channel."""
        return await self.delete_channel(channel, "commands")

    async def delete_queries_channel(self, channel: str) -> bool:
        """Delete a queries channel."""
        return await self.delete_channel(channel, "queries")

    async def list_commands_channels(self, channel_search: str = "") -> list[CQChannel]:
        """List commands channels, optionally filtered by *channel_search*."""
        return await self.list_channels("commands", channel_search)

    async def list_queries_channels(self, channel_search: str = "") -> list[CQChannel]:
        """List queries channels, optionally filtered by *channel_search*."""
        return await self.list_channels("queries", channel_search)

    async def create_queues_channel(self, channel: str) -> bool:
        """Create a queues channel."""
        return await self.create_channel(channel, "queues")

    async def delete_queues_channel(self, channel: str) -> bool:
        """Delete a queues channel."""
        return await self.delete_channel(channel, "queues")

    async def list_queues_channels(self, channel_search: str = "") -> list[QueuesChannel]:
        """List queues channels, optionally filtered by *channel_search*."""
        return await self.list_channels("queues", channel_search)
