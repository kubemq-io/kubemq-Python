"""Native async PubSub client for KubeMQ."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterator, Awaitable
from typing import (
    TYPE_CHECKING,
    Callable,
)

from kubemq._internal.deprecation import deprecated_async
from kubemq._internal.retry import BackoffCalculator
from kubemq._internal.telemetry import KubeMQTagsCarrier, create_link_from_context, error_code_to_error_type
from kubemq.common.async_cancellation_token import AsyncCancellationToken
from kubemq.core.client import NativeAsyncBaseClient
from pydantic import ValidationError

from kubemq.core.config import ClientConfig
from kubemq.core.exceptions import (
    KubeMQError,
    KubeMQHandlerError,
    KubeMQStreamBrokenError,
    KubeMQValidationError,
)
from kubemq.pubsub.async_event_sender import AsyncEventSender
from kubemq.pubsub.event_message import EventMessage
from kubemq.pubsub.event_message_received import EventMessageReceived
from kubemq.pubsub.event_send_result import EventSendResult
from kubemq.pubsub.event_store_message import EventStoreMessage
from kubemq.pubsub.event_store_message_received import EventStoreMessageReceived
from kubemq.pubsub.events_store_subscription import EventsStoreSubscription, EventsStoreType
from kubemq.pubsub.events_subscription import EventsSubscription

if TYPE_CHECKING:
    pass

_logger = logging.getLogger("kubemq.pubsub.async_client")

# Type aliases for callbacks
AsyncEventCallback = Callable[[EventMessageReceived], Awaitable[None]]
AsyncEventStoreCallback = Callable[[EventStoreMessageReceived], Awaitable[None]]
AsyncErrorCallback = Callable[[Exception], Awaitable[None]]


class AsyncClient(NativeAsyncBaseClient):
    """Native async PubSub client.

    Provides fire-and-forget events, persistent events store,
    and subscription capabilities using native gRPC async.

    Example:
        async with AsyncClient(address="localhost:50000") as client:
            # Send event
            await client.send_event(EventMessage(
                channel="events",
                body=b"Hello!",
            ))

            # Subscribe to events
            token = AsyncCancellationToken()
            async for event in client.subscribe_to_events(
                EventsSubscription(
                    channel="events",
                    on_receive_event_callback=lambda e: print(e.body),
                ),
                cancellation_token=token,
            ):
                print(f"Received: {event.body}")
                if should_stop:
                    token.cancel()

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
        """Initialize the async PubSub client.

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
        self._event_sender: AsyncEventSender | None = None

    async def _get_event_sender(self) -> AsyncEventSender:
        """Lazily initialize the bidirectional event stream sender."""
        if self._event_sender is None:
            self._ensure_connected()
            assert self._transport is not None
            self._event_sender = AsyncEventSender(self._transport)
            await self._event_sender.start()
        return self._event_sender

    async def close(self) -> None:
        """Close the client and its event sender."""
        if self._event_sender is not None:
            await self._event_sender.close()
            self._event_sender = None
        await super().close()

    # =========================================================================
    # Send Operations — GS-aligned verbs
    # =========================================================================

    async def publish_event(self, message: EventMessage) -> None:
        """Publish a fire-and-forget event via bidirectional stream.

        Uses ``SendEventsStream`` for high-throughput, fire-and-forget delivery.

        Args:
            message: The event message to publish.

        Raises:
            KubeMQConnectionError: If not connected.
            KubeMQValidationError: If message is invalid.
        """
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
                await sender.send(pb_event)
                self._instrumentor._metrics.record_sent_message("publish", message.channel)
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
                    duration, "publish", message.channel, error_type_val
                )

    async def send_event_unary(self, message: EventMessage) -> None:
        """Send an event message via unary SendEvent RPC.

        Uses the unary ``SendEvent`` RPC for single-shot delivery.

        Args:
            message: The event message to send.

        Raises:
            KubeMQValidationError: If message is invalid.
            KubeMQError: If the server returns Sent=false.
        """
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
                    duration, "publish", message.channel, error_type_val
                )

    @deprecated_async(replacement="publish_event()", since="4.0.0", removal="5.0.0")
    async def send_event(self, message: EventMessage) -> None:
        """Send a fire-and-forget event.

        Deprecated:
            Use ``publish_event()`` instead. Will be removed in v5.0.
        """
        return await self.publish_event(message)

    async def publish_event_store(self, message: EventStoreMessage) -> EventSendResult:
        """Publish a persistent event store message.

        Args:
            message: The event store message to publish.

        Returns:
            EventSendResult with persistence confirmation.
        """
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
                return EventSendResult.decode(result) if result else EventSendResult()
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
                    duration, "publish", message.channel, error_type_val
                )

    @deprecated_async(replacement="publish_event_store()", since="4.0.0", removal="5.0.0")
    async def send_event_store(self, message: EventStoreMessage) -> EventSendResult:
        """Send an event to events store (persistent).

        Deprecated:
            Use ``publish_event_store()`` instead. Will be removed in v5.0.
        """
        return await self.publish_event_store(message)

    async def send_events_batch(
        self,
        messages: list[EventMessage],
        max_concurrent: int = 100,
    ) -> list[EventSendResult]:
        """Send multiple events concurrently with backpressure control.

        Args:
            messages: List of events to send
            max_concurrent: Maximum concurrent sends (default 100)

        Returns:
            List of results (success or error for each)
        """
        self._ensure_connected()

        # Use semaphore for backpressure
        semaphore = asyncio.Semaphore(max_concurrent)

        async def bounded_send(msg: EventMessage, index: int) -> tuple[int, EventSendResult]:
            async with semaphore:
                try:
                    await self.publish_event(msg)
                    return (
                        index,
                        EventSendResult(
                            id=msg.id,
                            sent=True,
                            error="",
                        ),
                    )
                except Exception as e:
                    return (
                        index,
                        EventSendResult(
                            id=msg.id,
                            sent=False,
                            error=str(e),
                        ),
                    )

        tasks = [bounded_send(msg, i) for i, msg in enumerate(messages)]
        indexed_results = await asyncio.gather(*tasks)

        # Sort by original index
        sorted_results = sorted(indexed_results, key=lambda x: x[0])
        return [result for _, result in sorted_results]

    async def send_events_store_batch(
        self,
        messages: list[EventStoreMessage],
        max_concurrent: int = 100,
    ) -> list[EventSendResult]:
        """Send multiple events store messages concurrently with backpressure control.

        Args:
            messages: List of event store messages to send
            max_concurrent: Maximum concurrent sends (default 100)

        Returns:
            List of results for each message
        """
        self._ensure_connected()

        semaphore = asyncio.Semaphore(max_concurrent)

        async def bounded_send(msg: EventStoreMessage, index: int) -> tuple[int, EventSendResult]:
            async with semaphore:
                try:
                    result = await self.publish_event_store(msg)
                    return (index, result)
                except Exception as e:
                    return (
                        index,
                        EventSendResult(
                            id=msg.id,
                            sent=False,
                            error=str(e),
                        ),
                    )

        tasks = [bounded_send(msg, i) for i, msg in enumerate(messages)]
        indexed_results = await asyncio.gather(*tasks)

        sorted_results = sorted(indexed_results, key=lambda x: x[0])
        return [result for _, result in sorted_results]

    # =========================================================================
    # Subscription Operations
    # =========================================================================

    async def subscribe_to_events(
        self,
        subscription: EventsSubscription,
        cancellation_token: AsyncCancellationToken | None = None,
    ) -> AsyncIterator[EventMessageReceived]:
        """Subscribe to events with automatic stream reconnection.

        Stream-level transient errors trigger re-subscribe with exponential
        backoff. Per-message handler errors are isolated and do not terminate
        the stream.

        Args:
            subscription: Subscription configuration
            cancellation_token: Optional token to cancel subscription

        Yields:
            EventMessageReceived for each event

        Cancellation:
            Pass an ``AsyncCancellationToken`` to cancel the subscription.
            When cancelled, the async iterator terminates. If an
            ``asyncio.CancelledError`` is raised by task cancellation,
            it propagates normally per Python asyncio conventions.

        Timeout:
            Use ``asyncio.wait_for()`` to apply a timeout::

                await asyncio.wait_for(
                    client.subscribe_to_events(sub, token),
                    timeout=60.0,
                )

        Example:
            token = AsyncCancellationToken()
            async for event in client.subscribe_to_events(sub, token):
                process(event)
                if should_stop:
                    token.cancel()
        """
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
                                event = EventMessageReceived.decode(pb_event)

                                if subscription.on_receive_event_callback:
                                    try:
                                        if asyncio.iscoroutinefunction(subscription.on_receive_event_callback):
                                            await subscription.on_receive_event_callback(event)
                                        else:
                                            subscription.on_receive_event_callback(event)
                                    except Exception as handler_err:
                                        handler_error = KubeMQHandlerError(
                                            f"Message handler raised {type(handler_err).__name__}: {handler_err}",
                                            cause=handler_err,
                                            operation="MessageHandler",
                                            channel=subscription.channel if hasattr(subscription, "channel") else None,
                                        )
                                        if subscription.on_error_callback:
                                            try:
                                                if asyncio.iscoroutinefunction(subscription.on_error_callback):
                                                    await subscription.on_error_callback(str(handler_error))
                                                else:
                                                    subscription.on_error_callback(str(handler_error))
                                            except Exception:
                                                _logger.exception("Error in on_error callback itself")
                                        else:
                                            _logger.error("Unhandled handler error: %s", handler_error)

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

                    break  # clean stream exit

                except KubeMQError as e:
                    if not e.is_retryable or token.is_cancelled:
                        if subscription.on_error_callback:
                            error_msg = str(e)
                            if asyncio.iscoroutinefunction(subscription.on_error_callback):
                                await subscription.on_error_callback(error_msg)
                            else:
                                subscription.on_error_callback(error_msg)
                        raise

                    stream_error = KubeMQStreamBrokenError(
                        f"Stream broken: {e.message}",
                        operation=e.operation or "SubscribeToEvents",
                        channel=e.channel,
                        cause=e,
                    )
                    if subscription.on_error_callback:
                        error_msg = str(stream_error)
                        if asyncio.iscoroutinefunction(subscription.on_error_callback):
                            await subscription.on_error_callback(error_msg)
                        else:
                            subscription.on_error_callback(error_msg)

                    delay = backoff.delay_seconds(attempt)
                    _logger.debug(
                        "Stream reconnect attempt %d after %.1fs",
                        attempt + 1,
                        delay,
                    )
                    await asyncio.sleep(delay)
                    attempt += 1

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

    async def subscribe_to_events_store(
        self,
        subscription: EventsStoreSubscription,
        cancellation_token: AsyncCancellationToken | None = None,
    ) -> AsyncIterator[EventStoreMessageReceived]:
        """Subscribe to events store with automatic stream reconnection.

        Stream-level transient errors trigger re-subscribe with exponential
        backoff. Per-message handler errors are isolated and do not terminate
        the stream.

        Args:
            subscription: Subscription configuration
            cancellation_token: Optional token to cancel subscription

        Yields:
            EventStoreMessageReceived for each event

        Cancellation:
            Pass an ``AsyncCancellationToken`` to cancel the subscription.
            When cancelled, the async iterator terminates.
        """
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
                        resume_sub = subscription.model_copy(update={
                            "events_store_type": EventsStoreType.StartAtSequence,
                            "events_store_sequence_value": last_sequence + 1,
                        })
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
                                event = EventStoreMessageReceived.decode(pb_event)

                                if event.sequence > 0:
                                    last_sequence = event.sequence

                                if subscription.on_receive_event_callback:
                                    try:
                                        if asyncio.iscoroutinefunction(subscription.on_receive_event_callback):
                                            await subscription.on_receive_event_callback(event)
                                        else:
                                            subscription.on_receive_event_callback(event)
                                    except Exception as handler_err:
                                        handler_error = KubeMQHandlerError(
                                            f"Message handler raised {type(handler_err).__name__}: {handler_err}",
                                            cause=handler_err,
                                            operation="MessageHandler",
                                            channel=subscription.channel if hasattr(subscription, "channel") else None,
                                        )
                                        if subscription.on_error_callback:
                                            try:
                                                if asyncio.iscoroutinefunction(subscription.on_error_callback):
                                                    await subscription.on_error_callback(str(handler_error))
                                                else:
                                                    subscription.on_error_callback(str(handler_error))
                                            except Exception:
                                                _logger.exception("Error in on_error callback itself")
                                        else:
                                            _logger.error("Unhandled handler error: %s", handler_error)

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

                    break  # clean stream exit

                except KubeMQError as e:
                    if not e.is_retryable or token.is_cancelled:
                        if subscription.on_error_callback:
                            error_msg = str(e)
                            if asyncio.iscoroutinefunction(subscription.on_error_callback):
                                await subscription.on_error_callback(error_msg)
                            else:
                                subscription.on_error_callback(error_msg)
                        raise

                    stream_error = KubeMQStreamBrokenError(
                        f"Stream broken: {e.message}",
                        operation=e.operation or "SubscribeToEventsStore",
                        channel=e.channel,
                        cause=e,
                    )
                    if subscription.on_error_callback:
                        error_msg = str(stream_error)
                        if asyncio.iscoroutinefunction(subscription.on_error_callback):
                            await subscription.on_error_callback(error_msg)
                        else:
                            subscription.on_error_callback(error_msg)

                    delay = backoff.delay_seconds(attempt)
                    _logger.debug(
                        "Stream reconnect attempt %d after %.1fs",
                        attempt + 1,
                        delay,
                    )
                    await asyncio.sleep(delay)
                    attempt += 1

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

    async def subscribe_with_callback(
        self,
        subscription: EventsSubscription,
        callback: AsyncEventCallback,
        error_callback: AsyncErrorCallback | None = None,
        cancellation_token: AsyncCancellationToken | None = None,
        *,
        max_concurrent_callbacks: int = 1,
    ) -> None:
        """Subscribe to events with an async callback and stream reconnection.

        Messages are delivered to the callback function. By default,
        callbacks are invoked sequentially (one at a time). Set
        ``max_concurrent_callbacks`` to allow concurrent processing.

        Per-message handler errors are isolated and reported via error_callback
        without terminating the stream.

        Args:
            subscription: Subscription configuration
            callback: Async callback for each event
            error_callback: Optional async callback for errors
            cancellation_token: Optional token to cancel subscription
            max_concurrent_callbacks: Maximum number of callbacks that may
                execute concurrently. Default ``1`` (sequential). Must be
                >= 1 and <= 1000.

        Raises:
            ValueError: If ``max_concurrent_callbacks`` < 1 or > 1000.

        Cancellation:
            Pass an ``AsyncCancellationToken`` to cancel the subscription.
            When cancelled, in-flight callbacks are awaited before return.

        Long-Running Callbacks:
            If your callback performs CPU-intensive or blocking work,
            offload it to avoid blocking the event loop::

                async def my_callback(event):
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(None, heavy_work, event)
        """
        if max_concurrent_callbacks < 1:
            raise ValueError("max_concurrent_callbacks must be >= 1")
        if max_concurrent_callbacks > 1000:
            raise ValueError(
                "max_concurrent_callbacks must be <= 1000 "
                "(prevents accidental resource exhaustion)"
            )

        self._ensure_connected()
        assert self._transport is not None

        token = cancellation_token or AsyncCancellationToken()
        await self._register_subscription(token)

        current_task = asyncio.current_task()
        if current_task is not None:
            self._register_subscription_task(current_task)

        pending_tasks: set[asyncio.Task] = set()  # type: ignore[type-arg]
        backoff = BackoffCalculator(self._config.retry_policy)
        attempt = 0

        try:
            while not token.is_cancelled:
                try:
                    request = subscription.encode(self._config.client_id or "")

                    if max_concurrent_callbacks == 1:
                        # Fast path: sequential (no semaphore overhead)
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
                                    event = EventMessageReceived.decode(pb_event)
                                    try:
                                        await callback(event)
                                    except Exception as handler_err:
                                        handler_error = KubeMQHandlerError(
                                            f"Message handler raised {type(handler_err).__name__}: {handler_err}",
                                            cause=handler_err,
                                            operation="MessageHandler",
                                        )
                                        if error_callback:
                                            try:
                                                await error_callback(handler_error)
                                            except Exception:
                                                _logger.exception("Error in error_callback itself")
                                        else:
                                            _logger.error("Unhandled handler error: %s", handler_error)
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
                    else:
                        # Concurrent path: semaphore-limited task spawning
                        sem = asyncio.Semaphore(max_concurrent_callbacks)

                        async def _run_callback(evt: EventMessageReceived) -> None:
                            try:
                                await callback(evt)
                            except Exception as cb_err:
                                if error_callback:
                                    try:
                                        await error_callback(cb_err)
                                    except Exception:
                                        pass
                                elif self._logger:
                                    self._logger.error(
                                        "Unhandled callback exception: %s (%s)",
                                        cb_err,
                                        type(cb_err).__name__,
                                    )
                            finally:
                                sem.release()

                        async for pb_event in self._transport.subscribe_to_events(request, token):
                            attempt = 0
                            event = EventMessageReceived.decode(pb_event)
                            self._instrumentor._metrics.record_consumed_message(
                                "process", subscription.channel
                            )
                            await sem.acquire()
                            task = asyncio.create_task(_run_callback(event))
                            pending_tasks.add(task)
                            task.add_done_callback(pending_tasks.discard)

                    break  # clean stream exit

                except KubeMQError as e:
                    if not e.is_retryable or token.is_cancelled:
                        if error_callback:
                            await error_callback(e)
                        else:
                            raise
                        return

                    stream_error = KubeMQStreamBrokenError(
                        f"Stream broken: {e.message}",
                        operation=e.operation or "SubscribeToEvents",
                        channel=e.channel,
                        cause=e,
                    )
                    if error_callback:
                        await error_callback(stream_error)

                    delay = backoff.delay_seconds(attempt)
                    _logger.debug(
                        "Stream reconnect attempt %d after %.1fs",
                        attempt + 1,
                        delay,
                    )
                    await asyncio.sleep(delay)
                    attempt += 1

                except Exception as e:
                    if error_callback:
                        await error_callback(e)
                    else:
                        raise
                    return
        finally:
            if pending_tasks:
                await asyncio.gather(*pending_tasks, return_exceptions=True)
            await self._unregister_subscription(token)

    async def subscribe_store_with_callback(
        self,
        subscription: EventsStoreSubscription,
        callback: AsyncEventStoreCallback,
        error_callback: AsyncErrorCallback | None = None,
        cancellation_token: AsyncCancellationToken | None = None,
        *,
        max_concurrent_callbacks: int = 1,
    ) -> None:
        """Subscribe to events store with an async callback and stream reconnection.

        Messages are delivered to the callback function. By default,
        callbacks are invoked sequentially (one at a time). Set
        ``max_concurrent_callbacks`` to allow concurrent processing.

        Per-message handler errors are isolated and reported via error_callback
        without terminating the stream.

        Args:
            subscription: Subscription configuration
            callback: Async callback for each event
            error_callback: Optional async callback for errors
            cancellation_token: Optional token to cancel subscription
            max_concurrent_callbacks: Maximum number of callbacks that may
                execute concurrently. Default ``1`` (sequential). Must be
                >= 1 and <= 1000.

        Raises:
            ValueError: If ``max_concurrent_callbacks`` < 1 or > 1000.

        Cancellation:
            Pass an ``AsyncCancellationToken`` to cancel the subscription.
            When cancelled, in-flight callbacks are awaited before return.
        """
        if max_concurrent_callbacks < 1:
            raise ValueError("max_concurrent_callbacks must be >= 1")
        if max_concurrent_callbacks > 1000:
            raise ValueError(
                "max_concurrent_callbacks must be <= 1000 "
                "(prevents accidental resource exhaustion)"
            )

        self._ensure_connected()
        assert self._transport is not None

        token = cancellation_token or AsyncCancellationToken()
        await self._register_subscription(token)

        current_task = asyncio.current_task()
        if current_task is not None:
            self._register_subscription_task(current_task)

        pending_tasks: set[asyncio.Task] = set()  # type: ignore[type-arg]
        backoff = BackoffCalculator(self._config.retry_policy)
        attempt = 0
        last_sequence = 0

        try:
            while not token.is_cancelled:
                try:
                    if last_sequence > 0:
                        resume_sub = subscription.model_copy(update={
                            "events_store_type": EventsStoreType.StartAtSequence,
                            "events_store_sequence_value": last_sequence + 1,
                        })
                        request = resume_sub.encode(self._config.client_id or "")
                    else:
                        request = subscription.encode(self._config.client_id or "")

                    if max_concurrent_callbacks == 1:
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
                                    event = EventStoreMessageReceived.decode(pb_event)
                                    if event.sequence > 0:
                                        last_sequence = event.sequence
                                    try:
                                        await callback(event)
                                    except Exception as handler_err:
                                        handler_error = KubeMQHandlerError(
                                            f"Message handler raised {type(handler_err).__name__}: {handler_err}",
                                            cause=handler_err,
                                            operation="MessageHandler",
                                        )
                                        if error_callback:
                                            try:
                                                await error_callback(handler_error)
                                            except Exception:
                                                _logger.exception("Error in error_callback itself")
                                        else:
                                            _logger.error("Unhandled handler error: %s", handler_error)
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
                    else:
                        sem = asyncio.Semaphore(max_concurrent_callbacks)

                        async def _run_store_callback(evt: EventStoreMessageReceived) -> None:
                            try:
                                await callback(evt)
                            except Exception as cb_err:
                                if error_callback:
                                    try:
                                        await error_callback(cb_err)
                                    except Exception:
                                        pass
                                elif self._logger:
                                    self._logger.error(
                                        "Unhandled callback exception: %s (%s)",
                                        cb_err,
                                        type(cb_err).__name__,
                                    )
                            finally:
                                sem.release()

                        async for pb_event in self._transport.subscribe_to_events(request, token):
                            attempt = 0
                            event = EventStoreMessageReceived.decode(pb_event)
                            if event.sequence > 0:
                                last_sequence = event.sequence
                            self._instrumentor._metrics.record_consumed_message(
                                "process", subscription.channel
                            )
                            await sem.acquire()
                            task = asyncio.create_task(_run_store_callback(event))
                            pending_tasks.add(task)
                            task.add_done_callback(pending_tasks.discard)

                    break  # clean stream exit

                except KubeMQError as e:
                    if not e.is_retryable or token.is_cancelled:
                        if error_callback:
                            await error_callback(e)
                        else:
                            raise
                        return

                    stream_error = KubeMQStreamBrokenError(
                        f"Stream broken: {e.message}",
                        operation=e.operation or "SubscribeToEventsStore",
                        channel=e.channel,
                        cause=e,
                    )
                    if error_callback:
                        await error_callback(stream_error)

                    delay = backoff.delay_seconds(attempt)
                    _logger.debug(
                        "Stream reconnect attempt %d after %.1fs",
                        attempt + 1,
                        delay,
                    )
                    await asyncio.sleep(delay)
                    attempt += 1

                except Exception as e:
                    if error_callback:
                        await error_callback(e)
                    else:
                        raise
                    return
        finally:
            if pending_tasks:
                await asyncio.gather(*pending_tasks, return_exceptions=True)
            await self._unregister_subscription(token)


# Alias for backward compatibility and clearer naming
AsyncPubSubClient = AsyncClient
