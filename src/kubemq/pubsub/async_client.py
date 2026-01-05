"""Native async PubSub client for KubeMQ."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable
from typing import (
    TYPE_CHECKING,
    Callable,
)

from kubemq.common.async_cancellation_token import AsyncCancellationToken
from kubemq.core.client import NativeAsyncBaseClient
from kubemq.core.config import ClientConfig
from kubemq.pubsub.event_message import EventMessage
from kubemq.pubsub.event_message_received import EventMessageReceived
from kubemq.pubsub.event_send_result import EventSendResult
from kubemq.pubsub.event_store_message import EventStoreMessage
from kubemq.pubsub.event_store_message_received import EventStoreMessageReceived
from kubemq.pubsub.events_store_subscription import EventsStoreSubscription
from kubemq.pubsub.events_subscription import EventsSubscription

if TYPE_CHECKING:
    pass

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

    # =========================================================================
    # Send Operations
    # =========================================================================

    async def send_event(self, message: EventMessage) -> None:
        """Send a fire-and-forget event.

        Args:
            message: The event message to send

        Raises:
            KubeMQConnectionError: If not connected
            KubeMQTimeoutError: If send times out
            KubeMQValidationError: If message is invalid
        """
        self._ensure_connected()
        assert self._transport is not None
        pb_event = message.encode(self._config.client_id or "")
        await self._transport.send_event(pb_event)

    async def send_event_store(self, message: EventStoreMessage) -> EventSendResult:
        """Send an event to events store (persistent).

        Args:
            message: The event store message to send

        Returns:
            EventSendResult with persistence confirmation
        """
        self._ensure_connected()
        assert self._transport is not None
        pb_event = message.encode(self._config.client_id or "")
        result = await self._transport.send_event(pb_event)
        return EventSendResult.decode(result)

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
                    await self.send_event(msg)
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
                    result = await self.send_event_store(msg)
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
        """Subscribe to events.

        Can be used as async iterator.

        Args:
            subscription: Subscription configuration
            cancellation_token: Optional token to cancel subscription

        Yields:
            EventMessageReceived for each event

        Example:
            token = AsyncCancellationToken()
            async for event in client.subscribe_to_events(sub, token):
                process(event)
                if should_stop:
                    token.cancel()
        """
        self._ensure_connected()
        assert self._transport is not None

        # Create internal cancellation token if not provided
        token = cancellation_token or AsyncCancellationToken()
        await self._register_subscription(token)

        try:
            request = subscription.encode(self._config.client_id or "")

            async for pb_event in self._transport.subscribe_to_events(request, token):
                event = EventMessageReceived.decode(pb_event)

                # Call the subscription callback if provided
                if subscription.on_receive_event_callback:
                    if asyncio.iscoroutinefunction(subscription.on_receive_event_callback):
                        await subscription.on_receive_event_callback(event)
                    else:
                        subscription.on_receive_event_callback(event)

                yield event

        except Exception as e:
            # Call error callback if provided
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
        """Subscribe to events store (persistent events).

        Args:
            subscription: Subscription configuration
            cancellation_token: Optional token to cancel subscription

        Yields:
            EventStoreMessageReceived for each event
        """
        self._ensure_connected()
        assert self._transport is not None

        token = cancellation_token or AsyncCancellationToken()
        await self._register_subscription(token)

        try:
            request = subscription.encode(self._config.client_id or "")

            async for pb_event in self._transport.subscribe_to_events(request, token):
                event = EventStoreMessageReceived.decode(pb_event)

                # Call the subscription callback if provided
                if subscription.on_receive_event_callback:
                    if asyncio.iscoroutinefunction(subscription.on_receive_event_callback):
                        await subscription.on_receive_event_callback(event)
                    else:
                        subscription.on_receive_event_callback(event)

                yield event

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
    ) -> None:
        """Subscribe to events with an async callback.

        This method runs until cancelled and doesn't yield events.

        Args:
            subscription: Subscription configuration
            callback: Async callback for each event
            error_callback: Optional async callback for errors
            cancellation_token: Optional token to cancel subscription
        """
        self._ensure_connected()
        assert self._transport is not None

        token = cancellation_token or AsyncCancellationToken()
        await self._register_subscription(token)

        try:
            request = subscription.encode(self._config.client_id or "")

            async for pb_event in self._transport.subscribe_to_events(request, token):
                event = EventMessageReceived.decode(pb_event)
                await callback(event)

        except Exception as e:
            if error_callback:
                await error_callback(e)
            else:
                raise
        finally:
            await self._unregister_subscription(token)

    async def subscribe_store_with_callback(
        self,
        subscription: EventsStoreSubscription,
        callback: AsyncEventStoreCallback,
        error_callback: AsyncErrorCallback | None = None,
        cancellation_token: AsyncCancellationToken | None = None,
    ) -> None:
        """Subscribe to events store with an async callback.

        This method runs until cancelled and doesn't yield events.

        Args:
            subscription: Subscription configuration
            callback: Async callback for each event
            error_callback: Optional async callback for errors
            cancellation_token: Optional token to cancel subscription
        """
        self._ensure_connected()
        assert self._transport is not None

        token = cancellation_token or AsyncCancellationToken()
        await self._register_subscription(token)

        try:
            request = subscription.encode(self._config.client_id or "")

            async for pb_event in self._transport.subscribe_to_events(request, token):
                event = EventStoreMessageReceived.decode(pb_event)
                await callback(event)

        except Exception as e:
            if error_callback:
                await error_callback(e)
            else:
                raise
        finally:
            await self._unregister_subscription(token)


# Alias for backward compatibility and clearer naming
AsyncPubSubClient = AsyncClient
