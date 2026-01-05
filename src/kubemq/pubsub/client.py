"""PubSub client for KubeMQ."""

from __future__ import annotations

import asyncio
import threading
import time
from pathlib import Path

import grpc

from kubemq.common.cancellation_token import CancellationToken
from kubemq.common.channel_stats import PubSubChannel
from kubemq.common.helpers import decode_grpc_error
from kubemq.common.requests import (
    create_channel_request,
    delete_channel_request,
    list_pubsub_channels,
)
from kubemq.core import BaseClient, ClientConfig
from kubemq.core.compat import deprecated_async_method, run_in_thread
from kubemq.core.config import KeepAliveConfig, TLSConfig
from kubemq.pubsub.event_message import EventMessage
from kubemq.pubsub.event_message_received import EventMessageReceived
from kubemq.pubsub.event_send_result import EventSendResult
from kubemq.pubsub.event_sender import EventSender
from kubemq.pubsub.event_store_message import EventStoreMessage
from kubemq.pubsub.event_store_message_received import EventStoreMessageReceived
from kubemq.pubsub.events_store_subscription import EventsStoreSubscription
from kubemq.pubsub.events_subscription import EventsSubscription
from kubemq.transport.server_info import ServerInfo


class Client(BaseClient):
    """PubSub client for publishing and subscribing to events.

    Note:
        The `*_async()` methods on this class use thread-based async wrappers.
        For native async support with better performance, use `AsyncClient`
        (or `AsyncPubSubClient`) from `kubemq.pubsub.async_client` instead::

            from kubemq.pubsub import AsyncClient

            async with AsyncClient(address="localhost:50000") as client:
                await client.send_event(EventMessage(
                    channel="my-channel",
                    body=b"Hello, World!"
                ))

    Example:
        # Using context manager (recommended)
        with Client(address="localhost:50000") as client:
            client.send_events_message(EventMessage(
                channel="my-channel",
                body=b"Hello, World!"
            ))

        # Using configuration object
        config = ClientConfig(address="localhost:50000", client_id="my-app")
        client = Client(config=config)
        try:
            client.send_events_message(...)
        finally:
            client.close()
    """

    def __init__(
        self,
        address: str = "",
        client_id: str | None = None,
        auth_token: str | None = None,
        config: ClientConfig | None = None,
        # Legacy parameters for backward compatibility
        tls: bool = False,
        tls_cert_file: str = "",
        tls_key_file: str = "",
        tls_ca_file: str = "",
        max_send_size: int = 0,
        max_receive_size: int = 0,
        disable_auto_reconnect: bool = False,
        reconnect_interval_seconds: int = 0,
        keep_alive: bool = False,
        ping_interval_in_seconds: int = 0,
        ping_timeout_in_seconds: int = 0,
        log_level: int | None = None,
    ) -> None:
        """Initialize the PubSub client.

        Args:
            address: KubeMQ server address (host:port)
            client_id: Client identifier (defaults to hostname)
            auth_token: Authentication token
            config: Pre-built ClientConfig object (overrides other params)
            tls: Whether to use TLS (legacy)
            tls_cert_file: Path to TLS certificate (legacy)
            tls_key_file: Path to TLS key (legacy)
            tls_ca_file: Path to TLS CA certificate (legacy)
            max_send_size: Maximum send message size (legacy)
            max_receive_size: Maximum receive message size (legacy)
            disable_auto_reconnect: Disable auto-reconnect (legacy)
            reconnect_interval_seconds: Reconnect interval (legacy)
            keep_alive: Enable keep-alive (legacy)
            ping_interval_in_seconds: Keep-alive ping interval (legacy)
            ping_timeout_in_seconds: Keep-alive ping timeout (legacy)
            log_level: Logging level (legacy)
        """
        # Build config from legacy parameters if not provided
        if config is None:
            tls_config = (
                TLSConfig(
                    enabled=tls,
                    cert_file=Path(tls_cert_file) if tls_cert_file else None,
                    key_file=Path(tls_key_file) if tls_key_file else None,
                    ca_file=Path(tls_ca_file) if tls_ca_file else None,
                )
                if tls
                else TLSConfig()
            )

            keep_alive_config = (
                KeepAliveConfig(
                    enabled=keep_alive,
                    ping_interval_in_seconds=ping_interval_in_seconds or 30,
                    ping_timeout_in_seconds=ping_timeout_in_seconds or 10,
                )
                if keep_alive
                else KeepAliveConfig()
            )

            config = ClientConfig(
                address=address,
                client_id=client_id,
                auth_token=auth_token,
                tls=tls_config,
                keep_alive=keep_alive_config,
                max_send_size=max_send_size or ClientConfig.DEFAULT_MAX_MESSAGE_SIZE,
                max_receive_size=max_receive_size or ClientConfig.DEFAULT_MAX_MESSAGE_SIZE,
                auto_reconnect=not disable_auto_reconnect,
                reconnect_interval_seconds=reconnect_interval_seconds or 1,
                log_level=log_level,
            )

        super().__init__(config=config)

        # PubSub-specific initialization
        self._event_sender: EventSender | None = None
        self._sender_lock = threading.Lock()

        # Legacy attribute for backward compatibility
        self.connection = self._config.to_legacy_connection()

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close_async()

    async def close_async(self) -> None:
        """Asynchronous version of close().

        Closes the connection to the KubeMQ server and releases resources asynchronously.
        """
        self._shutdown_event.set()
        self._logger.debug(f"Client disconnecting from {self._config.address}")
        if self._transport is not None:
            await self._transport.close_async()

    @deprecated_async_method("AsyncPubSubClient.ping")
    async def ping_async(self) -> ServerInfo:
        """Asynchronous version of ping().

        Deprecated:
            Use `AsyncPubSubClient.ping()` for native async support.

        Returns:
            ServerInfo: The server information.

        Raises:
            KubeMQConnectionError: If an error occurs during the ping.
        """
        return await run_in_thread(self.ping)

    def _cleanup_resources(self) -> None:
        """Clean up PubSub-specific resources."""
        self._event_sender = None

    def _get_event_sender(self) -> EventSender:
        """Thread-safe lazy initialization of event sender."""
        self._ensure_connected()
        assert self._transport is not None
        with self._sender_lock:
            if self._event_sender is None:
                self._event_sender = EventSender(
                    self._transport,
                    self._shutdown_event,
                    self._logger,
                    self.connection,
                )
            return self._event_sender

    # Event methods
    def send_events_message(self, message: EventMessage) -> None:
        """Send an event message.

        Args:
            message: The event message to send
        """
        sender = self._get_event_sender()
        sender.send(message.encode(self._config.client_id or ""))

    @deprecated_async_method("AsyncPubSubClient.send_event")
    async def send_events_message_async(self, message: EventMessage) -> None:
        """Send an event message asynchronously.

        Deprecated:
            Use `AsyncPubSubClient.send_event()` for native async support.

        Args:
            message: The event message to send
        """
        await run_in_thread(self.send_events_message, message)

    def send_events_store_message(self, message: EventStoreMessage) -> EventSendResult:
        """Send an event store message.

        Args:
            message: The event store message to send

        Returns:
            EventSendResult with the send operation result
        """
        sender = self._get_event_sender()
        result = sender.send(message.encode(self._config.client_id or ""))
        if result is not None:
            return EventSendResult().decode(result)
        # Return empty result if no response (shouldn't happen for store messages)
        return EventSendResult()

    @deprecated_async_method("AsyncPubSubClient.send_event_store")
    async def send_events_store_message_async(self, message: EventStoreMessage) -> EventSendResult:
        """Send an event store message asynchronously.

        Deprecated:
            Use `AsyncPubSubClient.send_event_store()` for native async support.

        Args:
            message: The event store message to send

        Returns:
            EventSendResult with the send operation result
        """
        return await run_in_thread(self.send_events_store_message, message)

    # Channel management methods
    def create_events_channel(self, channel: str) -> bool | None:
        """Create an events channel.

        Args:
            channel: The name of the channel to create

        Returns:
            True if successful, None if there was an error
        """
        return create_channel_request(self._transport, self._config.client_id, channel, "events")

    async def create_events_channel_async(self, channel: str) -> bool | None:
        """Create an events channel asynchronously.

        Args:
            channel: The name of the channel to create

        Returns:
            True if successful, None if there was an error
        """
        return await run_in_thread(self.create_events_channel, channel)

    def create_events_store_channel(self, channel: str) -> bool | None:
        """Create an events store channel.

        Args:
            channel: The name of the channel to create

        Returns:
            True if successful, None if there was an error
        """
        return create_channel_request(
            self._transport, self._config.client_id, channel, "events_store"
        )

    async def create_events_store_channel_async(self, channel: str) -> bool | None:
        """Create an events store channel asynchronously.

        Args:
            channel: The name of the channel to create

        Returns:
            True if successful, None if there was an error
        """
        return await run_in_thread(self.create_events_store_channel, channel)

    def delete_events_channel(self, channel: str) -> bool | None:
        """Delete an events channel.

        Args:
            channel: The name of the channel to delete

        Returns:
            True if successful, None if there was an error
        """
        return delete_channel_request(self._transport, self._config.client_id, channel, "events")

    async def delete_events_channel_async(self, channel: str) -> bool | None:
        """Delete an events channel asynchronously.

        Args:
            channel: The name of the channel to delete

        Returns:
            True if successful, None if there was an error
        """
        return await run_in_thread(self.delete_events_channel, channel)

    def delete_events_store_channel(self, channel: str) -> bool | None:
        """Delete an events store channel.

        Args:
            channel: The name of the channel to delete

        Returns:
            True if successful, None if there was an error
        """
        return delete_channel_request(
            self._transport, self._config.client_id, channel, "events_store"
        )

    async def delete_events_store_channel_async(self, channel: str) -> bool | None:
        """Delete an events store channel asynchronously.

        Args:
            channel: The name of the channel to delete

        Returns:
            True if successful, None if there was an error
        """
        return await run_in_thread(self.delete_events_store_channel, channel)

    def list_events_channels(self, channel_search: str = "") -> list[PubSubChannel]:
        """List events channels.

        Args:
            channel_search: Optional filter string

        Returns:
            List of PubSubChannel objects
        """
        return list_pubsub_channels(
            self._transport, self._config.client_id, "events", channel_search
        )

    async def list_events_channels_async(self, channel_search: str = "") -> list[PubSubChannel]:
        """List events channels asynchronously.

        Args:
            channel_search: Optional filter string

        Returns:
            List of PubSubChannel objects
        """
        return await run_in_thread(self.list_events_channels, channel_search)

    def list_events_store_channels(self, channel_search: str = "") -> list[PubSubChannel]:
        """List events store channels.

        Args:
            channel_search: Optional filter string

        Returns:
            List of PubSubChannel objects
        """
        return list_pubsub_channels(
            self._transport, self._config.client_id, "events_store", channel_search
        )

    async def list_events_store_channels_async(
        self, channel_search: str = ""
    ) -> list[PubSubChannel]:
        """List events store channels asynchronously.

        Args:
            channel_search: Optional filter string

        Returns:
            List of PubSubChannel objects
        """
        return await run_in_thread(self.list_events_store_channels, channel_search)

    # Subscription methods
    def subscribe_to_events(
        self, subscription: EventsSubscription, cancel: CancellationToken | None = None
    ) -> None:
        """Subscribe to events.

        Args:
            subscription: The subscription configuration
            cancel: Optional cancellation token
        """
        self._subscribe(subscription, cancel)

    def subscribe_to_events_store(
        self, subscription: EventsStoreSubscription, cancel: CancellationToken | None = None
    ) -> None:
        """Subscribe to events store.

        Args:
            subscription: The subscription configuration
            cancel: Optional cancellation token
        """
        self._subscribe(subscription, cancel)

    def _subscribe(
        self,
        subscription: EventsSubscription | EventsStoreSubscription,
        cancel: CancellationToken | None,
    ) -> None:
        """Internal subscription handler."""
        self._ensure_connected()
        assert self._transport is not None
        if cancel is None:
            cancel = CancellationToken()
        cancel_token_event = cancel.event
        transport = self._transport  # Capture for lambda
        client_id = self._config.client_id or ""
        args: tuple = ()
        if isinstance(subscription, EventsStoreSubscription):
            args = (
                lambda: transport.kubemq_client().SubscribeToEvents(
                    subscription.encode(client_id)
                ),
                lambda message: subscription.raise_on_receive_message(
                    EventStoreMessageReceived().decode(message)
                ),
                lambda error: subscription.raise_on_error(error),
                cancel_token_event,
            )
        if isinstance(subscription, EventsSubscription):
            args = (
                lambda: transport.kubemq_client().SubscribeToEvents(
                    subscription.encode(client_id)
                ),
                lambda message: subscription.raise_on_receive_message(
                    EventMessageReceived().decode(message)
                ),
                lambda error: subscription.raise_on_error(error),
                cancel_token_event,
            )
        threading.Thread(target=self._subscribe_task, args=args, daemon=True).start()

    def _subscribe_task(
        self,
        stream_callable,
        decode_callable,
        error_callable,
        cancel_token: threading.Event,
    ) -> None:
        """Background subscription task."""
        while not cancel_token.is_set() and not self._shutdown_event.is_set():
            try:
                response = stream_callable()
                for message in response:
                    if cancel_token.is_set():
                        break
                    decode_callable(message)
            except grpc.RpcError as e:
                error_callable(decode_grpc_error(e))
                time.sleep(self._config.reconnect_interval_seconds)
                continue
            except Exception as e:
                error_callable(decode_grpc_error(e))
                time.sleep(self._config.reconnect_interval_seconds)
                continue

    # Async subscription methods
    def subscribe_to_events_async(
        self, subscription: EventsSubscription, cancel: CancellationToken | None = None
    ):
        """Asynchronous version of subscribe_to_events().

        Supports both sync and async callbacks.

        Args:
            subscription: The subscription configuration
            cancel: Optional cancellation token

        Returns:
            asyncio.Task object that can be awaited
        """
        return self._subscribe_async(subscription, cancel)

    def subscribe_to_events_store_async(
        self, subscription: EventsStoreSubscription, cancel: CancellationToken | None = None
    ):
        """Asynchronous version of subscribe_to_events_store().

        Supports both sync and async callbacks.

        Args:
            subscription: The subscription configuration
            cancel: Optional cancellation token

        Returns:
            asyncio.Task object that can be awaited
        """
        return self._subscribe_async(subscription, cancel)

    def _subscribe_async(
        self,
        subscription: EventsSubscription | EventsStoreSubscription,
        cancel: CancellationToken | None,
    ):
        """Internal async subscription handler."""
        self._ensure_connected()
        assert self._transport is not None
        if cancel is None:
            cancel = CancellationToken()
        cancel_token_event = cancel.event
        transport = self._transport  # Capture for lambda
        client_id = self._config.client_id or ""
        args: tuple = ()
        if isinstance(subscription, EventsStoreSubscription):
            args = (
                lambda: transport.kubemq_client().SubscribeToEvents(
                    subscription.encode(client_id)
                ),
                lambda message: subscription.raise_on_receive_message_async(
                    EventStoreMessageReceived().decode(message)
                ),
                lambda error: subscription.raise_on_error_async(error),
                cancel_token_event,
            )
        if isinstance(subscription, EventsSubscription):
            args = (
                lambda: transport.kubemq_client().SubscribeToEvents(
                    subscription.encode(client_id)
                ),
                lambda message: subscription.raise_on_receive_message_async(
                    EventMessageReceived().decode(message)
                ),
                lambda error: subscription.raise_on_error_async(error),
                cancel_token_event,
            )
        return asyncio.create_task(self._subscribe_task_async(*args))  # type: ignore[arg-type]

    async def _subscribe_task_async(
        self,
        stream_callable,
        decode_callable,
        error_callable,
        cancel_token: threading.Event,
    ):
        """Async version of subscription task that supports async callbacks."""
        while not cancel_token.is_set() and not self._shutdown_event.is_set():
            try:
                # Run the gRPC stream creation and iteration in a thread to avoid blocking
                response = await asyncio.to_thread(stream_callable)

                # Process messages from the stream
                while not cancel_token.is_set() and not self._shutdown_event.is_set():
                    try:
                        # Read next message from stream in a thread to avoid blocking
                        message = await asyncio.to_thread(next, response)
                        # Await the decode callable which handles both sync and async callbacks
                        await decode_callable(message)
                    except StopIteration:
                        # Stream ended, break inner loop to reconnect
                        break
            except grpc.RpcError as e:
                await error_callable(decode_grpc_error(e))
                await asyncio.sleep(self._config.reconnect_interval_seconds)
                continue
            except Exception as e:
                await error_callable(decode_grpc_error(e))
                await asyncio.sleep(self._config.reconnect_interval_seconds)
                continue
