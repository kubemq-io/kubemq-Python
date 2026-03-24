"""PubSub client for KubeMQ."""

from __future__ import annotations

import asyncio
import threading
import time
from pathlib import Path
from typing import Any

import grpc
import dataclasses

from kubemq._internal.deprecation import deprecated, deprecated_async
from kubemq._internal.retry import BackoffCalculator
from kubemq._internal.telemetry import (
    KubeMQTagsCarrier,
    create_link_from_context,
    error_code_to_error_type,
)
from kubemq.common.cancellation_token import CancellationToken
from kubemq.common.channel_stats import PubSubChannel
from kubemq.common.helpers import decode_grpc_error
from kubemq.common.requests import (
    create_channel_request,
    delete_channel_request,
    list_pubsub_channels,
)
from kubemq.core import BaseClient, ClientConfig
from kubemq.core.compat import run_in_thread
from kubemq.core.config import KeepAliveConfig, TLSConfig
from kubemq.core.exceptions import (
    ErrorCode,
    KubeMQHandlerError,
    KubeMQStreamBrokenError,
    KubeMQValidationError,
    from_grpc_error as convert_grpc_error,
)
from kubemq.pubsub.event_message import EventMessage
from kubemq.pubsub.event_message_received import EventReceived
from kubemq.pubsub.event_send_result import EventStoreResult
from kubemq.pubsub.event_sender import EventSender
from kubemq.pubsub.event_store_message import EventStoreMessage
from kubemq.pubsub.event_store_message_received import EventStoreReceived
from kubemq.pubsub.events_store_subscription import EventsStoreSubscription, EventStoreStartPosition
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

    Thread Safety:
        This class is thread-safe. Share across threads. One instance
        per application recommended.
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

    async def __aenter__(self) -> Client:
        """Async context manager entry."""
        return self

    async def __aexit__(
        self, exc_type: type | None, exc_val: BaseException | None, exc_tb: object | None
    ) -> None:
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

    @deprecated_async(replacement="AsyncPubSubClient.ping()", since="4.0.0", removal="5.0.0")
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
                    max_queue_size=self._config.max_send_queue_size,
                )
            return self._event_sender

    # Event methods — GS-aligned verbs

    def send_event(self, message: EventMessage) -> None:
        """Send an event message via unary SendEvent RPC.

        Uses the unary ``SendEvent`` RPC for single-shot delivery.

        Args:
            message: The event message to send.

        Raises:
            KubeMQValidationError: If the message fails validation (e.g.,
                empty channel, body exceeds ``max_send_size``).
            KubeMQConnectionError: If the server is unreachable or the
                connection is lost.
            KubeMQAuthenticationError: If the auth token is invalid or
                expired, or the client lacks permission for the channel.
            KubeMQTimeoutError: If the operation exceeds the server deadline.
            KubeMQClientClosedError: If the client has already been closed.
            KubeMQError: If the server returns ``Sent=false`` with an error
                message.

        See Also:
            :class:`~kubemq.pubsub.event_message.EventMessage`:
                Message type used for events.
            :meth:`subscribe_to_events`: Subscribe to receive events on a
                channel.
            :meth:`publish_event`: Fire-and-forget variant using the
                streaming sender.

        Example:
            >>> from kubemq.pubsub import Client
            >>> from kubemq.pubsub.event_message import EventMessage
            >>> with Client(address="localhost:50000") as client:
            ...     client.send_event(EventMessage(
            ...         channel="events.orders",
            ...         body=b'{"order_id": 123}',
            ...         metadata="new-order",
            ...     ))
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
                result = self._transport.kubemq_client().SendEvent(pb_event)
                self._instrumentor._metrics.record_sent_message("publish", message.channel)
                if result and not result.Sent and result.Error:
                    from kubemq.core.exceptions import KubeMQError

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

    def _publish_event_impl(self, message: EventMessage) -> None:
        """Internal implementation for event publishing."""
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
                sender.send(pb_event)
                self._instrumentor._metrics.record_sent_message("publish", message.channel)
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

    def publish_event(self, message: EventMessage) -> None:
        """Publish an event message (fire-and-forget).

        Uses the bidirectional streaming sender for higher throughput.
        Unlike :meth:`send_event`, this method does not return a result
        from the server.

        Args:
            message: The event message to publish.

        Raises:
            KubeMQValidationError: If the message fails validation (e.g.,
                empty channel, body exceeds ``max_send_size``).
            KubeMQConnectionError: If the server is unreachable or the
                connection is lost.
            KubeMQAuthenticationError: If the auth token is invalid or
                expired, or the client lacks permission for the channel.
            KubeMQClientClosedError: If the client has already been closed.

        See Also:
            :class:`~kubemq.pubsub.event_message.EventMessage`:
                Message type used for events.
            :meth:`subscribe_to_events`: Subscribe to receive events on a
                channel.
            :meth:`send_event`: Unary RPC variant with server confirmation.
        """
        self._publish_event_impl(message)

    @deprecated(replacement="publish_event()", since="4.0.0", removal="5.0.0")
    def send_events_message(self, message: EventMessage) -> None:
        """Send an event message.

        Deprecated:
            Use ``publish_event()`` instead. Will be removed in v5.0.

        Args:
            message: The event message to send

        Raises:
            KubeMQValidationError: If the message fails validation.
            KubeMQConnectionError: If the server is unreachable.
            KubeMQAuthenticationError: If authentication or authorization
                fails.
            KubeMQClientClosedError: If the client has already been closed.
        """
        self._publish_event_impl(message)

    @deprecated_async(
        replacement="AsyncPubSubClient.publish_event()", since="4.0.0", removal="5.0.0"
    )
    async def send_events_message_async(self, message: EventMessage) -> None:
        """Send an event message asynchronously.

        Deprecated:
            Use ``AsyncPubSubClient.publish_event()`` for native async support.

        Args:
            message: The event message to send

        Raises:
            KubeMQValidationError: If the message fails validation.
            KubeMQConnectionError: If the server is unreachable.
            KubeMQAuthenticationError: If authentication or authorization
                fails.
            KubeMQClientClosedError: If the client has already been closed.
        """
        await run_in_thread(self._publish_event_impl, message)

    def _send_event_store_impl(self, message: EventStoreMessage) -> EventStoreResult:
        """Internal implementation for event store publishing."""
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

    def send_event_store(self, message: EventStoreMessage) -> EventStoreResult:
        """Publish a persistent event store message.

        Args:
            message: The event store message to publish.

        Returns:
            EventStoreResult: Contains ``sent`` (bool indicating delivery
            success), ``id`` (the server-assigned message ID), and
            ``error`` (error description if the send failed).

        Raises:
            KubeMQValidationError: If the message fails validation (e.g.,
                empty channel, body exceeds ``max_send_size``).
            KubeMQConnectionError: If the server is unreachable or the
                connection is lost.
            KubeMQAuthenticationError: If the auth token is invalid or
                expired, or the client lacks permission for the channel.
            KubeMQClientClosedError: If the client has already been closed.

        See Also:
            :class:`~kubemq.pubsub.event_store_message.EventStoreMessage`:
                Message type for persistent events.
            :meth:`subscribe_to_events_store`: Subscribe to receive stored
                events with replay capability.

        Example:
            >>> from kubemq.pubsub import Client
            >>> from kubemq.pubsub.event_store_message import EventStoreMessage
            >>> with Client(address="localhost:50000") as client:
            ...     result = client.send_event_store(EventStoreMessage(
            ...         channel="events_store.audit",
            ...         body=b'{"action": "user.login", "user": "admin"}',
            ...         metadata="audit-trail",
            ...     ))
            ...     print(f"Sent: {result.sent}")
        """
        return self._send_event_store_impl(message)

    @deprecated(replacement="send_event_store()", since="4.0.0", removal="5.0.0")
    def send_events_store_message(self, message: EventStoreMessage) -> EventStoreResult:
        """Send an event store message.

        Deprecated:
            Use ``send_event_store()`` instead. Will be removed in v5.0.

        Args:
            message: The event store message to send

        Returns:
            EventStoreResult: Contains ``sent``, ``id``, and ``error`` fields.

        Raises:
            KubeMQValidationError: If the message fails validation.
            KubeMQConnectionError: If the server is unreachable.
            KubeMQAuthenticationError: If authentication or authorization
                fails.
            KubeMQClientClosedError: If the client has already been closed.
        """
        return self._send_event_store_impl(message)

    @deprecated_async(
        replacement="AsyncPubSubClient.send_event_store()", since="4.0.0", removal="5.0.0"
    )
    async def send_events_store_message_async(self, message: EventStoreMessage) -> EventStoreResult:
        """Send an event store message asynchronously.

        Deprecated:
            Use ``AsyncPubSubClient.send_event_store()`` for native async support.

        Args:
            message: The event store message to send

        Returns:
            EventStoreResult: Contains ``sent``, ``id``, and ``error`` fields.

        Raises:
            KubeMQValidationError: If the message fails validation.
            KubeMQConnectionError: If the server is unreachable.
            KubeMQAuthenticationError: If authentication or authorization
                fails.
            KubeMQClientClosedError: If the client has already been closed.
        """
        return await run_in_thread(self._send_event_store_impl, message)

    # Channel management methods
    def create_events_channel(self, channel: str) -> bool | None:
        """Create an events channel.

        Args:
            channel: The name of the channel to create.

        Returns:
            bool: ``True`` if the channel was created successfully.

        Raises:
            KubeMQConnectionError: If the server is unreachable.
            KubeMQAuthenticationError: If the auth token is invalid or the
                client lacks permission to create channels.
            KubeMQTimeoutError: If the operation exceeds the server deadline.
            KubeMQError: If the server rejects the request (e.g., channel
                already exists or invalid name).
        """
        return create_channel_request(self._transport, self._config.client_id, channel, "events")

    async def create_events_channel_async(self, channel: str) -> bool | None:
        """Create an events channel asynchronously.

        Args:
            channel: The name of the channel to create.

        Returns:
            bool: ``True`` if the channel was created successfully.

        Raises:
            KubeMQConnectionError: If the server is unreachable.
            KubeMQAuthenticationError: If the auth token is invalid or the
                client lacks permission to create channels.
            KubeMQTimeoutError: If the operation exceeds the server deadline.
            KubeMQError: If the server rejects the request.
        """
        return await run_in_thread(self.create_events_channel, channel)

    def create_events_store_channel(self, channel: str) -> bool | None:
        """Create an events store channel.

        Args:
            channel: The name of the channel to create.

        Returns:
            bool: ``True`` if the channel was created successfully.

        Raises:
            KubeMQConnectionError: If the server is unreachable.
            KubeMQAuthenticationError: If the auth token is invalid or the
                client lacks permission to create channels.
            KubeMQTimeoutError: If the operation exceeds the server deadline.
            KubeMQError: If the server rejects the request (e.g., channel
                already exists or invalid name).
        """
        return create_channel_request(
            self._transport, self._config.client_id, channel, "events_store"
        )

    async def create_events_store_channel_async(self, channel: str) -> bool | None:
        """Create an events store channel asynchronously.

        Args:
            channel: The name of the channel to create.

        Returns:
            bool: ``True`` if the channel was created successfully.

        Raises:
            KubeMQConnectionError: If the server is unreachable.
            KubeMQAuthenticationError: If the auth token is invalid or the
                client lacks permission to create channels.
            KubeMQTimeoutError: If the operation exceeds the server deadline.
            KubeMQError: If the server rejects the request.
        """
        return await run_in_thread(self.create_events_store_channel, channel)

    def delete_events_channel(self, channel: str) -> bool | None:
        """Delete an events channel.

        Args:
            channel: The name of the channel to delete.

        Returns:
            bool: ``True`` if the channel was deleted successfully.

        Raises:
            KubeMQConnectionError: If the server is unreachable.
            KubeMQAuthenticationError: If the auth token is invalid or the
                client lacks permission to delete channels.
            KubeMQChannelError: If the channel does not exist.
            KubeMQTimeoutError: If the operation exceeds the server deadline.
        """
        return delete_channel_request(self._transport, self._config.client_id, channel, "events")

    async def delete_events_channel_async(self, channel: str) -> bool | None:
        """Delete an events channel asynchronously.

        Args:
            channel: The name of the channel to delete.

        Returns:
            bool: ``True`` if the channel was deleted successfully.

        Raises:
            KubeMQConnectionError: If the server is unreachable.
            KubeMQAuthenticationError: If the auth token is invalid or the
                client lacks permission to delete channels.
            KubeMQChannelError: If the channel does not exist.
            KubeMQTimeoutError: If the operation exceeds the server deadline.
        """
        return await run_in_thread(self.delete_events_channel, channel)

    def delete_events_store_channel(self, channel: str) -> bool | None:
        """Delete an events store channel.

        Args:
            channel: The name of the channel to delete.

        Returns:
            bool: ``True`` if the channel was deleted successfully.

        Raises:
            KubeMQConnectionError: If the server is unreachable.
            KubeMQAuthenticationError: If the auth token is invalid or the
                client lacks permission to delete channels.
            KubeMQChannelError: If the channel does not exist.
            KubeMQTimeoutError: If the operation exceeds the server deadline.
        """
        return delete_channel_request(
            self._transport, self._config.client_id, channel, "events_store"
        )

    async def delete_events_store_channel_async(self, channel: str) -> bool | None:
        """Delete an events store channel asynchronously.

        Args:
            channel: The name of the channel to delete.

        Returns:
            bool: ``True`` if the channel was deleted successfully.

        Raises:
            KubeMQConnectionError: If the server is unreachable.
            KubeMQAuthenticationError: If the auth token is invalid or the
                client lacks permission to delete channels.
            KubeMQChannelError: If the channel does not exist.
            KubeMQTimeoutError: If the operation exceeds the server deadline.
        """
        return await run_in_thread(self.delete_events_store_channel, channel)

    def list_events_channels(self, channel_search: str = "") -> list[PubSubChannel]:
        """List events channels.

        Args:
            channel_search: Optional wildcard filter (e.g., ``"events.*"``).
                An empty string returns all channels.

        Returns:
            list[PubSubChannel]: Each entry contains the channel ``name``,
            ``type``, ``is_active`` status, and subscriber/publisher
            statistics.

        Raises:
            KubeMQConnectionError: If the server is unreachable.
            KubeMQAuthenticationError: If the auth token is invalid or the
                client lacks permission.
            KubeMQTimeoutError: If the operation exceeds the server deadline.
        """
        return list_pubsub_channels(
            self._transport, self._config.client_id, "events", channel_search
        )

    async def list_events_channels_async(self, channel_search: str = "") -> list[PubSubChannel]:
        """List events channels asynchronously.

        Args:
            channel_search: Optional wildcard filter (e.g., ``"events.*"``).
                An empty string returns all channels.

        Returns:
            list[PubSubChannel]: Each entry contains the channel ``name``,
            ``type``, ``is_active`` status, and subscriber/publisher
            statistics.

        Raises:
            KubeMQConnectionError: If the server is unreachable.
            KubeMQAuthenticationError: If the auth token is invalid or the
                client lacks permission.
            KubeMQTimeoutError: If the operation exceeds the server deadline.
        """
        return await run_in_thread(self.list_events_channels, channel_search)

    def list_events_store_channels(self, channel_search: str = "") -> list[PubSubChannel]:
        """List events store channels.

        Args:
            channel_search: Optional wildcard filter (e.g.,
                ``"events_store.*"``). An empty string returns all channels.

        Returns:
            list[PubSubChannel]: Each entry contains the channel ``name``,
            ``type``, ``is_active`` status, and subscriber/publisher
            statistics.

        Raises:
            KubeMQConnectionError: If the server is unreachable.
            KubeMQAuthenticationError: If the auth token is invalid or the
                client lacks permission.
            KubeMQTimeoutError: If the operation exceeds the server deadline.
        """
        return list_pubsub_channels(
            self._transport, self._config.client_id, "events_store", channel_search
        )

    async def list_events_store_channels_async(
        self, channel_search: str = ""
    ) -> list[PubSubChannel]:
        """List events store channels asynchronously.

        Args:
            channel_search: Optional wildcard filter (e.g.,
                ``"events_store.*"``). An empty string returns all channels.

        Returns:
            list[PubSubChannel]: Each entry contains the channel ``name``,
            ``type``, ``is_active`` status, and subscriber/publisher
            statistics.

        Raises:
            KubeMQConnectionError: If the server is unreachable.
            KubeMQAuthenticationError: If the auth token is invalid or the
                client lacks permission.
            KubeMQTimeoutError: If the operation exceeds the server deadline.
        """
        return await run_in_thread(self.list_events_store_channels, channel_search)

    # Subscription methods
    def subscribe_to_events(
        self, subscription: EventsSubscription, cancel: CancellationToken | None = None
    ) -> None:
        """Subscribe to events.

        Starts a background thread that streams events from the server.
        Messages are delivered to the subscription's callback. Errors
        (including stream breaks) are forwarded to the error callback
        and the stream is automatically reconnected with exponential
        backoff for retryable errors.

        Args:
            subscription: The subscription configuration including channel,
                optional group, and callback functions.
            cancel: Optional cancellation token to stop the subscription.

        Raises:
            KubeMQValidationError: If the subscription configuration is
                invalid (e.g., empty channel or missing callbacks).
            KubeMQConnectionError: If the server is unreachable or the
                initial connection fails.
            KubeMQAuthenticationError: If the auth token is invalid or the
                client lacks permission for the target channel.
            KubeMQClientClosedError: If the client has already been closed.

        Cancellation:
            Pass a ``CancellationToken`` to cancel the subscription from
            another thread. When cancelled, the subscription loop exits
            cleanly without raising an exception. The background thread
            terminates and resources are released.

        Callback Concurrency:
            Messages are delivered sequentially to the subscription's
            ``on_receive_message`` callback. Only one callback executes
            at a time per subscription. If you need concurrent processing,
            dispatch work to a ``concurrent.futures.ThreadPoolExecutor``
            within your callback.

        See Also:
            :class:`~kubemq.pubsub.events_subscription.EventsSubscription`:
                Subscription configuration for events.
            :meth:`send_event`: Publish events to a channel.
            :meth:`publish_event`: Fire-and-forget event publishing.

        Example:
            >>> from kubemq.pubsub import Client
            >>> from kubemq.pubsub.events_subscription import EventsSubscription
            >>> from kubemq.common.cancellation_token import CancellationToken
            >>> def on_event(event):
            ...     print(f"Received: {event.body}")
            >>> def on_error(err):
            ...     print(f"Error: {err}")
            >>> cancel = CancellationToken()
            >>> with Client(address="localhost:50000") as client:
            ...     client.subscribe_to_events(
            ...         EventsSubscription(
            ...             channel="events.>",
            ...             group="workers",
            ...             on_receive_event_callback=on_event,
            ...             on_error_callback=on_error,
            ...         ),
            ...         cancel=cancel,
            ...     )
        """
        self._subscribe(subscription, cancel)

    def subscribe_to_events_store(
        self, subscription: EventsStoreSubscription, cancel: CancellationToken | None = None
    ) -> None:
        """Subscribe to events store.

        Starts a background thread that streams persistent events from the
        server. Supports replay from a specific point in time, sequence
        number, or from the beginning of the store.

        Args:
            subscription: The subscription configuration including channel,
                store type (e.g., ``StartFromFirst``, ``StartAtSequence``),
                and callback functions.
            cancel: Optional cancellation token to stop the subscription.

        Raises:
            KubeMQValidationError: If the subscription configuration is
                invalid (e.g., empty channel or missing callbacks).
            KubeMQConnectionError: If the server is unreachable or the
                initial connection fails.
            KubeMQAuthenticationError: If the auth token is invalid or the
                client lacks permission for the target channel.
            KubeMQClientClosedError: If the client has already been closed.

        Cancellation:
            Pass a ``CancellationToken`` to cancel the subscription from
            another thread. When cancelled, the subscription loop exits
            cleanly without raising an exception.

        Callback Concurrency:
            Messages are delivered sequentially to the subscription's
            ``on_receive_message`` callback. Only one callback executes
            at a time per subscription.

        See Also:
            :class:`~kubemq.pubsub.events_store_subscription.EventsStoreSubscription`:
                Subscription configuration for persistent events.
            :meth:`send_event_store`: Publish persistent events.
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
        channel = subscription.channel
        args: tuple[Any, ...] = ()
        if isinstance(subscription, EventsStoreSubscription):
            last_seq = [0]

            def make_store_stream() -> Any:
                sub = subscription
                if last_seq[0] > 0:
                    sub = dataclasses.replace(
                        subscription,
                        events_store_type=EventStoreStartPosition.StartAtSequence,
                        events_store_sequence_value=last_seq[0] + 1,
                    )
                return transport.kubemq_client().SubscribeToEvents(sub.encode(client_id))

            def decode_and_track(message: Any) -> None:
                received = EventStoreReceived().decode(message)
                if received.sequence > 0:
                    last_seq[0] = received.sequence
                subscription.raise_on_receive_message(received)

            args = (
                make_store_stream,
                decode_and_track,
                lambda error: subscription.raise_on_error(error),
                cancel_token_event,
                channel,
                transport,
            )
        if isinstance(subscription, EventsSubscription):
            args = (
                lambda: transport.kubemq_client().SubscribeToEvents(subscription.encode(client_id)),
                lambda message: subscription.raise_on_receive_message(
                    EventReceived().decode(message)
                ),
                lambda error: subscription.raise_on_error(error),
                cancel_token_event,
                channel,
                transport,
            )
        thread = threading.Thread(target=self._subscribe_task, args=args, daemon=True)
        self._register_subscription_thread(thread)
        thread.start()

    def _subscribe_task(
        self,
        stream_callable: Any,
        decode_callable: Any,
        error_callable: Any,
        cancel_token: threading.Event,
        channel: str = "",
        transport: Any = None,
    ) -> None:
        """Background subscription task with exponential backoff on stream breaks."""
        backoff = BackoffCalculator(self._config.retry_policy)
        attempt = 0
        # PY-7: Track time of last successful message for backoff cooldown.
        # Only reset backoff after sustained success (5 seconds of no errors).
        _BACKOFF_COOLDOWN_SECONDS = 5.0
        _last_error_time: float = 0.0
        # PY-6: Track watcher thread and cancel event for cleanup.
        _watcher_thread: threading.Thread | None = None
        _watcher_cancel = threading.Event()

        while not cancel_token.is_set() and not self._shutdown_event.is_set():
            try:
                # PY-2: Force channel rebuild on retry iterations to get a
                # fresh gRPC channel instead of relying on transparent reconnect.
                if attempt > 0 and transport is not None:
                    try:
                        transport.recreate_channel()
                    except Exception:
                        pass  # stream_callable will fail and trigger backoff

                response = stream_callable()

                # PY-6: Signal and join the old watcher thread before
                # creating a new one to prevent thread leaks.
                if _watcher_thread is not None:
                    _watcher_cancel.set()
                    _watcher_thread.join(timeout=2.0)
                _watcher_cancel = threading.Event()

                # Start a watcher thread to cancel the gRPC stream when
                # the cancellation token or shutdown event fires.
                def _cancel_watcher(resp=response, stop=_watcher_cancel):  # type: ignore[assignment]
                    while (
                        not cancel_token.is_set()
                        and not self._shutdown_event.is_set()
                        and not stop.is_set()
                    ):
                        cancel_token.wait(timeout=0.5)
                    try:
                        resp.cancel()
                    except Exception:
                        pass

                _watcher_thread = threading.Thread(target=_cancel_watcher, daemon=True)
                _watcher_thread.start()

                for message in response:
                    if cancel_token.is_set():
                        break
                    # PY-7: Only reset backoff after sustained success
                    # (cooldown period with no errors), not after a single message.
                    now = time.monotonic()
                    if _last_error_time == 0.0 or (now - _last_error_time) >= _BACKOFF_COOLDOWN_SECONDS:
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
                _last_error_time = time.monotonic()  # PY-7
                sdk_error = convert_grpc_error(e, operation="Subscribe")
                stream_error = KubeMQStreamBrokenError(
                    f"Stream broken: {sdk_error.message}",
                    operation=sdk_error.operation,
                    channel=sdk_error.channel,
                    cause=e,
                )
                error_callable(str(stream_error))

                # PY-1: Only break on truly fatal codes (auth/permission).
                # Transient non-retryable codes like INTERNAL can occur
                # during broker restarts and should be retried with backoff.
                _FATAL_CODES = {ErrorCode.AUTH_FAILED, ErrorCode.PERMISSION_DENIED}
                if not sdk_error.is_retryable and sdk_error.code in _FATAL_CODES:
                    break

                delay = backoff.delay_seconds(attempt)
                attempt += 1
                cancel_token.wait(timeout=delay)
                continue
            except Exception as e:
                _last_error_time = time.monotonic()  # PY-7
                error_callable(decode_grpc_error(e))
                delay = backoff.delay_seconds(attempt)
                attempt += 1
                cancel_token.wait(timeout=delay)
                continue

    # Async subscription methods
    def subscribe_to_events_async(
        self, subscription: EventsSubscription, cancel: CancellationToken | None = None
    ) -> asyncio.Task[None]:
        """Asynchronous version of subscribe_to_events().

        Supports both sync and async callbacks.

        Args:
            subscription: The subscription configuration including channel,
                optional group, and callback functions.
            cancel: Optional cancellation token to stop the subscription.

        Returns:
            asyncio.Task[None]: A task that runs the subscription loop.
            Cancel via the ``CancellationToken`` or by cancelling the task.

        Raises:
            KubeMQValidationError: If the subscription configuration is
                invalid.
            KubeMQConnectionError: If the server is unreachable.
            KubeMQAuthenticationError: If authentication or authorization
                fails.
            KubeMQClientClosedError: If the client has already been closed.
        """
        return self._subscribe_async(subscription, cancel)

    def subscribe_to_events_store_async(
        self, subscription: EventsStoreSubscription, cancel: CancellationToken | None = None
    ) -> asyncio.Task[None]:
        """Asynchronous version of subscribe_to_events_store().

        Supports both sync and async callbacks.

        Args:
            subscription: The subscription configuration including channel,
                store type, and callback functions.
            cancel: Optional cancellation token to stop the subscription.

        Returns:
            asyncio.Task[None]: A task that runs the subscription loop.
            Cancel via the ``CancellationToken`` or by cancelling the task.

        Raises:
            KubeMQValidationError: If the subscription configuration is
                invalid.
            KubeMQConnectionError: If the server is unreachable.
            KubeMQAuthenticationError: If authentication or authorization
                fails.
            KubeMQClientClosedError: If the client has already been closed.
        """
        return self._subscribe_async(subscription, cancel)

    def _subscribe_async(
        self,
        subscription: EventsSubscription | EventsStoreSubscription,
        cancel: CancellationToken | None,
    ) -> asyncio.Task[None]:
        """Internal async subscription handler."""
        self._ensure_connected()
        assert self._transport is not None
        if cancel is None:
            cancel = CancellationToken()
        cancel_token_event = cancel.event
        transport = self._transport  # Capture for lambda
        client_id = self._config.client_id or ""
        channel = subscription.channel
        args: tuple[Any, ...] = ()
        if isinstance(subscription, EventsStoreSubscription):
            last_seq = [0]

            def make_store_stream_async() -> Any:
                sub = subscription
                if last_seq[0] > 0:
                    sub = dataclasses.replace(
                        subscription,
                        events_store_type=EventStoreStartPosition.StartAtSequence,
                        events_store_sequence_value=last_seq[0] + 1,
                    )
                return transport.kubemq_client().SubscribeToEvents(sub.encode(client_id))

            async def decode_and_track_async(message: Any) -> None:
                received = EventStoreReceived().decode(message)
                if received.sequence > 0:
                    last_seq[0] = received.sequence
                await subscription.raise_on_receive_message_async(received)

            args = (
                make_store_stream_async,
                decode_and_track_async,
                lambda error: subscription.raise_on_error_async(error),
                cancel_token_event,
                channel,
            )
        if isinstance(subscription, EventsSubscription):
            args = (
                lambda: transport.kubemq_client().SubscribeToEvents(subscription.encode(client_id)),
                lambda message: subscription.raise_on_receive_message_async(
                    EventReceived().decode(message)
                ),
                lambda error: subscription.raise_on_error_async(error),
                cancel_token_event,
                channel,
            )
        return asyncio.create_task(self._subscribe_task_async(*args))

    async def _subscribe_task_async(
        self,
        stream_callable: Any,
        decode_callable: Any,
        error_callable: Any,
        cancel_token: threading.Event,
        channel: str = "",
    ) -> None:
        """Async version of subscription task with exponential backoff."""
        backoff = BackoffCalculator(self._config.retry_policy)
        attempt = 0

        while not cancel_token.is_set() and not self._shutdown_event.is_set():
            try:
                response = await asyncio.to_thread(stream_callable)

                while not cancel_token.is_set() and not self._shutdown_event.is_set():
                    try:
                        message = await asyncio.to_thread(next, response)
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
                                await decode_callable(message)
                                self._instrumentor._metrics.record_consumed_message(
                                    "process", channel
                                )
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
                                await error_callable(str(handler_error))
                            finally:
                                duration = time.perf_counter() - start
                                self._instrumentor._metrics.record_operation_duration(
                                    duration, "process", channel, error_type_val
                                )
                    except StopIteration:
                        break
            except grpc.RpcError as e:
                sdk_error = convert_grpc_error(e, operation="Subscribe")
                stream_error = KubeMQStreamBrokenError(
                    f"Stream broken: {sdk_error.message}",
                    operation=sdk_error.operation,
                    channel=sdk_error.channel,
                    cause=e,
                )
                await error_callable(str(stream_error))

                if not sdk_error.is_retryable:
                    break

                delay = backoff.delay_seconds(attempt)
                attempt += 1
                await asyncio.sleep(delay)
                continue
            except Exception as e:
                await error_callable(decode_grpc_error(e))
                delay = backoff.delay_seconds(attempt)
                attempt += 1
                await asyncio.sleep(delay)
                continue
