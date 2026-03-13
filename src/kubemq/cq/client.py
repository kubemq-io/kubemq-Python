"""Commands and Queries client for KubeMQ."""

from __future__ import annotations

import asyncio
import threading
import time
from pathlib import Path

import grpc

from kubemq._internal.telemetry import KubeMQTagsCarrier, create_link_from_context, error_code_to_error_type, serialize_span_to_bytes
from kubemq.common.cancellation_token import CancellationToken
from kubemq.common.channel_stats import CQChannel
from kubemq.common.helpers import decode_grpc_error
from kubemq.common.requests import (
    create_channel_request,
    delete_channel_request,
    list_cq_channels,
)
from pydantic import ValidationError

from kubemq.core import BaseClient, ClientConfig
from kubemq._internal.deprecation import deprecated, deprecated_async
from kubemq.core.exceptions import KubeMQValidationError
from kubemq.core.compat import run_in_thread
from kubemq.core.config import KeepAliveConfig, TLSConfig
from kubemq.cq.command_message import CommandMessage
from kubemq.cq.command_message_received import CommandMessageReceived
from kubemq.cq.command_response_message import CommandResponseMessage
from kubemq.cq.commands_subscription import CommandsSubscription
from kubemq.cq.queries_subscription import QueriesSubscription
from kubemq.cq.query_message import QueryMessage
from kubemq.cq.query_message_received import QueryMessageReceived
from kubemq.cq.query_response_message import QueryResponseMessage
from kubemq.transport.server_info import ServerInfo


class Client(BaseClient):
    """Commands and Queries client for KubeMQ.

    Note:
        The `*_async()` methods on this class use thread-based async wrappers.
        For native async support with better performance, use `AsyncClient`
        (or `AsyncCQClient`) from `kubemq.cq.async_client` instead::

            from kubemq.cq import AsyncClient

            async with AsyncClient(address="localhost:50000") as client:
                response = await client.send_command(CommandMessage(
                    channel="my-commands",
                    body=b"do something",
                    timeout_in_seconds=30,
                ))

    Example:
        # Using context manager (recommended)
        with Client(address="localhost:50000") as client:
            response = client.send_command_request(CommandMessage(
                channel="my-commands",
                body=b"do something"
            ))

        # Using configuration object
        config = ClientConfig(address="localhost:50000", client_id="my-app")
        client = Client(config=config)
        try:
            response = client.send_command_request(...)
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
        """Initialize the Commands and Queries client.

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
        self._logger.debug(f"Client disconnecting from {self._config.address}")
        if self._transport is not None:
            await self._transport.close_async()
        self._logger.debug(f"Client disconnected from {self._config.address}")
        self._shutdown_event.set()

    @deprecated_async(replacement="AsyncCQClient.ping()", since="4.0.0", removal="5.0.0")
    async def ping_async(self) -> ServerInfo:
        """Asynchronous version of ping().

        Deprecated:
            Use `AsyncCQClient.ping()` for native async support.

        Returns:
            ServerInfo: The server information.

        Raises:
            KubeMQConnectionError: If an error occurs during the ping.
        """
        return await run_in_thread(self.ping)

    # Command methods — GS-aligned verbs

    def _send_command_impl(self, message: CommandMessage) -> CommandResponseMessage:
        """Internal implementation for sending a command."""
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
                return CommandResponseMessage().decode(response)
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

    def send_command(self, message: CommandMessage) -> CommandResponseMessage:
        """Send a command request and wait for response.

        Args:
            message: The command message to send.

        Returns:
            CommandResponseMessage with the response.
        """
        return self._send_command_impl(message)

    @deprecated(replacement="send_command()", since="4.0.0", removal="5.0.0")
    def send_command_request(self, message: CommandMessage) -> CommandResponseMessage:
        """Send a command request and wait for response.

        Deprecated:
            Use ``send_command()`` instead. Will be removed in v5.0.

        Args:
            message: The command message to send

        Returns:
            CommandResponseMessage with the response
        """
        return self._send_command_impl(message)

    @deprecated_async(replacement="AsyncCQClient.send_command()", since="4.0.0", removal="5.0.0")
    async def send_command_request_async(self, message: CommandMessage) -> CommandResponseMessage:
        """Send a command request asynchronously.

        Deprecated:
            Use ``AsyncCQClient.send_command()`` for native async support.

        Args:
            message: The command message to send

        Returns:
            CommandResponseMessage with the response
        """
        return await run_in_thread(self._send_command_impl, message)

    # Query methods — GS-aligned verbs

    def _send_query_impl(self, message: QueryMessage) -> QueryResponseMessage:
        """Internal implementation for sending a query."""
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
                return QueryResponseMessage().decode(response)
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

    def send_query(self, message: QueryMessage) -> QueryResponseMessage:
        """Send a query request and wait for response.

        Args:
            message: The query message to send.

        Returns:
            QueryResponseMessage with the response.
        """
        return self._send_query_impl(message)

    @deprecated(replacement="send_query()", since="4.0.0", removal="5.0.0")
    def send_query_request(self, message: QueryMessage) -> QueryResponseMessage:
        """Send a query request and wait for response.

        Deprecated:
            Use ``send_query()`` instead. Will be removed in v5.0.

        Args:
            message: The query message to send

        Returns:
            QueryResponseMessage with the response
        """
        return self._send_query_impl(message)

    @deprecated_async(replacement="AsyncCQClient.send_query()", since="4.0.0", removal="5.0.0")
    async def send_query_request_async(self, message: QueryMessage) -> QueryResponseMessage:
        """Send a query request asynchronously.

        Deprecated:
            Use ``AsyncCQClient.send_query()`` for native async support.

        Args:
            message: The query message to send

        Returns:
            QueryResponseMessage with the response
        """
        return await run_in_thread(self._send_query_impl, message)

    # Response methods
    def send_response_message(self, message: CommandResponseMessage | QueryResponseMessage) -> None:
        """Send a response message.

        Args:
            message: The response message to send
        """
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

    @deprecated_async(replacement="AsyncCQClient.send_response()", since="4.0.0", removal="5.0.0")
    async def send_response_message_async(
        self, message: CommandResponseMessage | QueryResponseMessage
    ) -> None:
        """Send a response message asynchronously.

        Deprecated:
            Use `AsyncCQClient.send_response()` for native async support.

        Args:
            message: The response message to send
        """
        await run_in_thread(self.send_response_message, message)

    # Channel management methods
    def create_commands_channel(self, channel: str) -> bool | None:
        """Create a commands channel.

        Args:
            channel: The name of the channel to create

        Returns:
            True if successful, None if there was an error
        """
        return create_channel_request(self._transport, self._config.client_id, channel, "commands")

    async def create_commands_channel_async(self, channel: str) -> bool | None:
        """Create a commands channel asynchronously.

        Args:
            channel: The name of the channel to create

        Returns:
            True if successful, None if there was an error
        """
        return await run_in_thread(self.create_commands_channel, channel)

    def create_queries_channel(self, channel: str) -> bool | None:
        """Create a queries channel.

        Args:
            channel: The name of the channel to create

        Returns:
            True if successful, None if there was an error
        """
        return create_channel_request(self._transport, self._config.client_id, channel, "queries")

    async def create_queries_channel_async(self, channel: str) -> bool | None:
        """Create a queries channel asynchronously.

        Args:
            channel: The name of the channel to create

        Returns:
            True if successful, None if there was an error
        """
        return await run_in_thread(self.create_queries_channel, channel)

    def delete_commands_channel(self, channel: str) -> bool | None:
        """Delete a commands channel.

        Args:
            channel: The name of the channel to delete

        Returns:
            True if successful, None if there was an error
        """
        return delete_channel_request(self._transport, self._config.client_id, channel, "commands")

    async def delete_commands_channel_async(self, channel: str) -> bool | None:
        """Delete a commands channel asynchronously.

        Args:
            channel: The name of the channel to delete

        Returns:
            True if successful, None if there was an error
        """
        return await run_in_thread(self.delete_commands_channel, channel)

    def delete_queries_channel(self, channel: str) -> bool | None:
        """Delete a queries channel.

        Args:
            channel: The name of the channel to delete

        Returns:
            True if successful, None if there was an error
        """
        return delete_channel_request(self._transport, self._config.client_id, channel, "queries")

    async def delete_queries_channel_async(self, channel: str) -> bool | None:
        """Delete a queries channel asynchronously.

        Args:
            channel: The name of the channel to delete

        Returns:
            True if successful, None if there was an error
        """
        return await run_in_thread(self.delete_queries_channel, channel)

    def list_commands_channels(self, channel_search: str = "") -> list[CQChannel]:
        """List commands channels.

        Args:
            channel_search: Optional filter string

        Returns:
            List of CQChannel objects
        """
        return list_cq_channels(self._transport, self._config.client_id, "commands", channel_search)

    async def list_commands_channels_async(self, channel_search: str = "") -> list[CQChannel]:
        """List commands channels asynchronously.

        Args:
            channel_search: Optional filter string

        Returns:
            List of CQChannel objects
        """
        return await run_in_thread(self.list_commands_channels, channel_search)

    def list_queries_channels(self, channel_search: str = "") -> list[CQChannel]:
        """List queries channels.

        Args:
            channel_search: Optional filter string

        Returns:
            List of CQChannel objects
        """
        return list_cq_channels(self._transport, self._config.client_id, "queries", channel_search)

    async def list_queries_channels_async(self, channel_search: str = "") -> list[CQChannel]:
        """List queries channels asynchronously.

        Args:
            channel_search: Optional filter string

        Returns:
            List of CQChannel objects
        """
        return await run_in_thread(self.list_queries_channels, channel_search)

    # Subscription methods
    def subscribe_to_commands(
        self, subscription: CommandsSubscription, cancel: CancellationToken | None = None
    ) -> None:
        """Subscribe to commands.

        Args:
            subscription: The subscription configuration
            cancel: Optional cancellation token

        Cancellation:
            Pass a ``CancellationToken`` to cancel the subscription from
            another thread. When cancelled, the subscription loop exits
            cleanly without raising an exception.

        Callback Concurrency:
            Messages are delivered sequentially to the subscription's
            ``on_receive_command`` callback. Only one callback executes
            at a time per subscription.
        """
        self._subscribe(subscription, cancel)

    def subscribe_to_queries(
        self, subscription: QueriesSubscription, cancel: CancellationToken | None = None
    ) -> None:
        """Subscribe to queries.

        Args:
            subscription: The subscription configuration
            cancel: Optional cancellation token

        Cancellation:
            Pass a ``CancellationToken`` to cancel the subscription from
            another thread. When cancelled, the subscription loop exits
            cleanly without raising an exception.

        Callback Concurrency:
            Messages are delivered sequentially to the subscription's
            ``on_receive_query`` callback. Only one callback executes
            at a time per subscription.
        """
        self._subscribe(subscription, cancel)

    def _subscribe(
        self,
        subscription: CommandsSubscription | QueriesSubscription,
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
        args: tuple = ()
        if isinstance(subscription, CommandsSubscription):
            args = (
                lambda: transport.kubemq_client().SubscribeToRequests(
                    subscription.encode(client_id)
                ),
                lambda message: subscription.raise_on_receive_message(
                    CommandMessageReceived().decode(message)
                ),
                lambda error: subscription.raise_on_error(error),
                cancel_token_event,
                channel,
            )
        if isinstance(subscription, QueriesSubscription):
            args = (
                lambda: transport.kubemq_client().SubscribeToRequests(
                    subscription.encode(client_id)
                ),
                lambda message: subscription.raise_on_receive_message(
                    QueryMessageReceived().decode(message)
                ),
                lambda error: subscription.raise_on_error(error),
                cancel_token_event,
                channel,
            )
        thread = threading.Thread(target=self._subscribe_task, args=args, daemon=True)
        self._register_subscription_thread(thread)
        thread.start()

    def _subscribe_task(
        self,
        stream_callable,
        decode_callable,
        error_callable,
        cancel_token: threading.Event,
        channel: str = "",
    ) -> None:
        """Background subscription task."""
        while not cancel_token.is_set() and not self._shutdown_event.is_set():
            try:
                response = stream_callable()
                for message in response:
                    if cancel_token.is_set():
                        break
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
                            self._instrumentor._metrics.record_consumed_message(
                                "process", channel
                            )
                        except Exception as handler_err:
                            error_type_val = error_code_to_error_type(
                                getattr(handler_err, "code", None)
                            )
                            self._instrumentor.record_error(
                                span, handler_err, error_type_val
                            )
                        finally:
                            duration = time.perf_counter() - start
                            self._instrumentor._metrics.record_operation_duration(
                                duration, "process", channel, error_type_val
                            )
            except grpc.RpcError as e:
                error_callable(decode_grpc_error(e))
                time.sleep(self._config.reconnect_interval_seconds)
                continue
            except Exception as e:
                error_callable(decode_grpc_error(e))
                time.sleep(self._config.reconnect_interval_seconds)
                continue

    # Async subscription methods
    def subscribe_to_commands_async(
        self, subscription: CommandsSubscription, cancel: CancellationToken | None = None
    ):
        """Asynchronous version of subscribe_to_commands().

        Supports both sync and async callbacks.

        Args:
            subscription: The subscription configuration
            cancel: Optional cancellation token

        Returns:
            asyncio.Task object that can be awaited
        """
        return self._subscribe_async(subscription, cancel)

    def subscribe_to_queries_async(
        self, subscription: QueriesSubscription, cancel: CancellationToken | None = None
    ):
        """Asynchronous version of subscribe_to_queries().

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
        subscription: CommandsSubscription | QueriesSubscription,
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
        channel = subscription.channel
        args: tuple = ()
        if isinstance(subscription, CommandsSubscription):
            args = (
                lambda: transport.kubemq_client().SubscribeToRequests(
                    subscription.encode(client_id)
                ),
                lambda message: subscription.raise_on_receive_message_async(
                    CommandMessageReceived().decode(message)
                ),
                lambda error: subscription.raise_on_error_async(error),
                cancel_token_event,
                channel,
            )
        if isinstance(subscription, QueriesSubscription):
            args = (
                lambda: transport.kubemq_client().SubscribeToRequests(
                    subscription.encode(client_id)
                ),
                lambda message: subscription.raise_on_receive_message_async(
                    QueryMessageReceived().decode(message)
                ),
                lambda error: subscription.raise_on_error_async(error),
                cancel_token_event,
                channel,
            )
        return asyncio.create_task(self._subscribe_task_async(*args))  # type: ignore[arg-type]

    async def _subscribe_task_async(
        self,
        stream_callable,
        decode_callable,
        error_callable,
        cancel_token: threading.Event,
        channel: str = "",
    ):
        """Async version of subscription task that supports async callbacks."""
        while not cancel_token.is_set() and not self._shutdown_event.is_set():
            try:
                response = await asyncio.to_thread(stream_callable)

                while not cancel_token.is_set() and not self._shutdown_event.is_set():
                    try:
                        message = await asyncio.to_thread(next, response)
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
                                self._instrumentor.record_error(
                                    span, handler_err, error_type_val
                                )
                            finally:
                                duration = time.perf_counter() - start
                                self._instrumentor._metrics.record_operation_duration(
                                    duration, "process", channel, error_type_val
                                )
                    except StopIteration:
                        break
            except grpc.RpcError as e:
                await error_callable(decode_grpc_error(e))
                await asyncio.sleep(self._config.reconnect_interval_seconds)
                continue
            except Exception as e:
                await error_callable(decode_grpc_error(e))
                await asyncio.sleep(self._config.reconnect_interval_seconds)
                continue
