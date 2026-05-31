"""Native async Commands and Queries client for KubeMQ."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import (
    TYPE_CHECKING,
    Any,
)

from kubemq._internal.retry import BackoffCalculator
from kubemq._internal.telemetry import (
    KubeMQTagsCarrier,
    create_link_from_context,
    error_code_to_error_type,
    serialize_span_to_bytes,
)
from kubemq.common.async_cancellation_token import AsyncCancellationToken
from kubemq.core.client import NativeAsyncBaseClient
from kubemq.core.config import ClientConfig
from kubemq.core.exceptions import (
    KubeMQClientClosedError,
    KubeMQConnectionError,
    KubeMQError,
    KubeMQValidationError,
)
from kubemq.cq.command_message import CommandMessage
from kubemq.cq.command_message_received import CommandReceived
from kubemq.cq.command_response_message import CommandResponse
from kubemq.cq.commands_subscription import CommandsSubscription
from kubemq.cq.queries_subscription import QueriesSubscription
from kubemq.cq.query_message import QueryMessage
from kubemq.cq.query_message_received import QueryReceived
from kubemq.cq.query_response_message import QueryResponse

if TYPE_CHECKING:
    pass

_logger = logging.getLogger("kubemq.cq.async_client")

# Type aliases for callbacks
AsyncCommandCallback = Callable[[CommandReceived], Awaitable[None]]
AsyncQueryCallback = Callable[[QueryReceived], Awaitable[None]]
AsyncErrorCallback = Callable[[Exception], Awaitable[None]]


class AsyncClient(NativeAsyncBaseClient):
    """Native async Commands and Queries client.

    Provides command/query request-response patterns with async support
    using native gRPC async.

    Example:
        async with AsyncClient(address="localhost:50000") as client:
            # Send command
            response = await client.send_command(CommandMessage(
                channel="commands",
                body=b"do something",
                timeout_in_seconds=30,
            ))

            # Send query
            response = await client.send_query(QueryMessage(
                channel="queries",
                body=b"get data",
                timeout_in_seconds=30,
            ))

            # Subscribe to commands
            token = AsyncCancellationToken()
            async for command in client.subscribe_to_commands(
                CommandsSubscription(
                    channel="commands",
                    on_receive_command_callback=lambda c: print(c.body),
                ),
                cancellation_token=token,
            ):
                # Process command
                await client.send_response(
                    CommandResponse(
                        command_received=command,
                        is_executed=True,
                    )
                )

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
        """Initialize the async CQ client.

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
    # Command Operations
    # =========================================================================

    async def send_command(
        self,
        message: CommandMessage,
    ) -> CommandResponse:
        """Send a command request and wait for response.

        Sends a command to a subscriber and awaits until a response is
        received or the command's ``timeout_in_seconds`` expires.

        Args:
            message: The command message to send.

        Returns:
            CommandResponse: Contains ``command_received`` (bool
            indicating a subscriber processed the command),
            ``is_executed`` (bool indicating execution success),
            ``error`` (error description from the responder),
            ``timestamp`` (when the response was generated), and
            ``tags`` (key-value metadata from the responder).

        Raises:
            KubeMQValidationError: If the message fails validation (e.g.,
                empty channel, body exceeds ``max_send_size``).
            KubeMQConnectionError: If the server is unreachable or the
                connection is lost.
            KubeMQAuthenticationError: If the auth token is invalid or
                expired, or the client lacks permission for the channel.
            KubeMQTimeoutError: If no subscriber responds before the
                command's timeout expires.
            KubeMQClientClosedError: If the client has already been closed.

        See Also:
            :class:`~kubemq.cq.command_message.CommandMessage`:
                Message type for commands.
            :meth:`CQClient.send_command`: Sync counterpart.
            :meth:`send_query`: Send a query expecting data in the
                response.
            :meth:`subscribe_to_commands`: Subscribe to receive commands.
        """
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
                    self._pick_pool_transport().send_request,
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

    async def send_commands_batch(
        self,
        messages: list[CommandMessage],
        max_concurrent: int = 100,
    ) -> list[CommandResponse]:
        """Send multiple commands concurrently with backpressure control.

        Args:
            messages: List of command messages to send.
            max_concurrent: Maximum concurrent sends (default 100).

        Returns:
            list[CommandResponse]: List of responses for each
            command, in the same order as the input messages. Failed
            sends produce responses with ``is_executed=False`` and
            ``error`` set.

        Raises:
            KubeMQValidationError: If any message fails validation.
            KubeMQConnectionError: If the server is unreachable or the
                connection is lost.
            KubeMQAuthenticationError: If the auth token is invalid or
                expired.
            KubeMQClientClosedError: If the client has already been closed.

        See Also:
            :meth:`send_command`: Send a single command.
            :meth:`send_queries_batch`: Send multiple queries concurrently.
        """
        self._ensure_connected()

        semaphore = asyncio.Semaphore(max_concurrent)

        async def bounded_send(msg: CommandMessage, index: int) -> tuple[int, CommandResponse]:
            async with semaphore:
                try:
                    response = await self.send_command(msg)
                    return (index, response)
                except Exception as e:
                    return (
                        index,
                        CommandResponse(
                            request_id=msg.id or "",
                            is_executed=False,
                            error=str(e),
                        ),
                    )

        tasks = [bounded_send(msg, i) for i, msg in enumerate(messages)]
        indexed_results = await asyncio.gather(*tasks)

        sorted_results = sorted(indexed_results, key=lambda x: x[0])
        return [result for _, result in sorted_results]

    # =========================================================================
    # Query Operations
    # =========================================================================

    async def send_query(
        self,
        message: QueryMessage,
    ) -> QueryResponse:
        """Send a query request and wait for response.

        Sends a query to a subscriber and awaits until a response
        containing data is received or the query's
        ``timeout_in_seconds`` expires.

        Args:
            message: The query message to send.

        Returns:
            QueryResponse: Contains ``body`` (response payload as
            bytes), ``metadata`` (response metadata string),
            ``is_executed`` (bool indicating execution success),
            ``error`` (error description from the responder),
            ``timestamp``, ``cache_hit`` (bool), and ``tags``
            (key-value metadata from the responder).

        Raises:
            KubeMQValidationError: If the message fails validation (e.g.,
                empty channel, body exceeds ``max_send_size``).
            KubeMQConnectionError: If the server is unreachable or the
                connection is lost.
            KubeMQAuthenticationError: If the auth token is invalid or
                expired, or the client lacks permission for the channel.
            KubeMQTimeoutError: If no subscriber responds before the
                query's timeout expires.
            KubeMQClientClosedError: If the client has already been closed.

        See Also:
            :class:`~kubemq.cq.query_message.QueryMessage`:
                Message type for queries.
            :meth:`CQClient.send_query`: Sync counterpart.
            :meth:`send_command`: Send a command expecting only
                success/failure.
            :meth:`subscribe_to_queries`: Subscribe to receive queries.
        """
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
                    self._pick_pool_transport().send_request,
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

    async def send_queries_batch(
        self,
        messages: list[QueryMessage],
        max_concurrent: int = 100,
    ) -> list[QueryResponse]:
        """Send multiple queries concurrently with backpressure control.

        Args:
            messages: List of query messages to send.
            max_concurrent: Maximum concurrent sends (default 100).

        Returns:
            list[QueryResponse]: List of responses for each
            query, in the same order as the input messages. Failed
            sends produce responses with ``is_executed=False`` and
            ``error`` set.

        Raises:
            KubeMQValidationError: If any message fails validation.
            KubeMQConnectionError: If the server is unreachable or the
                connection is lost.
            KubeMQAuthenticationError: If the auth token is invalid or
                expired.
            KubeMQClientClosedError: If the client has already been closed.

        See Also:
            :meth:`send_query`: Send a single query.
            :meth:`send_commands_batch`: Send multiple commands concurrently.
        """
        self._ensure_connected()

        semaphore = asyncio.Semaphore(max_concurrent)

        async def bounded_send(msg: QueryMessage, index: int) -> tuple[int, QueryResponse]:
            async with semaphore:
                try:
                    response = await self.send_query(msg)
                    return (index, response)
                except Exception as e:
                    return (
                        index,
                        QueryResponse(
                            request_id=msg.id or "",
                            is_executed=False,
                            error=str(e),
                        ),
                    )

        tasks = [bounded_send(msg, i) for i, msg in enumerate(messages)]
        indexed_results = await asyncio.gather(*tasks)

        sorted_results = sorted(indexed_results, key=lambda x: x[0])
        return [result for _, result in sorted_results]

    # =========================================================================
    # Response Operations
    # =========================================================================

    async def send_response(
        self,
        response: CommandResponse | QueryResponse,
    ) -> None:
        """Send a response to a command or query request.

        Called from within a command or query subscription callback to
        deliver the response to the original requester.

        Args:
            response: The response message to send. Use
                :class:`CommandResponse` for commands or
                :class:`QueryResponse` for queries.

        Raises:
            KubeMQConnectionError: If the server is unreachable or the
                connection is lost.
            KubeMQAuthenticationError: If the auth token is invalid or
                expired, or the client lacks permission.
            KubeMQTimeoutError: If the operation exceeds the server deadline.
            KubeMQClientClosedError: If the client has already been closed.

        See Also:
            :meth:`CQClient.send_response_message`: Sync counterpart.
            :meth:`subscribe_to_commands`: Subscribe to receive commands.
            :meth:`subscribe_to_queries`: Subscribe to receive queries.
        """
        self._ensure_connected()
        assert self._transport is not None
        channel = getattr(response, "reply_channel", "") or ""
        start = time.perf_counter()
        error_type_val = None
        with self._instrumentor.start_span("settle", channel) as span:
            try:
                pb_response = response.encode(self._config.client_id or "")
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

    # =========================================================================
    async def send_command_fast(self, message: CommandMessage) -> CommandResponse:
        """Send command — fast path, no instrumentation.

        Uses pipeline semaphore and connection pool when enabled for
        higher concurrent throughput.
        """
        self._ensure_connected()
        pb_request = message.encode(self._config.client_id or "")
        transport = self._pick_pool_transport()

        if self._pipeline_sem is not None:
            async with self._pipeline_sem:
                response = await self._retry_executor.execute(
                    "SendCommand",
                    transport.send_request,
                    pb_request,
                    timeout_seconds=message.timeout_in_seconds,
                    channel=message.channel,
                )
        else:
            response = await self._retry_executor.execute(
                "SendCommand",
                transport.send_request,
                pb_request,
                timeout_seconds=message.timeout_in_seconds,
                channel=message.channel,
            )
        return CommandResponse.decode(response)

    async def send_query_fast(self, message: QueryMessage) -> QueryResponse:
        """Send query — fast path, no instrumentation.

        Uses pipeline semaphore and connection pool when enabled for
        higher concurrent throughput.
        """
        self._ensure_connected()
        pb_request = message.encode(self._config.client_id or "")
        transport = self._pick_pool_transport()

        if self._pipeline_sem is not None:
            async with self._pipeline_sem:
                response = await self._retry_executor.execute(
                    "SendQuery",
                    transport.send_request,
                    pb_request,
                    timeout_seconds=message.timeout_in_seconds,
                    channel=message.channel,
                )
        else:
            response = await self._retry_executor.execute(
                "SendQuery",
                transport.send_request,
                pb_request,
                timeout_seconds=message.timeout_in_seconds,
                channel=message.channel,
            )
        return QueryResponse.decode(response)

    async def send_response_fast(self, response: CommandResponse | QueryResponse) -> None:
        """Send response — fast path, no instrumentation."""
        self._ensure_connected()
        assert self._transport is not None
        pb_response = response.encode(self._config.client_id or "")
        await self._transport.send_response(pb_response)

    # Subscription Operations
    # =========================================================================

    async def subscribe_to_commands_fast(
        self,
        subscription: CommandsSubscription,
        cancellation_token: AsyncCancellationToken | None = None,
    ) -> AsyncIterator[CommandReceived]:
        """Subscribe to commands -- fast path with automatic reconnection."""
        token = cancellation_token or AsyncCancellationToken()
        backoff = BackoffCalculator(self._config.retry_policy)
        attempt = 0

        while not token.is_cancelled:
            try:
                self._ensure_connected()
                assert self._transport is not None
                request = subscription.encode(self._config.client_id or "")
                async for pb_request in self._transport.subscribe_to_requests(request, token):
                    attempt = 0
                    yield CommandReceived.decode(pb_request)
                # Stream ended -- loop back and re-subscribe unless cancelled.
                if token.is_cancelled:
                    return
                _logger.warning(
                    "Commands fast-subscribe stream ended, reconnecting (attempt %d)",
                    attempt + 1,
                )
                delay = backoff.delay_seconds(attempt)
                attempt += 1
                await asyncio.sleep(delay)
                continue
            except KubeMQClientClosedError:
                raise
            except KubeMQError as e:
                if token.is_cancelled:
                    return
                if not e.is_retryable and not isinstance(e, KubeMQConnectionError):
                    raise
                _logger.warning(
                    "Commands fast-subscribe stream broken (attempt %d): %s",
                    attempt + 1,
                    e.message,
                )
                delay = backoff.delay_seconds(attempt)
                attempt += 1
                await asyncio.sleep(delay)
            except Exception as e:
                if token.is_cancelled:
                    return
                _logger.warning(
                    "Commands fast-subscribe error (attempt %d): %s",
                    attempt + 1,
                    e,
                )
                delay = backoff.delay_seconds(attempt)
                attempt += 1
                await asyncio.sleep(delay)

    async def subscribe_to_queries_fast(
        self,
        subscription: QueriesSubscription,
        cancellation_token: AsyncCancellationToken | None = None,
    ) -> AsyncIterator[QueryReceived]:
        """Subscribe to queries -- fast path with automatic reconnection."""
        token = cancellation_token or AsyncCancellationToken()
        backoff = BackoffCalculator(self._config.retry_policy)
        attempt = 0

        while not token.is_cancelled:
            try:
                self._ensure_connected()
                assert self._transport is not None
                request = subscription.encode(self._config.client_id or "")
                async for pb_request in self._transport.subscribe_to_requests(request, token):
                    attempt = 0
                    yield QueryReceived.decode(pb_request)
                # Stream ended -- loop back and re-subscribe unless cancelled.
                if token.is_cancelled:
                    return
                _logger.warning(
                    "Queries fast-subscribe stream ended, reconnecting (attempt %d)",
                    attempt + 1,
                )
                delay = backoff.delay_seconds(attempt)
                attempt += 1
                await asyncio.sleep(delay)
                continue
            except KubeMQClientClosedError:
                raise
            except KubeMQError as e:
                if token.is_cancelled:
                    return
                if not e.is_retryable and not isinstance(e, KubeMQConnectionError):
                    raise
                _logger.warning(
                    "Queries fast-subscribe stream broken (attempt %d): %s",
                    attempt + 1,
                    e.message,
                )
                delay = backoff.delay_seconds(attempt)
                attempt += 1
                await asyncio.sleep(delay)
            except Exception as e:
                if token.is_cancelled:
                    return
                _logger.warning(
                    "Queries fast-subscribe error (attempt %d): %s",
                    attempt + 1,
                    e,
                )
                delay = backoff.delay_seconds(attempt)
                attempt += 1
                await asyncio.sleep(delay)

    async def subscribe_to_commands(
        self,
        subscription: CommandsSubscription,
        cancellation_token: AsyncCancellationToken | None = None,
    ) -> AsyncIterator[CommandReceived]:
        """Subscribe to commands.

        Can be used as async iterator. Streams command requests from the
        server; for each received command, call :meth:`send_response` to
        deliver a response back to the sender.

        Args:
            subscription: Subscription configuration including channel,
                optional group, and callback functions.
            cancellation_token: Optional token to cancel subscription.

        Yields:
            CommandReceived: For each command received.

        Raises:
            KubeMQValidationError: If the subscription configuration is
                invalid (e.g., empty channel or missing callbacks).
            KubeMQConnectionError: If the server is unreachable or the
                initial connection fails.
            KubeMQAuthenticationError: If the auth token is invalid or the
                client lacks permission for the target channel.
            KubeMQClientClosedError: If the client has already been closed.

        Cancellation:
            Pass an ``AsyncCancellationToken`` to cancel the subscription.
            When cancelled, the async iterator terminates.

        See Also:
            :class:`~kubemq.cq.commands_subscription.CommandsSubscription`:
                Subscription configuration for commands.
            :meth:`CQClient.subscribe_to_commands`: Sync counterpart.
            :meth:`send_command`: Send a command to subscribers.
            :meth:`send_response`: Send a response to a command.

        Example:
            token = AsyncCancellationToken()
            async for command in client.subscribe_to_commands(sub, token):
                # Process command and send response
                await client.send_response(
                    CommandResponse(
                        command_received=command,
                        is_executed=True,
                    )
                )
                if should_stop:
                    token.cancel()
        """
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

        except asyncio.CancelledError:
            _logger.debug("Command subscription cancelled")
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
        """Subscribe to queries.

        Can be used as async iterator. Streams query requests from the
        server; for each received query, call :meth:`send_response` to
        deliver a data response back to the sender.

        Args:
            subscription: Subscription configuration including channel,
                optional group, and callback functions.
            cancellation_token: Optional token to cancel subscription.

        Yields:
            QueryReceived: For each query received.

        Raises:
            KubeMQValidationError: If the subscription configuration is
                invalid (e.g., empty channel or missing callbacks).
            KubeMQConnectionError: If the server is unreachable or the
                initial connection fails.
            KubeMQAuthenticationError: If the auth token is invalid or the
                client lacks permission for the target channel.
            KubeMQClientClosedError: If the client has already been closed.

        Cancellation:
            Pass an ``AsyncCancellationToken`` to cancel the subscription.
            When cancelled, the async iterator terminates.

        See Also:
            :class:`~kubemq.cq.queries_subscription.QueriesSubscription`:
                Subscription configuration for queries.
            :meth:`CQClient.subscribe_to_queries`: Sync counterpart.
            :meth:`send_query`: Send a query to subscribers.
            :meth:`send_response`: Send a response to a query.

        Example:
            token = AsyncCancellationToken()
            async for query in client.subscribe_to_queries(sub, token):
                # Process query and send response
                await client.send_response(
                    QueryResponse(
                        query_received=query,
                        is_executed=True,
                        body=b"result data",
                    )
                )
        """
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

        except asyncio.CancelledError:
            _logger.debug("Query subscription cancelled")
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

    async def subscribe_commands_with_callback(  # noqa: C901
        self,
        subscription: CommandsSubscription,
        callback: AsyncCommandCallback,
        error_callback: AsyncErrorCallback | None = None,
        cancellation_token: AsyncCancellationToken | None = None,
        *,
        max_concurrent_callbacks: int = 1,
    ) -> None:
        """Subscribe to commands with an async callback.

        This method runs until cancelled and doesn't yield commands.
        By default, callbacks are invoked sequentially. Set
        ``max_concurrent_callbacks`` to allow concurrent processing.

        Args:
            subscription: Subscription configuration including channel,
                optional group, and callback functions.
            callback: Async callback for each command.
            error_callback: Optional async callback for errors.
            cancellation_token: Optional token to cancel subscription.
            max_concurrent_callbacks: Maximum number of callbacks that may
                execute concurrently. Default ``1`` (sequential). Must be
                >= 1 and <= 1000.

        Raises:
            ValueError: If ``max_concurrent_callbacks`` < 1 or > 1000.
            KubeMQValidationError: If the subscription configuration is
                invalid.
            KubeMQConnectionError: If the server is unreachable or the
                initial connection fails.
            KubeMQAuthenticationError: If the auth token is invalid or the
                client lacks permission for the target channel.
            KubeMQClientClosedError: If the client has already been closed.

        Cancellation:
            Pass an ``AsyncCancellationToken`` to cancel the subscription.
            When cancelled, in-flight callbacks are awaited before return.

        See Also:
            :meth:`subscribe_to_commands`: Iterator-based subscription.
            :meth:`subscribe_queries_with_callback`: Subscribe to queries
                with a callback.
        """
        if max_concurrent_callbacks < 1:
            raise ValueError("max_concurrent_callbacks must be >= 1")
        if max_concurrent_callbacks > 1000:
            raise ValueError(
                "max_concurrent_callbacks must be <= 1000 (prevents accidental resource exhaustion)"
            )

        self._ensure_connected()
        assert self._transport is not None

        token = cancellation_token or AsyncCancellationToken()
        await self._register_subscription(token)

        current_task = asyncio.current_task()
        if current_task is not None:
            self._register_subscription_task(current_task)

        pending_tasks: set[asyncio.Task] = set()  # type: ignore[type-arg]

        try:
            request = subscription.encode(self._config.client_id or "")

            if max_concurrent_callbacks == 1:
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
                            await callback(command)
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

                async def _run_cmd_callback(cmd: CommandReceived) -> None:
                    try:
                        await callback(cmd)
                    except Exception as cb_err:
                        if error_callback:
                            with contextlib.suppress(Exception):
                                await error_callback(cb_err)
                        elif self._logger:
                            self._logger.error(
                                "Unhandled callback exception: %s (%s)",
                                cb_err,
                                type(cb_err).__name__,
                            )
                    finally:
                        sem.release()

                async for pb_request in self._transport.subscribe_to_requests(request, token):
                    command = CommandReceived.decode(pb_request)
                    self._instrumentor._metrics.record_consumed_message(
                        "process", subscription.channel
                    )
                    await sem.acquire()
                    task = asyncio.create_task(_run_cmd_callback(command))
                    pending_tasks.add(task)
                    task.add_done_callback(pending_tasks.discard)

        except Exception as e:
            if error_callback:
                await error_callback(e)
            else:
                raise
        finally:
            if pending_tasks:
                await asyncio.gather(*pending_tasks, return_exceptions=True)
            await self._unregister_subscription(token)

    async def subscribe_queries_with_callback(  # noqa: C901
        self,
        subscription: QueriesSubscription,
        callback: AsyncQueryCallback,
        error_callback: AsyncErrorCallback | None = None,
        cancellation_token: AsyncCancellationToken | None = None,
        *,
        max_concurrent_callbacks: int = 1,
    ) -> None:
        """Subscribe to queries with an async callback.

        This method runs until cancelled and doesn't yield queries.
        By default, callbacks are invoked sequentially. Set
        ``max_concurrent_callbacks`` to allow concurrent processing.

        Args:
            subscription: Subscription configuration including channel,
                optional group, and callback functions.
            callback: Async callback for each query.
            error_callback: Optional async callback for errors.
            cancellation_token: Optional token to cancel subscription.
            max_concurrent_callbacks: Maximum number of callbacks that may
                execute concurrently. Default ``1`` (sequential). Must be
                >= 1 and <= 1000.

        Raises:
            ValueError: If ``max_concurrent_callbacks`` < 1 or > 1000.
            KubeMQValidationError: If the subscription configuration is
                invalid.
            KubeMQConnectionError: If the server is unreachable or the
                initial connection fails.
            KubeMQAuthenticationError: If the auth token is invalid or the
                client lacks permission for the target channel.
            KubeMQClientClosedError: If the client has already been closed.

        Cancellation:
            Pass an ``AsyncCancellationToken`` to cancel the subscription.
            When cancelled, in-flight callbacks are awaited before return.

        See Also:
            :meth:`subscribe_to_queries`: Iterator-based subscription.
            :meth:`subscribe_commands_with_callback`: Subscribe to commands
                with a callback.
        """
        if max_concurrent_callbacks < 1:
            raise ValueError("max_concurrent_callbacks must be >= 1")
        if max_concurrent_callbacks > 1000:
            raise ValueError(
                "max_concurrent_callbacks must be <= 1000 (prevents accidental resource exhaustion)"
            )

        self._ensure_connected()
        assert self._transport is not None

        token = cancellation_token or AsyncCancellationToken()
        await self._register_subscription(token)

        current_task = asyncio.current_task()
        if current_task is not None:
            self._register_subscription_task(current_task)

        pending_tasks: set[asyncio.Task] = set()  # type: ignore[type-arg]

        try:
            request = subscription.encode(self._config.client_id or "")

            if max_concurrent_callbacks == 1:
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
                            await callback(query)
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

                async def _run_query_callback(qry: QueryReceived) -> None:
                    try:
                        await callback(qry)
                    except Exception as cb_err:
                        if error_callback:
                            with contextlib.suppress(Exception):
                                await error_callback(cb_err)
                        elif self._logger:
                            self._logger.error(
                                "Unhandled callback exception: %s (%s)",
                                cb_err,
                                type(cb_err).__name__,
                            )
                    finally:
                        sem.release()

                async for pb_request in self._transport.subscribe_to_requests(request, token):
                    query = QueryReceived.decode(pb_request)
                    self._instrumentor._metrics.record_consumed_message(
                        "process", subscription.channel
                    )
                    await sem.acquire()
                    task = asyncio.create_task(_run_query_callback(query))
                    pending_tasks.add(task)
                    task.add_done_callback(pending_tasks.discard)

        except Exception as e:
            if error_callback:
                await error_callback(e)
            else:
                raise
        finally:
            if pending_tasks:
                await asyncio.gather(*pending_tasks, return_exceptions=True)
            await self._unregister_subscription(token)


# Alias for backward compatibility and clearer naming
AsyncCQClient = AsyncClient
