"""Native async Commands and Queries client for KubeMQ."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Awaitable
from typing import (
    TYPE_CHECKING,
    Callable,
)

from pydantic import ValidationError

from kubemq._internal.telemetry import KubeMQTagsCarrier, create_link_from_context, error_code_to_error_type
from kubemq.common.async_cancellation_token import AsyncCancellationToken
from kubemq.core.client import NativeAsyncBaseClient
from kubemq.core.config import ClientConfig
from kubemq.core.exceptions import KubeMQValidationError
from kubemq.cq.command_message import CommandMessage
from kubemq.cq.command_message_received import CommandMessageReceived
from kubemq.cq.command_response_message import CommandResponseMessage
from kubemq.cq.commands_subscription import CommandsSubscription
from kubemq.cq.queries_subscription import QueriesSubscription
from kubemq.cq.query_message import QueryMessage
from kubemq.cq.query_message_received import QueryMessageReceived
from kubemq.cq.query_response_message import QueryResponseMessage

if TYPE_CHECKING:
    pass

# Type aliases for callbacks
AsyncCommandCallback = Callable[[CommandMessageReceived], Awaitable[None]]
AsyncQueryCallback = Callable[[QueryMessageReceived], Awaitable[None]]
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
                    CommandResponseMessage(
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
        **kwargs,
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
    ) -> CommandResponseMessage:
        """Send a command request and wait for response.

        Args:
            message: The command message to send

        Returns:
            CommandResponseMessage with the response
        """
        self._validate_message_size(message.body)
        start = time.perf_counter()
        error_type_val = None
        with self._instrumentor.start_span("send", message.channel) as span:
            try:
                self._ensure_connected()
                assert self._transport is not None
                pb_request = message.encode(self._config.client_id or "")
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
                return CommandResponseMessage.decode(response)
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

    async def send_commands_batch(
        self,
        messages: list[CommandMessage],
        max_concurrent: int = 100,
    ) -> list[CommandResponseMessage]:
        """Send multiple commands concurrently with backpressure control.

        Args:
            messages: List of command messages to send
            max_concurrent: Maximum concurrent sends (default 100)

        Returns:
            List of responses for each command
        """
        self._ensure_connected()

        semaphore = asyncio.Semaphore(max_concurrent)

        async def bounded_send(
            msg: CommandMessage, index: int
        ) -> tuple[int, CommandResponseMessage]:
            async with semaphore:
                try:
                    response = await self.send_command(msg)
                    return (index, response)
                except Exception as e:
                    return (
                        index,
                        CommandResponseMessage(
                            command_received=CommandMessageReceived(
                                id=msg.id or "",
                                channel=msg.channel,
                            ),
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
    ) -> QueryResponseMessage:
        """Send a query request and wait for response.

        Args:
            message: The query message to send

        Returns:
            QueryResponseMessage with the response
        """
        self._validate_message_size(message.body)
        start = time.perf_counter()
        error_type_val = None
        with self._instrumentor.start_span("send", message.channel) as span:
            try:
                self._ensure_connected()
                assert self._transport is not None
                pb_request = message.encode(self._config.client_id or "")
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
                return QueryResponseMessage.decode(response)
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

    async def send_queries_batch(
        self,
        messages: list[QueryMessage],
        max_concurrent: int = 100,
    ) -> list[QueryResponseMessage]:
        """Send multiple queries concurrently with backpressure control.

        Args:
            messages: List of query messages to send
            max_concurrent: Maximum concurrent sends (default 100)

        Returns:
            List of responses for each query
        """
        self._ensure_connected()

        semaphore = asyncio.Semaphore(max_concurrent)

        async def bounded_send(msg: QueryMessage, index: int) -> tuple[int, QueryResponseMessage]:
            async with semaphore:
                try:
                    response = await self.send_query(msg)
                    return (index, response)
                except Exception as e:
                    return (
                        index,
                        QueryResponseMessage(
                            query_received=QueryMessageReceived(
                                id=msg.id or "",
                                channel=msg.channel,
                            ),
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
        response: CommandResponseMessage | QueryResponseMessage,
    ) -> None:
        """Send a response to a command or query request.

        Args:
            response: The response message to send
        """
        self._ensure_connected()
        assert self._transport is not None
        channel = getattr(response, "reply_channel", "") or ""
        start = time.perf_counter()
        error_type_val = None
        with self._instrumentor.start_span("settle", channel) as span:
            try:
                pb_response = response.encode(self._config.client_id or "")
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
    # Subscription Operations
    # =========================================================================

    async def subscribe_to_commands(
        self,
        subscription: CommandsSubscription,
        cancellation_token: AsyncCancellationToken | None = None,
    ) -> AsyncIterator[CommandMessageReceived]:
        """Subscribe to commands.

        Can be used as async iterator.

        Args:
            subscription: Subscription configuration
            cancellation_token: Optional token to cancel subscription

        Yields:
            CommandMessageReceived for each command

        Cancellation:
            Pass an ``AsyncCancellationToken`` to cancel the subscription.
            When cancelled, the async iterator terminates.

        Example:
            token = AsyncCancellationToken()
            async for command in client.subscribe_to_commands(sub, token):
                # Process command and send response
                await client.send_response(
                    CommandResponseMessage(
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
                        command = CommandMessageReceived.decode(pb_request)

                        if subscription.on_receive_command_callback:
                            if asyncio.iscoroutinefunction(subscription.on_receive_command_callback):
                                await subscription.on_receive_command_callback(command)
                            else:
                                subscription.on_receive_command_callback(command)

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
    ) -> AsyncIterator[QueryMessageReceived]:
        """Subscribe to queries.

        Can be used as async iterator.

        Args:
            subscription: Subscription configuration
            cancellation_token: Optional token to cancel subscription

        Yields:
            QueryMessageReceived for each query

        Cancellation:
            Pass an ``AsyncCancellationToken`` to cancel the subscription.
            When cancelled, the async iterator terminates.

        Example:
            token = AsyncCancellationToken()
            async for query in client.subscribe_to_queries(sub, token):
                # Process query and send response
                await client.send_response(
                    QueryResponseMessage(
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
                        query = QueryMessageReceived.decode(pb_request)

                        if subscription.on_receive_query_callback:
                            if asyncio.iscoroutinefunction(subscription.on_receive_query_callback):
                                await subscription.on_receive_query_callback(query)
                            else:
                                subscription.on_receive_query_callback(query)

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

    async def subscribe_commands_with_callback(
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
            subscription: Subscription configuration
            callback: Async callback for each command
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
                            command = CommandMessageReceived.decode(pb_request)
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

                async def _run_cmd_callback(cmd: CommandMessageReceived) -> None:
                    try:
                        await callback(cmd)
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

                async for pb_request in self._transport.subscribe_to_requests(request, token):
                    command = CommandMessageReceived.decode(pb_request)
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

    async def subscribe_queries_with_callback(
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
            subscription: Subscription configuration
            callback: Async callback for each query
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
                            query = QueryMessageReceived.decode(pb_request)
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

                async def _run_query_callback(qry: QueryMessageReceived) -> None:
                    try:
                        await callback(qry)
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

                async for pb_request in self._transport.subscribe_to_requests(request, token):
                    query = QueryMessageReceived.decode(pb_request)
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
