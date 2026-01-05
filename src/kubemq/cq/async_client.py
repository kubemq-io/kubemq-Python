"""Native async Commands and Queries client for KubeMQ."""

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
        self._ensure_connected()
        assert self._transport is not None
        pb_request = message.encode(self._config.client_id or "")
        response = await self._transport.send_request(
            pb_request,
            timeout_seconds=message.timeout_in_seconds,
        )
        return CommandResponseMessage.decode(response)

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
        self._ensure_connected()
        assert self._transport is not None
        pb_request = message.encode(self._config.client_id or "")
        response = await self._transport.send_request(
            pb_request,
            timeout_seconds=message.timeout_in_seconds,
        )
        return QueryResponseMessage.decode(response)

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
        pb_response = response.encode(self._config.client_id or "")
        await self._transport.send_response(pb_response)

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
                command = CommandMessageReceived.decode(pb_request)

                # Call the subscription callback if provided
                if subscription.on_receive_command_callback:
                    if asyncio.iscoroutinefunction(subscription.on_receive_command_callback):
                        await subscription.on_receive_command_callback(command)
                    else:
                        subscription.on_receive_command_callback(command)

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
                query = QueryMessageReceived.decode(pb_request)

                # Call the subscription callback if provided
                if subscription.on_receive_query_callback:
                    if asyncio.iscoroutinefunction(subscription.on_receive_query_callback):
                        await subscription.on_receive_query_callback(query)
                    else:
                        subscription.on_receive_query_callback(query)

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
    ) -> None:
        """Subscribe to commands with an async callback.

        This method runs until cancelled and doesn't yield commands.

        Args:
            subscription: Subscription configuration
            callback: Async callback for each command
            error_callback: Optional async callback for errors
            cancellation_token: Optional token to cancel subscription
        """
        self._ensure_connected()
        assert self._transport is not None

        token = cancellation_token or AsyncCancellationToken()
        await self._register_subscription(token)

        try:
            request = subscription.encode(self._config.client_id or "")

            async for pb_request in self._transport.subscribe_to_requests(request, token):
                command = CommandMessageReceived.decode(pb_request)
                await callback(command)

        except Exception as e:
            if error_callback:
                await error_callback(e)
            else:
                raise
        finally:
            await self._unregister_subscription(token)

    async def subscribe_queries_with_callback(
        self,
        subscription: QueriesSubscription,
        callback: AsyncQueryCallback,
        error_callback: AsyncErrorCallback | None = None,
        cancellation_token: AsyncCancellationToken | None = None,
    ) -> None:
        """Subscribe to queries with an async callback.

        This method runs until cancelled and doesn't yield queries.

        Args:
            subscription: Subscription configuration
            callback: Async callback for each query
            error_callback: Optional async callback for errors
            cancellation_token: Optional token to cancel subscription
        """
        self._ensure_connected()
        assert self._transport is not None

        token = cancellation_token or AsyncCancellationToken()
        await self._register_subscription(token)

        try:
            request = subscription.encode(self._config.client_id or "")

            async for pb_request in self._transport.subscribe_to_requests(request, token):
                query = QueryMessageReceived.decode(pb_request)
                await callback(query)

        except Exception as e:
            if error_callback:
                await error_callback(e)
            else:
                raise
        finally:
            await self._unregister_subscription(token)


# Alias for backward compatibility and clearer naming
AsyncCQClient = AsyncClient
