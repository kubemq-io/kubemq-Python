"""Native async transport layer using grpc.aio.

This module provides the AsyncTransport class for native async gRPC operations.
All operations are non-blocking and integrate with asyncio event loop.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

import grpc
import grpc.aio

from kubemq.grpc import kubemq_pb2 as pb, kubemq_pb2_grpc

from ..core.config import ClientConfig
from ..core.exceptions import (
    KubeMQConnectionError,
    KubeMQTimeoutError,
    from_grpc_error,
)
from .interceptors import (
    AsyncStreamStreamAuthInterceptor,
    AsyncStreamUnaryAuthInterceptor,
    AsyncUnaryStreamAuthInterceptor,
    AsyncUnaryUnaryAuthInterceptor,
)
from .server_info import ServerInfo

if TYPE_CHECKING:
    from ..common.async_cancellation_token import AsyncCancellationToken


class AsyncTransport:
    """Native async transport layer using grpc.aio.

    This transport uses native gRPC async operations without thread pools.
    All operations are non-blocking and integrate with asyncio event loop.

    Example:
        config = ClientConfig(address="localhost:50000")
        transport = AsyncTransport(config)
        await transport.connect()

        try:
            server_info = await transport.ping()
            print(f"Connected to {server_info.host}")
        finally:
            await transport.close()

    Attributes:
        is_connected: Whether the transport is connected.
    """

    def __init__(self, config: ClientConfig) -> None:
        """Initialize the async transport.

        Args:
            config: Client configuration containing address, auth, TLS settings, etc.
        """
        self._config = config
        self._channel: grpc.aio.Channel | None = None
        self._stub: kubemq_pb2_grpc.kubemqStub | None = None
        self._connected = False
        self._closing = False
        self._logger = logging.getLogger(f"kubemq.{self.__class__.__name__}")

        # Track active streams for graceful shutdown
        self._active_streams: set[grpc.aio.Call] = set()
        self._streams_lock = asyncio.Lock()

    async def connect(self, verify: bool = True) -> None:
        """Establish async gRPC connection.

        Args:
            verify: If True, verify connection with ping (default True).
                   Set to False for lazy connection.

        Raises:
            KubeMQConnectionError: If connection fails.
        """
        if self._connected:
            return

        # Build interceptors for all call types
        interceptors = self._build_interceptors()

        # Build channel options
        options = self._build_channel_options()

        # Create channel
        if self._config.tls.enabled:
            credentials = self._get_ssl_credentials()
            self._channel = grpc.aio.secure_channel(
                self._config.address,
                credentials,
                options=options,
                interceptors=interceptors,
            )
        else:
            self._channel = grpc.aio.insecure_channel(
                self._config.address,
                options=options,
                interceptors=interceptors,
            )

        self._stub = kubemq_pb2_grpc.kubemqStub(self._channel)

        # Set connected before verification so ping() can work
        self._connected = True

        # Verify connection
        if verify:
            try:
                await asyncio.wait_for(self.ping(), timeout=5.0)
            except asyncio.TimeoutError:
                self._logger.warning(
                    "Connection verification timed out, channel created but unverified"
                )
                # Channel exists, just unverified - keep connected
            except Exception as e:
                self._connected = False
                await self._cleanup_channel()
                raise KubeMQConnectionError(
                    f"Failed to connect to {self._config.address}: {e}",
                    cause=e,
                ) from e

    def _build_interceptors(self) -> list[grpc.aio.ClientInterceptor]:
        """Build all required interceptors."""
        auth_token = self._config.auth_token
        return [
            AsyncUnaryUnaryAuthInterceptor(auth_token),
            AsyncUnaryStreamAuthInterceptor(auth_token),
            AsyncStreamUnaryAuthInterceptor(auth_token),
            AsyncStreamStreamAuthInterceptor(auth_token),
        ]

    def _build_channel_options(self) -> list[tuple]:
        """Build gRPC channel options."""
        options = [
            ("grpc.max_send_message_length", self._config.max_send_size),
            ("grpc.max_receive_message_length", self._config.max_receive_size),
        ]

        if self._config.keep_alive.enabled:
            options.extend(
                [
                    (
                        "grpc.keepalive_time_ms",
                        self._config.keep_alive.ping_interval_in_seconds * 1000,
                    ),
                    (
                        "grpc.keepalive_timeout_ms",
                        self._config.keep_alive.ping_timeout_in_seconds * 1000,
                    ),
                    (
                        "grpc.keepalive_permit_without_calls",
                        1 if self._config.keep_alive.permit_without_calls else 0,
                    ),
                ]
            )

        return options

    def _get_ssl_credentials(self) -> grpc.ChannelCredentials:
        """Get SSL credentials for TLS connection."""
        tls = self._config.tls

        root_certs = None
        if tls.ca_file:
            with open(tls.ca_file, "rb") as f:
                root_certs = f.read()

        private_key = None
        certificate_chain = None
        if tls.cert_file and tls.key_file:
            with open(tls.key_file, "rb") as f:
                private_key = f.read()
            with open(tls.cert_file, "rb") as f:
                certificate_chain = f.read()

        return grpc.ssl_channel_credentials(
            root_certificates=root_certs,
            private_key=private_key,
            certificate_chain=certificate_chain,
        )

    # =========================================================================
    # Basic Operations
    # =========================================================================

    async def ping(self) -> ServerInfo:
        """Ping server using native async.

        Returns:
            ServerInfo with server details.

        Raises:
            KubeMQConnectionError: If not connected or connection fails.
        """
        self._ensure_connected()
        try:
            response = await self._stub.Ping(pb.Empty())
            return ServerInfo(
                host=response.Host,
                version=response.Version,
                server_start_time=response.ServerStartTime,
                server_up_time_seconds=response.ServerUpTimeSeconds,
            )
        except grpc.aio.AioRpcError as e:
            raise from_grpc_error(e) from e

    async def close(self) -> None:
        """Close connection gracefully with active stream cleanup."""
        if not self._connected or self._closing:
            return

        self._closing = True
        self._logger.debug("Starting graceful shutdown")

        # Cancel all active streams
        async with self._streams_lock:
            for call in self._active_streams:
                call.cancel()
            self._active_streams.clear()

        # Close channel
        await self._cleanup_channel()

        self._connected = False
        self._closing = False
        self._logger.debug("Shutdown complete")

    async def _cleanup_channel(self) -> None:
        """Clean up the gRPC channel."""
        if self._channel:
            await self._channel.close()
            self._channel = None
            self._stub = None

    @property
    def is_connected(self) -> bool:
        """Check if transport is connected."""
        return self._connected and not self._closing

    def _ensure_connected(self) -> None:
        """Ensure transport is connected."""
        if not self._connected:
            raise KubeMQConnectionError("Transport is not connected")
        if self._closing:
            raise KubeMQConnectionError("Transport is closing")

    async def _register_stream(self, call: grpc.aio.Call) -> None:
        """Register an active stream for tracking."""
        async with self._streams_lock:
            self._active_streams.add(call)

    async def _unregister_stream(self, call: grpc.aio.Call) -> None:
        """Unregister a completed stream."""
        async with self._streams_lock:
            self._active_streams.discard(call)

    # =========================================================================
    # Event Send Operations
    # =========================================================================

    async def send_event(self, event: pb.Event) -> pb.Result:
        """Send event using native async.

        Fire-and-forget event sending (non-persistent).

        Args:
            event: The event protobuf to send.

        Returns:
            Result protobuf (usually empty for fire-and-forget).

        Raises:
            KubeMQConnectionError: If not connected.
            KubeMQTimeoutError: If send times out.
        """
        self._ensure_connected()
        try:
            result = await asyncio.wait_for(
                self._stub.SendEvent(event),
                timeout=self._config.default_timeout_seconds,
            )
            return result
        except asyncio.TimeoutError as e:
            raise KubeMQTimeoutError(
                f"Send event timed out after {self._config.default_timeout_seconds}s"
            ) from e
        except grpc.aio.AioRpcError as e:
            raise from_grpc_error(e) from e

    # =========================================================================
    # Request/Response Operations (Commands & Queries)
    # =========================================================================

    async def send_request(
        self,
        request: pb.Request,
        timeout_seconds: int | None = None,
    ) -> pb.Response:
        """Send command/query request and wait for response.

        Args:
            request: The command/query request.
            timeout_seconds: Timeout for response (defaults to config timeout).

        Returns:
            Response from the responder.

        Raises:
            KubeMQConnectionError: If not connected.
            KubeMQTimeoutError: If request times out.
        """
        self._ensure_connected()
        timeout = timeout_seconds or self._config.default_timeout_seconds

        try:
            response = await asyncio.wait_for(
                self._stub.SendRequest(request),
                timeout=timeout,
            )
            return response
        except asyncio.TimeoutError as e:
            raise KubeMQTimeoutError(f"Request timed out after {timeout}s") from e
        except grpc.aio.AioRpcError as e:
            raise from_grpc_error(e) from e

    async def send_response(self, response: pb.Response) -> None:
        """Send response to a command/query request.

        Args:
            response: The response protobuf.

        Raises:
            KubeMQConnectionError: If not connected.
            KubeMQTimeoutError: If send times out.
        """
        self._ensure_connected()
        try:
            await asyncio.wait_for(
                self._stub.SendResponse(response),
                timeout=self._config.default_timeout_seconds,
            )
        except asyncio.TimeoutError as e:
            raise KubeMQTimeoutError("Send response timed out") from e
        except grpc.aio.AioRpcError as e:
            raise from_grpc_error(e) from e

    # =========================================================================
    # Subscription Operations (Streaming)
    # =========================================================================

    async def subscribe_to_events(
        self,
        request: pb.Subscribe,
        cancellation_token: AsyncCancellationToken | None = None,
    ) -> AsyncIterator[pb.EventReceive]:
        """Subscribe to events using native async streaming.

        Args:
            request: The subscription request.
            cancellation_token: Optional token to cancel subscription.

        Yields:
            EventReceive protobuf messages.

        Raises:
            KubeMQConnectionError: On connection issues.
        """
        self._ensure_connected()

        call: grpc.aio.UnaryStreamCall = self._stub.SubscribeToEvents(request)
        await self._register_stream(call)

        try:
            async for event in call:
                # Check cancellation
                if cancellation_token and cancellation_token.is_cancelled:
                    self._logger.debug("Subscription cancelled by token")
                    call.cancel()
                    break

                # Check if transport is closing
                if self._closing:
                    self._logger.debug("Subscription cancelled due to transport closing")
                    break

                yield event

        except grpc.aio.AioRpcError as e:
            # CANCELLED is expected during shutdown/cancellation
            if e.code() != grpc.StatusCode.CANCELLED:
                raise from_grpc_error(e) from e
        finally:
            await self._unregister_stream(call)

    async def subscribe_to_requests(
        self,
        request: pb.Subscribe,
        cancellation_token: AsyncCancellationToken | None = None,
    ) -> AsyncIterator[pb.Request]:
        """Subscribe to commands/queries using native async streaming.

        Args:
            request: The subscription request.
            cancellation_token: Optional token to cancel subscription.

        Yields:
            Request protobuf messages (commands or queries).

        Raises:
            KubeMQConnectionError: On connection issues.
        """
        self._ensure_connected()

        call = self._stub.SubscribeToRequests(request)
        await self._register_stream(call)

        try:
            async for req in call:
                if cancellation_token and cancellation_token.is_cancelled:
                    call.cancel()
                    break
                if self._closing:
                    break
                yield req
        except grpc.aio.AioRpcError as e:
            if e.code() != grpc.StatusCode.CANCELLED:
                raise from_grpc_error(e) from e
        finally:
            await self._unregister_stream(call)

    # =========================================================================
    # Queue Operations
    # =========================================================================

    async def send_queue_message(
        self,
        message: pb.QueueMessage,
    ) -> pb.SendQueueMessageResult:
        """Send a single queue message.

        Args:
            message: The queue message to send.

        Returns:
            SendQueueMessageResult with send confirmation.

        Raises:
            KubeMQConnectionError: If not connected.
            KubeMQTimeoutError: If send times out.
        """
        self._ensure_connected()
        try:
            result = await asyncio.wait_for(
                self._stub.SendQueueMessage(message),
                timeout=self._config.default_timeout_seconds,
            )
            return result
        except asyncio.TimeoutError as e:
            raise KubeMQTimeoutError(
                f"Queue send timed out after {self._config.default_timeout_seconds}s"
            ) from e
        except grpc.aio.AioRpcError as e:
            raise from_grpc_error(e) from e

    async def send_queue_messages_batch(
        self,
        request: pb.QueueMessagesBatchRequest,
    ) -> pb.QueueMessagesBatchResponse:
        """Send multiple queue messages in a batch.

        Args:
            request: Batch request containing messages.

        Returns:
            QueueMessagesBatchResponse with results for each message.

        Raises:
            KubeMQConnectionError: If not connected.
            KubeMQTimeoutError: If send times out.
        """
        self._ensure_connected()
        try:
            result = await asyncio.wait_for(
                self._stub.SendQueueMessagesBatch(request),
                timeout=self._config.default_timeout_seconds,
            )
            return result
        except asyncio.TimeoutError as e:
            raise KubeMQTimeoutError(
                f"Queue batch send timed out after {self._config.default_timeout_seconds}s"
            ) from e
        except grpc.aio.AioRpcError as e:
            raise from_grpc_error(e) from e

    async def receive_queue_messages(
        self,
        request: pb.ReceiveQueueMessagesRequest,
    ) -> pb.ReceiveQueueMessagesResponse:
        """Receive queue messages (polling).

        Args:
            request: The receive request with channel and poll parameters.

        Returns:
            ReceiveQueueMessagesResponse with received messages.

        Raises:
            KubeMQConnectionError: If not connected.
            KubeMQTimeoutError: If receive times out.
        """
        self._ensure_connected()
        try:
            # Use WaitTimeSeconds from request, add buffer for network
            timeout = request.WaitTimeSeconds + 5
            result = await asyncio.wait_for(
                self._stub.ReceiveQueueMessages(request),
                timeout=timeout,
            )
            return result
        except asyncio.TimeoutError as e:
            raise KubeMQTimeoutError("Queue receive timed out") from e
        except grpc.aio.AioRpcError as e:
            raise from_grpc_error(e) from e

    async def ack_all_queue_messages(
        self,
        request: pb.AckAllQueueMessagesRequest,
    ) -> pb.AckAllQueueMessagesResponse:
        """Acknowledge all messages in a queue.

        Args:
            request: The ack all request.

        Returns:
            AckAllQueueMessagesResponse with number of affected messages.

        Raises:
            KubeMQConnectionError: If not connected.
            KubeMQTimeoutError: If operation times out.
        """
        self._ensure_connected()
        try:
            result = await asyncio.wait_for(
                self._stub.AckAllQueueMessages(request),
                timeout=self._config.default_timeout_seconds,
            )
            return result
        except asyncio.TimeoutError as e:
            raise KubeMQTimeoutError("Ack all timed out") from e
        except grpc.aio.AioRpcError as e:
            raise from_grpc_error(e) from e

    # =========================================================================
    # Queue Bidirectional Streaming
    # =========================================================================

    async def queues_upstream(
        self,
        request_iterator: AsyncIterator[pb.QueuesUpstreamRequest],
        cancellation_token: AsyncCancellationToken | None = None,
    ) -> AsyncIterator[pb.QueuesUpstreamResponse]:
        """Bidirectional streaming for queue message sending.

        Args:
            request_iterator: Async generator yielding requests to send.
            cancellation_token: Optional cancellation token.

        Yields:
            QueuesUpstreamResponse for each sent message.
        """
        self._ensure_connected()

        # Create bidirectional stream
        call: grpc.aio.StreamStreamCall = self._stub.QueuesUpstream(request_iterator)
        await self._register_stream(call)

        try:
            async for response in call:
                if cancellation_token and cancellation_token.is_cancelled:
                    call.cancel()
                    break
                if self._closing:
                    break
                yield response
        except grpc.aio.AioRpcError as e:
            if e.code() != grpc.StatusCode.CANCELLED:
                raise from_grpc_error(e) from e
        finally:
            await self._unregister_stream(call)

    async def queues_downstream(
        self,
        request_iterator: AsyncIterator[pb.QueuesDownstreamRequest],
        cancellation_token: AsyncCancellationToken | None = None,
    ) -> AsyncIterator[pb.QueuesDownstreamResponse]:
        """Bidirectional streaming for queue message receiving.

        Args:
            request_iterator: Async generator yielding poll/ack/reject requests.
            cancellation_token: Optional cancellation token.

        Yields:
            QueuesDownstreamResponse with received messages.
        """
        self._ensure_connected()

        call: grpc.aio.StreamStreamCall = self._stub.QueuesDownstream(request_iterator)
        await self._register_stream(call)

        try:
            async for response in call:
                if cancellation_token and cancellation_token.is_cancelled:
                    call.cancel()
                    break
                if self._closing:
                    break
                yield response
        except grpc.aio.AioRpcError as e:
            if e.code() != grpc.StatusCode.CANCELLED:
                raise from_grpc_error(e) from e
        finally:
            await self._unregister_stream(call)

    # =========================================================================
    # Queue Info Operations
    # =========================================================================

    async def queues_info(
        self,
        request: pb.QueuesInfoRequest,
    ) -> pb.QueuesInfoResponse:
        """Get queue information.

        Args:
            request: The queue info request.

        Returns:
            QueuesInfoResponse with queue details.

        Raises:
            KubeMQConnectionError: If not connected.
            KubeMQTimeoutError: If operation times out.
        """
        self._ensure_connected()
        try:
            result = await asyncio.wait_for(
                self._stub.QueuesInfo(request),
                timeout=self._config.default_timeout_seconds,
            )
            return result
        except asyncio.TimeoutError as e:
            raise KubeMQTimeoutError("Queue info request timed out") from e
        except grpc.aio.AioRpcError as e:
            raise from_grpc_error(e) from e

    # =========================================================================
    # Context Manager Support
    # =========================================================================

    async def __aenter__(self) -> AsyncTransport:
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: object | None,
    ) -> None:
        """Async context manager exit."""
        await self.close()
