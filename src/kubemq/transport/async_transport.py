"""Native async transport layer using grpc.aio.

This module provides the AsyncTransport class for native async gRPC operations.
All operations are non-blocking and integrate with asyncio event loop.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

import grpc
import grpc.aio

from kubemq._internal.auth import TokenHolder, TokenManager
from kubemq._internal.compat import check_server_compatibility
from kubemq.grpc import kubemq_pb2 as pb, kubemq_pb2_grpc

from ..core.config import ClientConfig
from ..core.exceptions import (
    KubeMQAuthenticationError,
    KubeMQClientClosedError,
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
        self._draining = False
        self._logger = logging.getLogger(f"kubemq.{self.__class__.__name__}")
        self._token_holder = TokenHolder(config.auth_token)
        self._token_manager: Optional[TokenManager] = None

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

        # Initialize TokenManager if credential provider exists
        if self._config.credential_provider and self._token_manager is None:
            self._token_manager = TokenManager(
                provider=self._config.credential_provider,
                token_holder=self._token_holder,
                credential_timeout=self._config.credential_timeout,
            )
            await self._token_manager.get_token()

        # Build interceptors for all call types
        interceptors = self._build_interceptors()

        # Build channel options
        options = self._build_channel_options()

        # Create channel
        tls_effective = self._config._resolve_tls_enabled()

        if tls_effective:
            if self._config.tls.insecure_skip_verify:
                self._logger.warning(
                    "certificate verification is disabled — "
                    "this is insecure and should not be used in production"
                )
            credentials = self._build_ssl_credentials()
            self._channel = grpc.aio.secure_channel(
                self._config.address,
                credentials,
                options=options,
                interceptors=interceptors,
            )
        else:
            self._logger.warning(
                "Using insecure connection to %s — TLS is disabled. "
                "Set tls=TLSConfig(enabled=True, ...) for encrypted communication.",
                self._config.address,
            )
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
                server_info = await asyncio.wait_for(self.ping(), timeout=5.0)
                check_server_compatibility(server_info.version, self._logger)
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
        """Build all required interceptors.

        All interceptors share the same TokenHolder so token rotation
        via set_token() takes effect on the next gRPC call.
        """
        return [
            AsyncUnaryUnaryAuthInterceptor(self._token_holder),
            AsyncUnaryStreamAuthInterceptor(self._token_holder),
            AsyncStreamUnaryAuthInterceptor(self._token_holder),
            AsyncStreamStreamAuthInterceptor(self._token_holder),
        ]

    def _build_channel_options(self) -> list[tuple[str, Any]]:
        """Build gRPC channel options."""
        options: list[tuple[str, Any]] = [
            ("grpc.max_send_message_length", self._config.max_send_size),
            ("grpc.max_receive_message_length", self._config.max_receive_size),
        ]

        if self._config.tls.server_name_override:
            options.append(
                ("grpc.ssl_target_name_override", self._config.tls.server_name_override)
            )

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

    def _build_ssl_credentials(self) -> grpc.ChannelCredentials:
        """Build SSL credentials from TLSConfig, supporting both file paths and PEM bytes.

        PEM bytes fields take precedence over file paths (mutual exclusivity
        is enforced by TLSConfig validation). For file-based credentials,
        files are re-read on each call to support cert rotation on reconnect.
        """
        tls = self._config.tls

        root_certs = tls.ca_pem
        if root_certs is None and tls.ca_file:
            root_certs = Path(tls.ca_file).read_bytes()

        private_key = tls.key_pem
        if private_key is None and tls.key_file:
            private_key = Path(tls.key_file).read_bytes()

        certificate_chain = tls.cert_pem
        if certificate_chain is None and tls.cert_file:
            certificate_chain = Path(tls.cert_file).read_bytes()

        return grpc.ssl_channel_credentials(
            root_certificates=root_certs,
            private_key=private_key,
            certificate_chain=certificate_chain,
        )

    def set_token(self, token: str) -> None:
        """Update the auth token without reconnecting.

        The new token takes effect on the next gRPC call.
        Thread-safe: can be called from any thread.
        """
        self._token_holder.token = token

    async def _handle_unauthenticated(self, error: KubeMQAuthenticationError) -> bool:
        """Handle UNAUTHENTICATED by refreshing credentials.

        Returns True if refresh succeeded and the caller should retry.
        """
        if self._token_manager is None:
            return False
        try:
            await self._token_manager.invalidate()
            await self._token_manager.get_token()
            return True
        except Exception:
            return False

    async def _create_channel_for_reconnect(self) -> grpc.aio.Channel:
        """Create a new gRPC channel with fresh TLS credentials.

        Called by ReconnectionManager on each reconnection attempt.
        Re-reads certificate files from disk to support cert-manager rotation.
        Refreshes auth token via TokenManager if configured.
        """
        if self._token_manager:
            await self._token_manager.get_token()

        interceptors = self._build_interceptors()
        options = self._build_channel_options()

        tls_effective = self._config._resolve_tls_enabled()

        if tls_effective:
            if self._config.tls.insecure_skip_verify:
                self._logger.warning(
                    "certificate verification is disabled — "
                    "this is insecure and should not be used in production"
                )

            try:
                self._logger.debug(
                    "Reloading TLS credentials from source cert_source=%s",
                    "file" if self._config.tls.cert_file else "pem_bytes",
                )
                credentials = self._build_ssl_credentials()
                self._logger.debug("TLS credentials reloaded successfully")
            except FileNotFoundError as e:
                self._logger.error("Certificate file not found during reconnection: %s", e)
                raise KubeMQConnectionError(
                    f"Certificate file not found during reconnection: {e}",
                    cause=e,
                    is_retryable=True,
                ) from e
            except PermissionError as e:
                self._logger.error("Certificate file not readable during reconnection: %s", e)
                raise KubeMQConnectionError(
                    f"Certificate file permission denied during reconnection: {e}",
                    cause=e,
                    is_retryable=True,
                ) from e
            except OSError as e:
                self._logger.error("Certificate file I/O error during reconnection: %s", e)
                raise KubeMQConnectionError(
                    f"Certificate file I/O error during reconnection: {e}",
                    cause=e,
                    is_retryable=True,
                ) from e

            channel = grpc.aio.secure_channel(
                self._config.address,
                credentials,
                options=options,
                interceptors=interceptors,
            )
        else:
            channel = grpc.aio.insecure_channel(
                self._config.address,
                options=options,
                interceptors=interceptors,
            )

        return channel

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

    async def close(self, *, drain_timeout: float | None = None) -> None:
        """Close connection with optional drain of in-flight operations.

        Args:
            drain_timeout: Maximum seconds to wait for in-flight operations
                          to complete before forcing close. Uses config default
                          if not specified.

        Behaviour:
        1. Set _draining flag — new operations raise KubeMQClientClosedError
        2. Wait for active streams to complete (up to drain_timeout)
        3. Force-cancel remaining streams
        4. Close gRPC channel
        """
        if self._closing:
            return

        effective_timeout = (
            drain_timeout
            if drain_timeout is not None
            else self._config.drain_timeout
        )

        self._closing = True
        self._draining = True
        self._logger.debug(
            "Starting graceful shutdown drain_timeout=%.1fs",
            effective_timeout,
        )

        # Wait for active streams, then force-cancel remaining
        async with self._streams_lock:
            if self._active_streams:
                pending = [
                    self._wait_for_stream(call)
                    for call in self._active_streams
                ]
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*pending, return_exceptions=True),
                        timeout=effective_timeout,
                    )
                except asyncio.TimeoutError:
                    self._logger.debug(
                        "Drain timeout reached, force-cancelling streams"
                    )
                    for call in self._active_streams:
                        call.cancel()
            self._active_streams.clear()

        if self._token_manager:
            await self._token_manager.close()

        await self._cleanup_channel()

        self._connected = False
        self._closing = False
        self._draining = False
        self._logger.debug("Shutdown complete")

    async def _wait_for_stream(self, call: grpc.aio.Call) -> None:
        """Wait for a single gRPC stream call to complete."""
        try:
            await call
        except Exception:
            pass

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
        """Ensure transport is connected and not draining/closing."""
        if self._closing or self._draining:
            raise KubeMQClientClosedError("Transport is closing")
        if not self._connected:
            raise KubeMQConnectionError("Transport is not connected")

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
