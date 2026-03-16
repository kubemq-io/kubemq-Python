import asyncio
import logging
import threading
from collections.abc import Sequence

import grpc
from grpc import Channel
from grpc._cython.cygrpc import ChannelCredentials

import kubemq.grpc.kubemq_pb2_grpc as kubemq_pb2_grpc
from kubemq._internal.auth import TokenHolder
from kubemq._internal.compat import check_server_compatibility
from kubemq.grpc import Empty
from kubemq.transport.channel_manager import ChannelManager
from kubemq.transport.connection import Connection
from kubemq.transport.interceptors import AuthInterceptorsAsync
from kubemq.transport.keep_alive import KeepAliveConfig
from kubemq.transport.server_info import ServerInfo
from kubemq.transport.tls_config import TlsConfig


def _get_ssl_credentials(tls_config: TlsConfig) -> ChannelCredentials:
    certificate_chain = _read_file(tls_config.cert_file) if tls_config.cert_file else None
    private_key = _read_file(tls_config.key_file) if tls_config.key_file else None
    root_certificates = _read_file(tls_config.ca_file) if tls_config.ca_file else None
    return grpc.ssl_channel_credentials(root_certificates, private_key, certificate_chain)


def _read_file(file_path: str) -> bytes:
    with open(file_path, "rb") as f:
        return f.read()


def _get_call_options(connection: Connection) -> Sequence[tuple[str, int]]:
    options = [
        ("grpc.max_send_message_length", connection.max_send_size),
        ("grpc.max_receive_message_length", connection.max_receive_size),
    ]
    if (
        connection.keep_alive
        and isinstance(connection.keep_alive, KeepAliveConfig)
        and connection.keep_alive.enabled
    ):
        options.append(
            (
                "grpc.keepalive_time_ms",
                connection.keep_alive.ping_interval_in_seconds * 1000,
            )
        )
        options.append(
            (
                "grpc.keepalive_timeout_ms",
                connection.keep_alive.ping_timeout_in_seconds * 1000,
            )
        )
        options.append(("grpc.keepalive_permit_without_calls", 1))
        options.append(
            (
                "grpc.http2.min_time_between_pings_ms",
                connection.keep_alive.ping_interval_in_seconds * 1000,
            )
        )
        options.append(
            (
                "grpc.http2.min_ping_interval_without_data_ms",
                connection.keep_alive.ping_interval_in_seconds * 1000,
            )
        )
    return options


class SyncTransport:
    """Synchronous transport layer for KubeMQ.

    This is the sync/thread-based transport implementation.
    For native async operations, use AsyncTransport from async_transport module.

    Note: The alias `Transport` is provided for backward compatibility
    and will be deprecated in future versions.
    """

    def __init__(self, connection: Connection) -> None:
        self._opts: Connection = connection.complete()
        self._channel: Channel | None = None
        self._client: kubemq_pb2_grpc.kubemqStub | None = None
        self._async_channel: Channel | None = None
        self._async_client: kubemq_pb2_grpc.kubemqStub | None = None
        self._is_connected_lock = threading.Lock()
        self._is_connected: bool = False
        self._logger = logging.getLogger("KubeMQ")
        self._channel_manager: ChannelManager | None = None
        self._token_holder = TokenHolder(self._opts.auth_token or None)

    def initialize(self) -> "SyncTransport":
        """Initialize the transport by creating the gRPC channel and verifying connectivity."""
        try:
            if not self._opts.tls.enabled:
                self._logger.warning(
                    "Using insecure connection to %s — TLS is disabled. "
                    "Set tls=TLSConfig(enabled=True, ...) for encrypted communication.",
                    self._opts.address,
                )
            # Initialize the channel manager
            self._channel_manager = ChannelManager(
                self._opts, self._logger, token_holder=self._token_holder
            )
            self._client = self._channel_manager.get_client()
            with self._is_connected_lock:
                self._is_connected = True
            try:
                server_info = self.ping()
                check_server_compatibility(server_info.version, self._logger)
            except Exception:
                self._logger.debug("Could not verify server version during initialization")
        except Exception as ex:
            with self._is_connected_lock:
                self._is_connected = False
            raise ex
        return self

    def _initialize_async(self) -> None:
        auth_interceptor_async: AuthInterceptorsAsync = AuthInterceptorsAsync(self._token_holder)
        interceptors_async: Sequence[grpc.aio.ClientInterceptor] = [auth_interceptor_async]
        if self._opts.tls.enabled:
            try:
                credentials: ChannelCredentials = _get_ssl_credentials(self._opts.tls)
                self._async_channel = grpc.aio.secure_channel(
                    self._opts.address,
                    credentials,
                    options=_get_call_options(self._opts),
                    interceptors=interceptors_async,
                )
            except Exception as e:
                raise e
        else:
            self._async_channel = grpc.aio.insecure_channel(
                self._opts.address,
                options=_get_call_options(self._opts),
                interceptors=interceptors_async,
            )

        self._async_client = kubemq_pb2_grpc.kubemqStub(self._async_channel)  # type: ignore[no-untyped-call]

    def ping(self) -> ServerInfo:
        """Ping the server and return server information."""
        if self._client is None:
            raise RuntimeError("Transport not initialized - call initialize() first")
        response = self._client.Ping(Empty())
        return ServerInfo(
            host=response.Host,
            version=response.Version,
            server_start_time=response.ServerStartTime,
            server_up_time_seconds=response.ServerUpTimeSeconds,
        )

    def kubemq_client(self) -> kubemq_pb2_grpc.kubemqStub:
        """Get the current gRPC client stub."""
        if self._channel_manager:
            return self._channel_manager.get_client()
        if self._client is None:
            raise RuntimeError("Transport not initialized - call initialize() first")
        return self._client

    def kubemq_async_client(self) -> kubemq_pb2_grpc.kubemqStub:
        """Get the async gRPC client stub, initializing if needed."""
        if self._async_client is None:
            self._initialize_async()
        if self._async_client is None:
            raise RuntimeError("Failed to initialize async client")
        return self._async_client

    def is_connected(self) -> bool:
        """Check if the transport is connected to the server."""
        if self._channel_manager:
            return self._channel_manager.connection_state.is_accepting_requests()

        with self._is_connected_lock:
            return self._is_connected

    def recreate_channel(self) -> kubemq_pb2_grpc.kubemqStub:
        """Recreates the gRPC channel and client after a connection failure.

        Returns:
            kubemq_pb2_grpc.kubemqStub: New client instance
        """
        if self._channel_manager:
            return self._channel_manager.recreate_channel()

        # This should never be reached with the new architecture
        self._logger.error("Channel manager not initialized, cannot recreate channel")
        raise ConnectionError("Channel manager not initialized, cannot recreate channel")

    def set_token(self, token: str) -> None:
        """Update the auth token without reconnecting.

        The new token takes effect on the next gRPC call.
        Thread-safe: can be called from any thread.
        """
        self._token_holder.token = token

    async def close_async(self) -> None:
        """Close the transport asynchronously."""
        if self._channel_manager:
            self._channel_manager.close()
            with self._is_connected_lock:
                self._is_connected = False

        if self._async_channel is not None:
            await self._async_channel.close()
            self._async_channel = None
            self._async_client = None

    def close(self) -> None:
        """Close the transport synchronously."""
        if self._channel_manager:
            self._channel_manager.close()
            with self._is_connected_lock:
                self._is_connected = False

        if self._async_channel is not None:
            channel_to_close = self._async_channel
            try:
                # Check if we're running in an async context
                asyncio.get_running_loop()
                # In async context - just nullify and let garbage collection handle cleanup
                self._async_channel = None
                self._async_client = None
            except RuntimeError:
                loop = asyncio.new_event_loop()
                try:
                    loop.run_until_complete(channel_to_close.close())
                finally:
                    loop.close()
                self._async_channel = None
                self._async_client = None


# Backward compatibility alias
Transport = SyncTransport
