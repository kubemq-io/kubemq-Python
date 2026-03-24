from __future__ import annotations

import logging
import threading
import time
from typing import TYPE_CHECKING, Any

import grpc

import kubemq.grpc.kubemq_pb2_grpc as kubemq_pb2_grpc
from kubemq.grpc import Empty
from kubemq.transport.connection import Connection
from kubemq.transport.interceptors import AuthInterceptors
from kubemq.transport.tls_config import TlsConfig

if TYPE_CHECKING:
    from kubemq._internal.auth import TokenHolder


def _get_ssl_credentials(tls_config: TlsConfig) -> grpc.ChannelCredentials:
    from kubemq.transport.transport import _get_ssl_credentials as get_creds

    return get_creds(tls_config)


def _get_call_options(connection: Connection) -> list[tuple[str, Any]]:
    from kubemq.transport.transport import _get_call_options as get_opts

    return list(get_opts(connection))


class ConnectionState:
    """Class for managing the shared connection state across components."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.is_connected = True

    def set_connected(self, value: bool) -> bool:
        """Set the connection state and return True if it changed."""
        with self.lock:
            old_state = self.is_connected
            self.is_connected = value
            return old_state != value  # Return True if state changed

    def is_accepting_requests(self) -> bool:
        """Check if the channel is accepting requests."""
        with self.lock:
            return self.is_connected


class ChannelManager:
    """Centralized manager for gRPC channel creation and reconnection.

    This class coordinates channel access and recreation across multiple components.
    """

    def __init__(
        self,
        connection: Connection,
        logger: logging.Logger,
        token_holder: TokenHolder | None = None,
    ) -> None:
        self._opts = connection.complete()
        self._connection = connection
        self._channel: grpc.Channel | None = None
        self._client: kubemq_pb2_grpc.kubemqStub | None = None
        self._channel_lock = threading.Lock()
        self.connection_state = ConnectionState()
        self.logger = logger
        self._registered_clients: list[Any] = []
        self._token_holder = token_holder
        self._initialize_channel()

    def _build_auth_interceptor(self) -> AuthInterceptors:
        """Build an auth interceptor using TokenHolder if available."""
        if self._token_holder is not None:
            return AuthInterceptors(self._token_holder)
        from kubemq._internal.auth import TokenHolder as _TH

        holder = _TH(self._opts.auth_token or None)
        self._token_holder = holder
        return AuthInterceptors(holder)

    def _initialize_channel(self) -> None:
        """Initialize the gRPC channel and client stub."""
        with self._channel_lock:
            try:
                auth_interceptor = self._build_auth_interceptor()
                interceptors = [auth_interceptor]
                credentials = (
                    _get_ssl_credentials(self._opts.tls) if self._opts.tls.enabled else None
                )
                self._channel = (
                    grpc.secure_channel(
                        self._opts.address,
                        credentials,
                        options=_get_call_options(self._opts),
                    )
                    if credentials
                    else grpc.insecure_channel(
                        self._opts.address, options=_get_call_options(self._opts)
                    )
                )
                self._channel = grpc.intercept_channel(self._channel, *interceptors)
                self._client = kubemq_pb2_grpc.kubemqStub(self._channel)  # type: ignore[no-untyped-call]
                self._test_connection()
                self.connection_state.set_connected(True)
            except Exception as ex:
                self.logger.error(f"Failed to initialize channel: {str(ex)}")
                self.connection_state.set_connected(False)
                raise ex

    def _test_connection(self) -> bool:
        """Test the connection to the server."""
        try:
            if self._client is None:
                return False
            self._client.Ping(Empty())
            return True
        except Exception as e:
            self.logger.error(f"Connection test failed: {str(e)}")
            return False

    def register_client(self, client_ref: Any) -> None:
        """Register a client to be notified of channel updates."""
        self._registered_clients.append(client_ref)

    def get_client(self) -> kubemq_pb2_grpc.kubemqStub:
        """Get the current gRPC client stub."""
        with self._channel_lock:
            if self._client is None:
                raise RuntimeError("Channel not initialized")
            return self._client

    def is_channel_healthy(self) -> bool:
        """Check if the channel is in a healthy state."""
        try:
            return self._test_connection()
        except Exception:
            return False

    def recreate_channel(self) -> kubemq_pb2_grpc.kubemqStub:
        """Recreate the gRPC channel and client after a connection failure.

        Returns:
            kubemq_pb2_grpc.kubemqStub: New client instance
        """
        self.logger.info("Starting channel recreation process")
        self.connection_state.set_connected(False)

        # Check if auto-reconnect is disabled
        if self._connection.disable_auto_reconnect:
            self.logger.warning(
                "Auto-reconnect is disabled, not attempting to recreate channel"
            )
            raise ConnectionError("Auto-reconnect is disabled by configuration")

        # PY-3: Sleep OUTSIDE the lock to avoid blocking other components
        # (e.g., senders and receivers) that share the same channel lock.
        reconnect_seconds = self._connection.reconnect_interval_seconds
        self.logger.info(f"Waiting {reconnect_seconds} seconds before reconnection")
        time.sleep(reconnect_seconds)

        with self._channel_lock:
            # Close existing channel if exists
            if self._channel is not None:
                try:
                    self._channel.close()
                except Exception as e:
                    self.logger.warning(f"Error closing existing channel: {str(e)}")
                self._channel = None
                self._client = None

            # Recreate channel with existing credentials and options
            try:
                auth_interceptor = self._build_auth_interceptor()
                interceptors = [auth_interceptor]
                credentials = (
                    _get_ssl_credentials(self._opts.tls) if self._opts.tls.enabled else None
                )

                self._channel = (
                    grpc.secure_channel(
                        self._opts.address,
                        credentials,
                        options=_get_call_options(self._opts),
                    )
                    if credentials
                    else grpc.insecure_channel(
                        self._opts.address, options=_get_call_options(self._opts)
                    )
                )
                self._channel = grpc.intercept_channel(self._channel, *interceptors)
                self._client = kubemq_pb2_grpc.kubemqStub(self._channel)  # type: ignore[no-untyped-call]

                # Test the connection
                if self._test_connection():
                    self.logger.info("Successfully recreated gRPC channel and verified connection")
                    self.connection_state.set_connected(True)
                else:
                    self.logger.warning("Channel recreated but connection test failed")
                    # We'll keep the connected state as False
            except Exception as e:
                self.logger.error(f"Failed to recreate channel: {str(e)}")
                raise e

            if self._client is None:
                raise RuntimeError("Failed to recreate channel - client is None")
            return self._client

    def close(self) -> None:
        """Close the channel and clean up resources."""
        with self._channel_lock:
            if self._channel is not None:
                try:
                    self._channel.close()
                except Exception as e:
                    self.logger.warning(f"Error while closing channel: {str(e)}")
                self._channel = None
                self._client = None
            self.connection_state.set_connected(False)
