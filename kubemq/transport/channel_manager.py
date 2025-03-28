import threading
import logging
import time
import grpc
from kubemq.transport.connection import Connection
from kubemq.transport.interceptors import AuthInterceptors
from kubemq.transport.tls_config import TlsConfig
import kubemq.grpc.kubemq_pb2_grpc as kubemq_pb2_grpc
from kubemq.grpc import Empty


def _get_ssl_credentials(tls_config: TlsConfig):
    from kubemq.transport.transport import _get_ssl_credentials as get_creds

    return get_creds(tls_config)


def _get_call_options(connection: Connection):
    from kubemq.transport.transport import _get_call_options as get_opts

    return get_opts(connection)


class ConnectionState:
    """
    Class for managing the shared connection state across components.
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.is_connected = True

    def set_connected(self, value: bool):
        with self.lock:
            old_state = self.is_connected
            self.is_connected = value
            return old_state != value  # Return True if state changed

    def is_accepting_requests(self) -> bool:
        with self.lock:
            return self.is_connected


class ChannelManager:
    """
    Centralized manager for gRPC channel creation and reconnection.

    This class coordinates channel access and recreation across multiple components.
    """

    def __init__(self, connection: Connection, logger: logging.Logger):
        self._opts = connection.complete()
        self._connection = connection
        self._channel = None
        self._client = None
        self._channel_lock = threading.Lock()
        self.connection_state = ConnectionState()
        self.logger = logger
        self._registered_clients = []
        self._initialize_channel()

    def _initialize_channel(self):
        """Initialize the gRPC channel and client stub"""
        with self._channel_lock:
            try:
                auth_interceptor = AuthInterceptors(self._opts.auth_token)
                interceptors = [auth_interceptor]
                credentials = (
                    _get_ssl_credentials(self._opts.tls)
                    if self._opts.tls.enabled
                    else None
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
                self._client = kubemq_pb2_grpc.kubemqStub(self._channel)
                self._test_connection()
                self.connection_state.set_connected(True)
            except Exception as ex:
                self.logger.error(f"Failed to initialize channel: {str(ex)}")
                self.connection_state.set_connected(False)
                raise ex

    def _test_connection(self):
        """Test the connection to the server"""
        try:
            self._client.Ping(Empty())
            return True
        except Exception as e:
            self.logger.error(f"Connection test failed: {str(e)}")
            return False

    def register_client(self, client_ref):
        """Register a client to be notified of channel updates"""
        self._registered_clients.append(client_ref)

    def get_client(self) -> kubemq_pb2_grpc.kubemqStub:
        """Get the current gRPC client stub"""
        with self._channel_lock:
            return self._client

    def is_channel_healthy(self) -> bool:
        """Check if the channel is in a healthy state"""
        try:
            return self._test_connection()
        except Exception:
            return False

    def recreate_channel(self) -> kubemq_pb2_grpc.kubemqStub:
        """
        Recreate the gRPC channel and client after a connection failure

        Returns:
            kubemq_pb2_grpc.kubemqStub: New client instance
        """
        with self._channel_lock:
            self.logger.info("Starting channel recreation process")
            self.connection_state.set_connected(False)

            # Check if auto-reconnect is disabled
            if self._connection.disable_auto_reconnect:
                self.logger.warning(
                    "Auto-reconnect is disabled, not attempting to recreate channel"
                )
                raise ConnectionError("Auto-reconnect is disabled by configuration")

            # Close existing channel if exists
            if self._channel is not None:
                try:
                    self._channel.close()
                except Exception as e:
                    self.logger.warning(f"Error closing existing channel: {str(e)}")
                self._channel = None
                self._client = None

            # Use fixed reconnection interval from client configuration
            reconnect_seconds = self._connection.reconnect_interval_seconds
            if reconnect_seconds <= 0:
                reconnect_seconds = 1  # Default to 1 second if not specified

            self.logger.info(f"Waiting {reconnect_seconds} seconds before reconnection")
            time.sleep(reconnect_seconds)

            # Recreate channel with existing credentials and options
            try:
                auth_interceptor = AuthInterceptors(self._opts.auth_token)
                interceptors = [auth_interceptor]
                credentials = (
                    _get_ssl_credentials(self._opts.tls)
                    if self._opts.tls.enabled
                    else None
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
                self._client = kubemq_pb2_grpc.kubemqStub(self._channel)

                # Test the connection
                if self._test_connection():
                    self.logger.info(
                        "Successfully recreated gRPC channel and verified connection"
                    )
                    self.connection_state.set_connected(True)
                else:
                    self.logger.warning("Channel recreated but connection test failed")
                    # We'll keep the connected state as False
            except Exception as e:
                self.logger.error(f"Failed to recreate channel: {str(e)}")
                raise e

            return self._client

    def close(self):
        """Close the channel and clean up resources"""
        with self._channel_lock:
            if self._channel is not None:
                try:
                    self._channel.close()
                except Exception as e:
                    self.logger.warning(f"Error while closing channel: {str(e)}")
                self._channel = None
                self._client = None
            self.connection_state.set_connected(False)
