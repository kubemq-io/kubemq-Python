"""Base client classes for KubeMQ Python SDK."""

from __future__ import annotations

import asyncio
import logging
import threading
from abc import ABC
from types import TracebackType
from typing import TYPE_CHECKING, TypeVar

from kubemq.core.compat import run_in_thread
from kubemq.core.config import ClientConfig
from kubemq.core.exceptions import KubeMQConnectionError, from_grpc_error
from kubemq.core.types import ServerInfo

if TYPE_CHECKING:
    from kubemq.common.async_cancellation_token import AsyncCancellationToken
    from kubemq.transport.async_transport import AsyncTransport
    from kubemq.transport.transport import Transport

T = TypeVar("T", bound="BaseClient")
AT = TypeVar("AT", bound="AsyncBaseClient")
NAT = TypeVar("NAT", bound="NativeAsyncBaseClient")


class BaseClient(ABC):  # noqa: B024
    """Abstract base class for synchronous KubeMQ clients.

    Provides common functionality for PubSub, Queues, and CQ clients:
    - Connection management (connect, close, reconnect)
    - Context manager support (with statement)
    - Server ping/health check
    - Thread-safe resource management

    Subclasses must implement domain-specific methods.
    """

    def __init__(
        self,
        address: str = "",
        client_id: str | None = None,
        auth_token: str | None = None,
        config: ClientConfig | None = None,
        **kwargs,
    ) -> None:
        """Initialize the client.

        Can be initialized either with individual parameters or a ClientConfig object.

        Args:
            address: KubeMQ server address (host:port)
            client_id: Client identifier (defaults to hostname)
            auth_token: Authentication token
            config: Pre-built ClientConfig object (overrides other params)
            **kwargs: Additional configuration options passed to ClientConfig
        """
        if config is not None:
            self._config = config
        else:
            self._config = ClientConfig(
                address=address,
                client_id=client_id,
                auth_token=auth_token,
                **kwargs,
            )

        self._transport: Transport | None = None
        self._logger = logging.getLogger(f"kubemq.{self.__class__.__name__}")
        self._lock = threading.RLock()
        self._closed = False
        self._shutdown_event = threading.Event()

        # Set up logging level
        if self._config.log_level is not None:
            self._logger.setLevel(self._config.log_level)
        else:
            self._logger.setLevel(logging.CRITICAL + 1)  # Effectively disabled

        # Initialize transport
        self._initialize()

    def _initialize(self) -> None:
        """Initialize the transport and establish connection."""
        from kubemq.transport.transport import Transport

        try:
            connection = self._config.to_legacy_connection()
            self._transport = Transport(connection).initialize()
            self._logger.debug(f"Connected to {self._config.address}")
        except Exception as e:
            self._logger.error(f"Failed to connect: {e}")
            raise from_grpc_error(e) from e

    @property
    def is_connected(self) -> bool:
        """Check if the client is connected to the server."""
        return self._transport is not None and self._transport.is_connected()

    @property
    def config(self) -> ClientConfig:
        """Get the client configuration."""
        return self._config

    def ping(self) -> ServerInfo:
        """Ping the server and return server information.

        Returns:
            ServerInfo with host, version, and uptime information

        Raises:
            KubeMQConnectionError: If not connected or ping fails
        """
        self._ensure_connected()
        assert self._transport is not None  # guaranteed by _ensure_connected
        try:
            self._logger.debug(f"Pinging {self._config.address}")
            return self._transport.ping()
        except Exception as e:
            self._logger.error(f"Ping failed: {e}")
            raise from_grpc_error(e) from e

    def close(self) -> None:
        """Close the client and release all resources.

        This method is idempotent - calling it multiple times is safe.
        """
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._shutdown_event.set()

            self._logger.debug(f"Closing connection to {self._config.address}")
            self._cleanup_resources()

            if self._transport:
                try:
                    self._transport.close()
                except Exception as e:
                    self._logger.warning(f"Error closing transport: {e}")
                finally:
                    self._transport = None

    def _cleanup_resources(self) -> None:  # noqa: B027
        """Clean up any additional resources held by the client.

        Subclasses can override this to clean up their specific resources.
        This is called before the transport is closed.
        """

    def _ensure_connected(self) -> None:
        """Ensure the client is connected.

        Raises:
            RuntimeError: If the client is closed
            KubeMQConnectionError: If not connected
        """
        if self._closed:
            raise RuntimeError("Client is closed")
        if not self.is_connected:
            raise KubeMQConnectionError("Client is not connected to server")

    def __enter__(self: T) -> T:
        """Enter context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit context manager and close the client."""
        self.close()


class AsyncBaseClient(ABC):  # noqa: B024
    """Abstract base class for asynchronous KubeMQ clients.

    Provides common functionality for async PubSub, Queues, and CQ clients:
    - Async connection management
    - Async context manager support (async with)
    - Server ping/health check
    - Coroutine-safe resource management

    Note: This class uses thread-based async wrappers for the transport layer.
    Phase 4 will implement native async gRPC support.
    """

    def __init__(
        self,
        address: str = "",
        client_id: str | None = None,
        auth_token: str | None = None,
        config: ClientConfig | None = None,
        **kwargs,
    ) -> None:
        """Initialize the async client.

        Note: The client is NOT connected after initialization.
        Use `await client.connect()` or `async with client:` to connect.

        Args:
            address: KubeMQ server address (host:port)
            client_id: Client identifier (defaults to hostname)
            auth_token: Authentication token
            config: Pre-built ClientConfig object (overrides other params)
            **kwargs: Additional configuration options
        """
        if config is not None:
            self._config = config
        else:
            self._config = ClientConfig(
                address=address,
                client_id=client_id,
                auth_token=auth_token,
                **kwargs,
            )

        self._transport: Transport | None = None
        self._logger = logging.getLogger(f"kubemq.{self.__class__.__name__}")
        self._closed = False
        self._close_lock = asyncio.Lock()  # Thread safety for async close

        # Set up logging level
        if self._config.log_level is not None:
            self._logger.setLevel(self._config.log_level)
        else:
            self._logger.setLevel(logging.CRITICAL + 1)

    async def connect(self) -> None:
        """Connect to the KubeMQ server.

        This method must be called before using the client, unless
        using the async context manager.

        Raises:
            KubeMQConnectionError: If connection fails
        """
        from kubemq.transport.transport import Transport

        try:
            connection = self._config.to_legacy_connection()
            # Use thread wrapper for blocking initialization
            self._transport = await run_in_thread(lambda: Transport(connection).initialize())
            self._logger.debug(f"Connected to {self._config.address}")
        except Exception as e:
            self._logger.error(f"Failed to connect: {e}")
            raise from_grpc_error(e) from e

    @property
    def is_connected(self) -> bool:
        """Check if the client is connected to the server."""
        return self._transport is not None and self._transport.is_connected()

    @property
    def config(self) -> ClientConfig:
        """Get the client configuration."""
        return self._config

    async def ping(self) -> ServerInfo:
        """Ping the server and return server information.

        Returns:
            ServerInfo with host, version, and uptime information

        Raises:
            KubeMQConnectionError: If not connected or ping fails
        """
        self._ensure_connected()
        assert self._transport is not None  # guaranteed by _ensure_connected
        try:
            self._logger.debug(f"Pinging {self._config.address}")
            return await run_in_thread(self._transport.ping)
        except Exception as e:
            self._logger.error(f"Ping failed: {e}")
            raise from_grpc_error(e) from e

    async def close(self) -> None:
        """Close the client and release all resources.

        This method is idempotent - calling it multiple times is safe.
        Uses async lock to prevent race conditions from concurrent coroutines.
        """
        async with self._close_lock:
            if self._closed:
                return
            self._closed = True

            self._logger.debug(f"Closing connection to {self._config.address}")
            await self._cleanup_resources()

            if self._transport:
                try:
                    # Transport has close_async() method - use it directly
                    await self._transport.close_async()
                except Exception as e:
                    self._logger.warning(f"Error closing transport: {e}")
                finally:
                    self._transport = None

    async def _cleanup_resources(self) -> None:  # noqa: B027
        """Clean up any additional resources held by the client.

        Subclasses can override this to clean up their specific resources.
        """

    def _ensure_connected(self) -> None:
        """Ensure the client is connected.

        Raises:
            RuntimeError: If the client is closed
            KubeMQConnectionError: If not connected
        """
        if self._closed:
            raise RuntimeError("Client is closed")
        if not self.is_connected:
            raise KubeMQConnectionError("Client is not connected to server")

    async def __aenter__(self: AT) -> AT:
        """Enter async context manager and connect."""
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit async context manager and close the client."""
        await self.close()


class NativeAsyncBaseClient(ABC):  # noqa: B024
    """Abstract base class for native async KubeMQ clients.

    This class uses native gRPC async operations via AsyncTransport,
    without thread pools. It provides optimal performance for async applications.

    Features:
    - Native async gRPC operations (no thread wrappers)
    - Graceful shutdown with in-flight operation tracking
    - Subscription tracking for cleanup
    - Connection state management

    Note: This is the Phase 4 implementation. For backward compatibility
    with thread-wrapped async, use AsyncBaseClient.
    """

    def __init__(
        self,
        address: str = "",
        client_id: str | None = None,
        auth_token: str | None = None,
        config: ClientConfig | None = None,
        **kwargs,
    ) -> None:
        """Initialize the native async client.

        Note: The client is NOT connected after initialization.
        Use `await client.connect()` or `async with client:` to connect.

        Args:
            address: KubeMQ server address (host:port)
            client_id: Client identifier (defaults to hostname)
            auth_token: Authentication token
            config: Pre-built ClientConfig object (overrides other params)
            **kwargs: Additional configuration options
        """
        if config is not None:
            self._config = config
        else:
            self._config = ClientConfig(
                address=address,
                client_id=client_id,
                auth_token=auth_token,
                **kwargs,
            )

        self._transport: AsyncTransport | None = None
        self._logger = logging.getLogger(f"kubemq.{self.__class__.__name__}")
        self._closed = False
        self._closing = False
        self._connect_lock = asyncio.Lock()

        # Track active subscriptions for cleanup
        self._active_subscriptions: set[AsyncCancellationToken] = set()
        self._subscriptions_lock = asyncio.Lock()

        # Set up logging level
        if self._config.log_level is not None:
            self._logger.setLevel(self._config.log_level)
        else:
            self._logger.setLevel(logging.CRITICAL + 1)

    async def connect(self) -> None:
        """Connect to the KubeMQ server using native async transport.

        This method must be called before using the client, unless
        using the async context manager.

        Raises:
            KubeMQConnectionError: If connection fails
            RuntimeError: If client is closed
        """
        from kubemq.transport.async_transport import AsyncTransport

        async with self._connect_lock:
            if self._transport is not None:
                return
            if self._closed:
                raise RuntimeError("Client is closed and cannot be reconnected")

            try:
                self._transport = AsyncTransport(self._config)
                await self._transport.connect()
                self._logger.debug(f"Connected to {self._config.address}")
            except Exception as e:
                self._transport = None
                self._logger.error(f"Failed to connect: {e}")
                raise from_grpc_error(e) from e

    @property
    def is_connected(self) -> bool:
        """Check if the client is connected to the server."""
        return self._transport is not None and self._transport.is_connected and not self._closing

    @property
    def config(self) -> ClientConfig:
        """Get the client configuration."""
        return self._config

    async def ping(self) -> ServerInfo:
        """Ping the server using native async.

        Returns:
            ServerInfo with host, version, and uptime information

        Raises:
            KubeMQConnectionError: If not connected or ping fails
        """
        self._ensure_connected()
        assert self._transport is not None  # guaranteed by _ensure_connected
        return await self._transport.ping()

    async def close(self) -> None:
        """Close client with graceful shutdown.

        Shutdown sequence:
        1. Stop accepting new operations
        2. Cancel all active subscriptions
        3. Wait for in-flight operations (with timeout)
        4. Close transport

        This method is idempotent - calling it multiple times is safe.
        """
        async with self._connect_lock:
            if self._closed or self._closing:
                return

            self._closing = True
            self._logger.debug("Starting client shutdown")

            # 1. Cancel all subscriptions
            async with self._subscriptions_lock:
                for token in self._active_subscriptions:
                    token.cancel()
                self._active_subscriptions.clear()

            # 2. Close transport (handles its own stream cleanup)
            if self._transport:
                await self._transport.close()
                self._transport = None

            self._closed = True
            self._closing = False
            self._logger.debug("Client shutdown complete")

    def _ensure_connected(self) -> None:
        """Ensure client is connected and not closing.

        Raises:
            RuntimeError: If the client is closed or closing
            KubeMQConnectionError: If not connected
        """
        if self._closed:
            raise RuntimeError("Client is closed")
        if self._closing:
            raise RuntimeError("Client is shutting down")
        if not self.is_connected:
            raise KubeMQConnectionError("Client is not connected to server")

    async def _register_subscription(
        self,
        token: AsyncCancellationToken,
    ) -> None:
        """Register an active subscription for tracking.

        Args:
            token: The cancellation token for the subscription.
        """
        async with self._subscriptions_lock:
            self._active_subscriptions.add(token)

    async def _unregister_subscription(
        self,
        token: AsyncCancellationToken,
    ) -> None:
        """Unregister a completed subscription.

        Args:
            token: The cancellation token to unregister.
        """
        async with self._subscriptions_lock:
            self._active_subscriptions.discard(token)

    async def __aenter__(self: NAT) -> NAT:
        """Enter async context manager and connect."""
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit async context manager and close the client."""
        await self.close()
