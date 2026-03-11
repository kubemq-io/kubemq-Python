"""Base client classes for KubeMQ Python SDK."""

from __future__ import annotations

import asyncio
import threading
import time
from abc import ABC
from types import TracebackType
from typing import TYPE_CHECKING, Any, TypeVar

from kubemq._internal.logging import NOOP_LOGGER, StdLibLoggerAdapter
from kubemq._internal.telemetry import KubeMQInstrumentor, KubeMQMetrics
from kubemq.core.compat import run_in_thread
from kubemq.core.config import ClientConfig
from kubemq.core.exceptions import (
    KubeMQClientClosedError,
    KubeMQConnectionError,
    KubeMQValidationError,
    from_grpc_error,
)
from kubemq.core.types import ServerInfo

if TYPE_CHECKING:
    from kubemq._internal.transport.state import AnyStateCallback
    from kubemq.common.async_cancellation_token import AsyncCancellationToken
    from kubemq.core.types import ConnectionState
    from kubemq.transport.async_transport import AsyncTransport
    from kubemq.transport.transport import Transport

T = TypeVar("T", bound="BaseClient")
AT = TypeVar("AT", bound="AsyncBaseClient")
NAT = TypeVar("NAT", bound="NativeAsyncBaseClient")


def _resolve_logger(config: ClientConfig, class_name: str) -> Any:
    """Resolve the logger to use based on ClientConfig.

    Priority:
        1. config.logger (user-injected Logger Protocol instance)
        2. StdLibLoggerAdapter (when config.log_level is set — backward compat)
        3. NOOP_LOGGER (default — zero overhead)
    """
    if config.logger is not None:
        return config.logger
    if config.log_level is not None:
        return StdLibLoggerAdapter(
            name=f"kubemq.{class_name}",
            level=config.log_level,
        )
    return NOOP_LOGGER


def _create_instrumentor(config: ClientConfig, logger: Any) -> KubeMQInstrumentor:
    """Create a KubeMQInstrumentor and attach KubeMQMetrics."""
    instrumentor = KubeMQInstrumentor(
        client_id=config.client_id or "",
        address=config.address,
        tracer_provider=config.tracer_provider,
        meter_provider=config.meter_provider,
        logger=logger,
    )
    instrumentor._metrics = KubeMQMetrics(
        meter=instrumentor._meter,
        max_channel_cardinality=config.max_channel_cardinality,
        channel_allowlist=set(config.channel_allowlist),
        logger=logger,
    )
    return instrumentor


class BaseClient(ABC):  # noqa: B024
    """Abstract base class for synchronous KubeMQ clients.

    Provides common functionality for PubSub, Queues, and CQ clients:
    - Connection management (connect, close, reconnect)
    - Context manager support (with statement)
    - Server ping/health check
    - Thread-safe resource management

    Subclasses must implement domain-specific methods.

    Thread Safety:
        A single client instance uses one gRPC channel and is safe to share
        across threads. Create one client and reuse it; do not create a new
        client per operation.

        Example — shared client across threads::

            client = PubSubClient(address="localhost:50000")
            for _ in range(10):
                threading.Thread(target=worker, args=(client,)).start()
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
        self._logger: Any = _resolve_logger(self._config, self.__class__.__name__)
        self._instrumentor = _create_instrumentor(self._config, self._logger)
        self._lock = threading.RLock()
        self._closed = False
        self._shutdown_event = threading.Event()
        self._subscription_threads: list[threading.Thread] = []
        self._subscription_threads_lock = threading.Lock()

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

    def set_token(self, token: str) -> None:
        """Update the auth token for all subsequent requests.

        Takes effect immediately on the next gRPC call — no reconnection required.
        Thread-safe: can be called from any thread.

        Args:
            token: The new auth token string.
        """
        if self._transport is not None:
            self._transport.set_token(token)

    def _register_subscription_thread(self, thread: threading.Thread) -> None:
        """Register a subscription thread for shutdown tracking."""
        with self._subscription_threads_lock:
            self._subscription_threads = [
                t for t in self._subscription_threads if t.is_alive()
            ]
            self._subscription_threads.append(thread)

    def close(self) -> None:
        """Close the client and release all resources.

        Waits for in-flight subscription callbacks to complete up to
        ``callback_completion_timeout`` (default 30s), then forces
        shutdown of remaining threads.

        This method is idempotent - calling it multiple times is safe.
        """
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._shutdown_event.set()

            self._logger.debug(f"Closing connection to {self._config.address}")
            self._cleanup_resources()

            callback_timeout = getattr(
                self._config, "callback_completion_timeout", 30.0
            )
            with self._subscription_threads_lock:
                threads_to_join = list(self._subscription_threads)
                self._subscription_threads.clear()

        if threads_to_join:
            self._logger.debug(
                "Waiting for %d subscription threads to drain",
                len(threads_to_join),
            )
            deadline = time.monotonic() + callback_timeout
            for thread in threads_to_join:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._logger.warning(
                        "Callback drain timeout (%.1fs) reached",
                        callback_timeout,
                    )
                    break
                thread.join(timeout=remaining)

        with self._lock:
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

    def _validate_message_size(self, body: bytes) -> None:
        """Validate message body size against config.max_send_size.

        Raises:
            KubeMQValidationError: If body exceeds max_send_size.
        """
        max_size = self._config.max_send_size
        if max_size > 0 and len(body) > max_size:
            raise KubeMQValidationError(
                f"Message body size ({len(body)} bytes) exceeds maximum "
                f"send size ({max_size} bytes). "
                f"Reduce message size or increase max_send_size in ClientConfig.",
                is_retryable=False,
            )

    def _ensure_connected(self) -> None:
        """Ensure the client is connected.

        Raises:
            KubeMQClientClosedError: If the client is closed
            KubeMQConnectionError: If not connected
        """
        if self._closed:
            raise KubeMQClientClosedError("Client is closed")
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

    Thread Safety:
        A single client instance uses one gRPC channel and is safe to share
        across tasks. Create one client and reuse it; do not create a new
        client per operation.

        Example — shared async client across tasks::

            async with AsyncPubSubClient(address="localhost:50000") as client:
                tasks = [process(client) for _ in range(10)]
                await asyncio.gather(*tasks)
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
        self._logger: Any = _resolve_logger(self._config, self.__class__.__name__)
        self._instrumentor = _create_instrumentor(self._config, self._logger)
        self._closed = False
        self._close_lock = asyncio.Lock()

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
            KubeMQClientClosedError: If the client is closed
            KubeMQConnectionError: If not connected
        """
        if self._closed:
            raise KubeMQClientClosedError("Client is closed")
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

    Thread Safety:
        A single client instance uses one gRPC channel and is safe to share
        across tasks. Create one client and reuse it; do not create a new
        client per operation.

        Example — shared async client across tasks::

            async with AsyncPubSubClient(address="localhost:50000") as client:
                tasks = [process(client) for _ in range(10)]
                await asyncio.gather(*tasks)
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

        from kubemq._internal.retry import RetryExecutor
        self._retry_executor = RetryExecutor(self._config.retry_policy)

        self._logger: Any = _resolve_logger(self._config, self.__class__.__name__)
        self._instrumentor = _create_instrumentor(self._config, self._logger)
        self._closed = False
        self._closing = False
        self._connect_lock = asyncio.Lock()

        # Track active subscriptions for cleanup
        self._active_subscriptions: set[AsyncCancellationToken] = set()
        self._subscriptions_lock = asyncio.Lock()
        self._subscription_tasks: set[asyncio.Task] = set()  # type: ignore[type-arg]

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
                raise KubeMQClientClosedError("Client is closed and cannot be reconnected")

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

    def set_token(self, token: str) -> None:
        """Update the auth token for all subsequent requests.

        Takes effect immediately on the next gRPC call — no reconnection required.
        Thread-safe: can be called from any thread or coroutine.

        Args:
            token: The new auth token string.
        """
        if self._transport is not None:
            self._transport.set_token(token)

    @property
    def connection_state(self) -> ConnectionState:
        """Current connection lifecycle state."""
        from kubemq.core.types import ConnectionState

        if self._transport is None:
            return ConnectionState.IDLE
        return self._transport.connection_state

    def on_connected(self, callback: AnyStateCallback) -> None:
        """Register callback for CONNECTING -> READY transition."""
        if self._transport is not None:
            self._transport.on_connected(callback)

    def on_disconnected(self, callback: AnyStateCallback) -> None:
        """Register callback for READY -> RECONNECTING transition."""
        if self._transport is not None:
            self._transport.on_disconnected(callback)

    def on_reconnecting(self, callback: AnyStateCallback) -> None:
        """Register callback for any -> RECONNECTING transition."""
        if self._transport is not None:
            self._transport.on_reconnecting(callback)

    def on_reconnected(self, callback: AnyStateCallback) -> None:
        """Register callback for RECONNECTING -> READY transition."""
        if self._transport is not None:
            self._transport.on_reconnected(callback)

    async def close(self) -> None:
        """Close client with graceful callback drain.

        Shutdown sequence:
            1. Set ``_closing`` flag — new operations raise ``KubeMQClientClosedError``.
            2. Cancel all subscription tokens (stops new message delivery).
            3. Wait for in-flight callbacks up to ``callback_completion_timeout`` (default 30s).
            4. Force-cancel remaining callback tasks after timeout.
            5. Close transport (which handles its own drain per SPEC-CONN-4).

        This method is idempotent - calling it multiple times is safe.
        """
        async with self._connect_lock:
            if self._closed or self._closing:
                return

            self._closing = True
            self._logger.debug("Starting client shutdown")

            callback_timeout = getattr(
                self._config, "callback_completion_timeout", 30.0
            )

            # 1. Cancel all subscription tokens (stops new message delivery)
            async with self._subscriptions_lock:
                for token in self._active_subscriptions:
                    token.cancel()

            # 2. Wait for subscription tasks to complete (callback drain)
            async with self._subscriptions_lock:
                tasks = list(self._subscription_tasks)

            if tasks:
                self._logger.debug(
                    "Waiting for %d subscription tasks to drain callbacks",
                    len(tasks),
                )
                done, pending = await asyncio.wait(
                    tasks, timeout=callback_timeout
                )
                if pending:
                    self._logger.warning(
                        "Callback drain timeout (%.1fs) reached, "
                        "%d tasks still pending — cancelling",
                        callback_timeout,
                        len(pending),
                    )
                    for task in pending:
                        task.cancel()
                    await asyncio.gather(*pending, return_exceptions=True)

            # 3. Clear subscriptions and tasks
            async with self._subscriptions_lock:
                self._active_subscriptions.clear()
                self._subscription_tasks.clear()

            # 4. Close transport (handles its own stream drain per SPEC-CONN-4)
            if self._transport:
                await self._transport.close()
                self._transport = None

            self._closed = True
            self._closing = False
            self._logger.debug("Client shutdown complete")

    def _validate_message_size(self, body: bytes) -> None:
        """Validate message body size against config.max_send_size.

        Raises:
            KubeMQValidationError: If body exceeds max_send_size.
        """
        max_size = self._config.max_send_size
        if max_size > 0 and len(body) > max_size:
            raise KubeMQValidationError(
                f"Message body size ({len(body)} bytes) exceeds maximum "
                f"send size ({max_size} bytes). "
                f"Reduce message size or increase max_send_size in ClientConfig.",
                is_retryable=False,
            )

    def _ensure_connected(self) -> None:
        """Ensure client is connected and not closing.

        Raises:
            KubeMQClientClosedError: If the client is closed or closing
            KubeMQConnectionError: If not connected
        """
        if self._closed:
            raise KubeMQClientClosedError("Client is closed")
        if self._closing:
            raise KubeMQClientClosedError("Client is shutting down")
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

    def _register_subscription_task(self, task: asyncio.Task) -> None:  # type: ignore[type-arg]
        """Register an asyncio.Task for shutdown tracking."""
        self._subscription_tasks.add(task)
        task.add_done_callback(self._subscription_tasks.discard)

    def _unregister_subscription_task(self, task: asyncio.Task) -> None:  # type: ignore[type-arg]
        """Remove a subscription task from tracking."""
        self._subscription_tasks.discard(task)

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
