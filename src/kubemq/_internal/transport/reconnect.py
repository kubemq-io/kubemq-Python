"""Auto-reconnection with bounded buffering.

Provides ReconnectionManager for async reconnection with exponential
backoff, bounded message buffering during reconnection, and
subscription recovery after reconnection.

Implements REQ-CONN-1 from 02-connection-transport-spec.md.
"""

from __future__ import annotations

import asyncio
import collections
import contextlib
import logging
import threading
from collections.abc import Awaitable, Callable
from typing import Any

from kubemq.core.exceptions import KubeMQBufferFullError

BufferDrainCallback = Callable[[int], None]
AsyncBufferDrainCallback = Callable[[int], Awaitable[None]]
AnyBufferDrainCallback = BufferDrainCallback | AsyncBufferDrainCallback


class ReconnectConfig:
    """Reconnection parameters extracted from ClientConfig.

    Internal value object — not exposed publicly.
    All public configuration goes through ClientConfig fields.
    """

    __slots__ = (
        "max_reconnect_attempts",
        "initial_reconnect_delay_ms",
        "max_reconnect_delay_ms",
        "reconnect_backoff_multiplier",
        "reconnect_buffer_size",
        "auto_reconnect",
    )

    def __init__(
        self,
        *,
        max_reconnect_attempts: int = -1,
        initial_reconnect_delay_ms: int = 500,
        max_reconnect_delay_ms: int = 30_000,
        reconnect_backoff_multiplier: float = 2.0,
        reconnect_buffer_size: int = 8 * 1024 * 1024,
        auto_reconnect: bool = True,
    ) -> None:
        self.max_reconnect_attempts = max_reconnect_attempts
        self.initial_reconnect_delay_ms = initial_reconnect_delay_ms
        self.max_reconnect_delay_ms = max_reconnect_delay_ms
        self.reconnect_backoff_multiplier = reconnect_backoff_multiplier
        self.reconnect_buffer_size = reconnect_buffer_size
        self.auto_reconnect = auto_reconnect


class _BoundedByteBuffer:
    """Thread-safe bounded buffer that tracks total byte size.

    When buffer reaches max_bytes, raises KubeMQBufferFullError.
    If max_bytes is 0, every put() fails immediately (no buffering — Q2 decision).
    Items are stored in FIFO order via collections.deque.
    """

    def __init__(self, max_bytes: int) -> None:
        self._max_bytes = max_bytes
        self._buffer: collections.deque[tuple[bytes, dict[str, Any]]] = collections.deque()
        self._current_bytes = 0
        self._lock = threading.Lock()

    @property
    def current_bytes(self) -> int:
        with self._lock:
            return self._current_bytes

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._buffer)

    def put(self, data: bytes, metadata: dict[str, Any] | None = None) -> None:
        """Add item to buffer. Raises KubeMQBufferFullError if full."""
        with self._lock:
            if self._current_bytes + len(data) > self._max_bytes:
                raise KubeMQBufferFullError(
                    f"Reconnection buffer full ({self._current_bytes}/{self._max_bytes} bytes)",
                    buffer_size=self._max_bytes,
                    operation="publish",
                )
            self._buffer.append((data, metadata or {}))
            self._current_bytes += len(data)

    def drain_all(self) -> list[tuple[bytes, dict[str, Any]]]:
        """Remove and return all items in FIFO order."""
        with self._lock:
            items = list(self._buffer)
            self._buffer.clear()
            self._current_bytes = 0
            return items

    def discard_all(self) -> int:
        """Discard all items. Returns count of discarded items."""
        with self._lock:
            count = len(self._buffer)
            self._buffer.clear()
            self._current_bytes = 0
            return count


class _AsyncBoundedByteBuffer:
    """Async-safe bounded buffer using asyncio.Lock.

    Supports 'error' and 'block' overflow modes.
    If max_bytes is 0, every put() fails immediately (no buffering — Q2 decision).
    """

    def __init__(self, max_bytes: int, overflow_mode: str = "error") -> None:
        self._max_bytes = max_bytes
        self._overflow_mode = overflow_mode
        self._buffer: collections.deque[tuple[bytes, dict[str, Any]]] = collections.deque()
        self._current_bytes = 0
        self._lock = asyncio.Lock()
        self._space_available = asyncio.Event()
        self._space_available.set()

    @property
    def current_bytes(self) -> int:
        return self._current_bytes

    @property
    def count(self) -> int:
        return len(self._buffer)

    async def put(self, data: bytes, metadata: dict[str, Any] | None = None) -> None:
        """Add item to buffer. Behaviour on full depends on overflow_mode."""
        if self._max_bytes == 0:
            raise KubeMQBufferFullError(
                "Buffering disabled (max_bytes=0)",
                buffer_size=0,
                operation="publish",
            )
        if self._overflow_mode == "block":
            while True:
                async with self._lock:
                    if self._current_bytes + len(data) <= self._max_bytes:
                        self._buffer.append((data, metadata or {}))
                        self._current_bytes += len(data)
                        return
                    self._space_available.clear()
                await self._space_available.wait()
        else:
            async with self._lock:
                if self._current_bytes + len(data) > self._max_bytes:
                    raise KubeMQBufferFullError(
                        f"Reconnection buffer full ({self._current_bytes}/{self._max_bytes} bytes)",
                        buffer_size=self._max_bytes,
                        operation="publish",
                    )
                self._buffer.append((data, metadata or {}))
                self._current_bytes += len(data)

    async def drain_all(self) -> list[tuple[bytes, dict[str, Any]]]:
        """Remove and return all items in FIFO order."""
        async with self._lock:
            items = list(self._buffer)
            self._buffer.clear()
            self._current_bytes = 0
            self._space_available.set()
            return items

    async def discard_all(self) -> int:
        """Discard all items. Returns count of discarded items."""
        async with self._lock:
            count = len(self._buffer)
            self._buffer.clear()
            self._current_bytes = 0
            self._space_available.set()
            return count


class ReconnectionManager:
    """Manages async reconnection with exponential backoff.

    Coordinates:
    1. Connection drop detection
    2. Exponential backoff with jitter (via BackoffCalculator from spec 01)
    3. Bounded message buffering during reconnection
    4. Subscription recovery after reconnection
    5. DNS re-resolution (new channel per attempt)
    6. State machine transitions (READY -> RECONNECTING -> READY or CLOSED)
    """

    def __init__(
        self,
        config: ReconnectConfig,
        backoff: Any,
        on_buffer_drain: AnyBufferDrainCallback | None = None,
        logger: Any = None,
    ) -> None:
        self._config = config
        self._backoff = backoff
        self._buffer = _AsyncBoundedByteBuffer(config.reconnect_buffer_size)
        self._on_buffer_drain = on_buffer_drain
        self._logger = logger or logging.getLogger("kubemq.reconnect")
        self._reconnect_task: asyncio.Task[None] | None = None
        self._cancelled = False

    @property
    def buffer(self) -> _AsyncBoundedByteBuffer:
        return self._buffer

    @property
    def is_reconnecting(self) -> bool:
        return self._reconnect_task is not None and not self._reconnect_task.done()

    async def start_reconnection(
        self,
        connect_fn: Callable[[], Awaitable[None]],
        subscription_recovery_fn: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        """Begin reconnection loop.

        Args:
            connect_fn: Async callable that establishes a new connection.
                        Must create a new gRPC channel (DNS re-resolution).
            subscription_recovery_fn: Async callable that re-subscribes
                        all active subscriptions after connection is restored.
        """
        if self.is_reconnecting:
            return

        self._cancelled = False
        self._reconnect_task = asyncio.create_task(
            self._reconnect_loop(connect_fn, subscription_recovery_fn)
        )

    async def _reconnect_loop(
        self,
        connect_fn: Callable[[], Awaitable[None]],
        subscription_recovery_fn: Callable[[], Awaitable[None]] | None,
    ) -> None:
        """Core reconnection loop with exponential backoff."""
        attempt = 0
        max_attempts = self._config.max_reconnect_attempts

        while not self._cancelled:
            if max_attempts > 0 and attempt >= max_attempts:
                self._logger.warning(
                    "Max reconnection attempts reached attempts=%d max_attempts=%d",
                    attempt,
                    max_attempts,
                )
                await self._discard_buffer_and_notify()
                return

            delay_ms = self._backoff.delay_ms(attempt)
            delay_s = delay_ms / 1000.0

            self._logger.info(
                "Attempting reconnection attempt=%d delay=%.1fs",
                attempt + 1,
                delay_s,
            )

            await asyncio.sleep(delay_s)

            if self._cancelled:
                return

            try:
                await connect_fn()

                self._logger.info(
                    "Reconnection successful attempt=%d",
                    attempt + 1,
                )

                # Drain buffered messages after successful reconnection
                buffered = await self._buffer.drain_all()
                if buffered:
                    self._logger.info(
                        "Flushing buffered messages count=%d",
                        len(buffered),
                    )
                    if self._on_buffer_drain:
                        try:
                            if asyncio.iscoroutinefunction(self._on_buffer_drain):
                                await self._on_buffer_drain(len(buffered))
                            else:
                                self._on_buffer_drain(len(buffered))
                        except Exception:
                            self._logger.error(
                                "OnBufferDrain callback raised an exception",
                                exc_info=True,
                            )

                # Recover subscriptions
                if subscription_recovery_fn:
                    await subscription_recovery_fn()

                return

            except Exception as exc:
                self._logger.warning(
                    "Reconnection attempt failed attempt=%d error=%s",
                    attempt + 1,
                    str(exc),
                )
                attempt += 1

    async def buffer_message(self, data: bytes, metadata: dict[str, Any] | None = None) -> None:
        """Buffer a message during reconnection.

        Raises:
            KubeMQBufferFullError: When the buffer is full.
        """
        await self._buffer.put(data, metadata)

    async def cancel(self) -> None:
        """Cancel reconnection, discard buffer, and notify via callback."""
        self._cancelled = True
        if self._reconnect_task and not self._reconnect_task.done():
            self._reconnect_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reconnect_task
        await self._discard_buffer_and_notify()

    async def _discard_buffer_and_notify(self) -> None:
        """Discard buffered messages and fire OnBufferDrain callback."""
        count = await self._buffer.discard_all()
        if count > 0 and self._on_buffer_drain:
            try:
                if asyncio.iscoroutinefunction(self._on_buffer_drain):
                    await self._on_buffer_drain(count)
                else:
                    self._on_buffer_drain(count)
            except Exception:
                self._logger.error("OnBufferDrain callback raised an exception")
