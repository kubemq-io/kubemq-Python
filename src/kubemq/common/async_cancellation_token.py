"""Async-safe cancellation token using asyncio.Event.

This module provides cancellation tokens for async operations,
allowing graceful cancellation of subscriptions and long-running operations.
"""

from __future__ import annotations

import asyncio
import contextlib
import weakref
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .cancellation_token import CancellationToken


class AsyncCancellationToken:
    """Async-safe cancellation token using asyncio.Event.

    Usage:
        token = AsyncCancellationToken()

        # In subscription
        async for event in client.subscribe_to_events(sub, cancellation_token=token):
            process(event)

        # To cancel from another coroutine
        token.cancel()

    Parent-child relationships:
        # Child cancels when parent cancels
        parent = AsyncCancellationToken()
        child = AsyncCancellationToken(parent=parent)

        parent.cancel()  # Both parent and child are now cancelled

    Linked tokens:
        # Token cancels when ANY parent cancels
        token1 = AsyncCancellationToken()
        token2 = AsyncCancellationToken()
        linked = AsyncCancellationToken.create_linked(token1, token2)

        token1.cancel()  # linked is now cancelled
    """

    def __init__(self, parent: AsyncCancellationToken | None = None) -> None:
        """Initialize the cancellation token.

        Args:
            parent: Optional parent token. If provided, this token will be
                   cancelled when the parent is cancelled.
        """
        self._event = asyncio.Event()
        self._parent = parent
        self._children: list[weakref.ref[AsyncCancellationToken]] = []

        if parent is not None:
            parent._register_child(self)

    def cancel(self) -> None:
        """Signal cancellation. Thread-safe and idempotent.

        Can be called from any thread or coroutine. Multiple calls
        have no additional effect after the first.
        """
        if self._event.is_set():
            return  # Already cancelled - idempotent

        self._event.set()

        # Propagate to children - copy list to avoid modification during iteration
        children = list(self._children)
        for child_ref in children:
            child = child_ref()
            if child is not None:
                child.cancel()

    @property
    def is_cancelled(self) -> bool:
        """Check if cancellation has been requested.

        Returns:
            True if this token or any parent token has been cancelled.
        """
        if self._event.is_set():
            return True
        return bool(self._parent is not None and self._parent.is_cancelled)

    async def wait(self, timeout: float | None = None) -> bool:
        """Wait for cancellation.

        Args:
            timeout: Maximum time to wait in seconds. If None, waits indefinitely.

        Returns:
            True if cancelled, False if timeout occurred.
        """
        try:
            await asyncio.wait_for(self._event.wait(), timeout)
            return True
        except asyncio.TimeoutError:
            return False

    def _register_child(self, child: AsyncCancellationToken) -> None:
        """Register a child token for cascading cancellation.

        Args:
            child: The child token to register.
        """
        self._children.append(weakref.ref(child))
        # Cleanup dead references periodically
        self._children = [ref for ref in self._children if ref() is not None]

        # If parent is already cancelled, cancel child immediately
        if self.is_cancelled:
            child.cancel()

    @classmethod
    def create_linked(cls, *parents: AsyncCancellationToken) -> AsyncCancellationToken:
        """Create a token that cancels when ANY parent cancels.

        Args:
            *parents: Parent tokens to link to.

        Returns:
            A new token that will be cancelled when any parent is cancelled.

        Example:
            token1 = AsyncCancellationToken()
            token2 = AsyncCancellationToken()
            linked = AsyncCancellationToken.create_linked(token1, token2)

            token1.cancel()  # linked is now cancelled
        """
        token = cls()
        for parent in parents:
            parent._register_child(token)
        return token

    def __repr__(self) -> str:
        """Return string representation."""
        status = "cancelled" if self.is_cancelled else "active"
        return f"AsyncCancellationToken({status})"


class CancellationTokenBridge:
    """Bridge between sync CancellationToken and AsyncCancellationToken.

    Useful when sync code needs to cancel async operations. The bridge
    monitors a sync token and cancels the async token when the sync
    token is cancelled.

    Args:
        sync_token: The sync CancellationToken to monitor.
        poll_interval_seconds: How often to check sync token (default 0.1s).

    Example:
        from kubemq.common.cancellation_token import CancellationToken

        sync_token = CancellationToken()
        bridge = CancellationTokenBridge(sync_token)

        await bridge.start_monitoring()

        # Use bridge.async_token for async operations
        async for event in client.subscribe(sub, cancellation_token=bridge.async_token):
            ...

        # From sync code
        sync_token.cancel()  # Will trigger async cancellation

        await bridge.stop_monitoring()
    """

    def __init__(
        self,
        sync_token: CancellationToken,
        poll_interval_seconds: float = 0.1,
    ) -> None:
        """Initialize the bridge.

        Args:
            sync_token: The sync CancellationToken to monitor.
            poll_interval_seconds: How often to check sync token (default 0.1s).
        """
        self._sync_token = sync_token
        self._async_token = AsyncCancellationToken()
        self._poll_interval = poll_interval_seconds
        self._monitor_task: asyncio.Task[None] | None = None

    @property
    def async_token(self) -> AsyncCancellationToken:
        """Get the async cancellation token.

        Returns:
            The AsyncCancellationToken that will be cancelled when the
            sync token is cancelled.
        """
        return self._async_token

    async def start_monitoring(self) -> None:
        """Start monitoring sync token for cancellation.

        Creates a background task that polls the sync token and cancels
        the async token when the sync token is cancelled.
        """
        if self._monitor_task is not None:
            return  # Already monitoring

        async def _monitor() -> None:
            while not self._sync_token.is_cancelled:
                await asyncio.sleep(self._poll_interval)
            self._async_token.cancel()

        self._monitor_task = asyncio.create_task(_monitor())

    async def stop_monitoring(self) -> None:
        """Stop the monitoring task.

        Cancels the background monitoring task if it's running.
        """
        if self._monitor_task:
            self._monitor_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._monitor_task
            self._monitor_task = None

    async def __aenter__(self) -> CancellationTokenBridge:
        """Async context manager entry."""
        await self.start_monitoring()
        return self

    async def __aexit__(
        self,
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: object | None,
    ) -> None:
        """Async context manager exit."""
        await self.stop_monitoring()
