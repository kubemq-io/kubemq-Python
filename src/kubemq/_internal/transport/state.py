"""Connection state machine and callback dispatch.

Manages the connection lifecycle state transitions and fires registered
user callbacks asynchronously (never blocking the connection lifecycle).
"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from kubemq.core.types import ConnectionState

StateCallback = Callable[[], None]
AsyncStateCallback = Callable[[], Awaitable[None]]
AnyStateCallback = StateCallback | AsyncStateCallback


class ConnectionStateManager:
    """Manages connection state transitions and callback dispatch.

    Ensures:
    - State transitions are serialized (no concurrent transitions)
    - Callbacks never block the connection lifecycle
    - Invalid transitions are rejected silently (logged at DEBUG)
    """

    _VALID_TRANSITIONS: dict[ConnectionState, set[ConnectionState]] = {
        ConnectionState.IDLE: {ConnectionState.CONNECTING, ConnectionState.CLOSED},
        ConnectionState.CONNECTING: {
            ConnectionState.READY,
            ConnectionState.RECONNECTING,
            ConnectionState.CLOSED,
        },
        ConnectionState.READY: {ConnectionState.RECONNECTING, ConnectionState.CLOSED},
        ConnectionState.RECONNECTING: {ConnectionState.READY, ConnectionState.CLOSED},
        ConnectionState.CLOSED: set(),
    }

    def __init__(self, logger: Any = None) -> None:
        self._state = ConnectionState.IDLE
        self._lock = threading.Lock()
        self._logger = logger or logging.getLogger("kubemq.state")
        self._callback_executor: ThreadPoolExecutor | None = None
        self._callbacks: dict[str, list[AnyStateCallback]] = {
            "on_connected": [],
            "on_disconnected": [],
            "on_reconnecting": [],
            "on_reconnected": [],
            "on_closed": [],
        }

    @property
    def state(self) -> ConnectionState:
        with self._lock:
            return self._state

    def transition_to(self, new_state: ConnectionState) -> bool:
        """Attempt state transition. Returns True if transition was valid.

        Fires appropriate callbacks after successful transition.
        Invalid transitions are logged at DEBUG and return False.
        """
        with self._lock:
            old_state = self._state
            if new_state not in self._VALID_TRANSITIONS.get(old_state, set()):
                if self._logger:
                    self._logger.debug(
                        "Invalid state transition from_state=%s to_state=%s",
                        old_state.value,
                        new_state.value,
                    )
                return False
            self._state = new_state

        if self._logger:
            self._logger.info(
                "Connection state changed from_state=%s to_state=%s",
                old_state.value,
                new_state.value,
            )

        self._fire_callbacks(old_state, new_state)
        return True

    def _fire_callbacks(self, old_state: ConnectionState, new_state: ConnectionState) -> None:
        """Fire callbacks for the given transition.

        Callbacks are invoked asynchronously — never block the caller.
        Sync callbacks run in a daemon thread. Async callbacks are scheduled
        as tasks in the current event loop (if one exists).
        """
        callback_keys = self._transition_to_callback_keys(old_state, new_state)
        if not callback_keys:
            return

        for callback_key in callback_keys:
            for cb in self._callbacks.get(callback_key, []):
                if asyncio.iscoroutinefunction(cb):
                    try:
                        loop = asyncio.get_running_loop()
                        async_cb: AsyncStateCallback = cb  # narrowed by iscoroutinefunction
                        loop.create_task(self._safe_async_callback(async_cb))
                    except RuntimeError:
                        pass
                else:
                    sync_cb: StateCallback = cb  # type: ignore[assignment]  # narrowed by iscoroutinefunction check
                    self._get_callback_executor().submit(self._safe_sync_callback, sync_cb)

    def _transition_to_callback_keys(
        self, old_state: ConnectionState, new_state: ConnectionState
    ) -> list[str]:
        """Return all callback keys to fire for a given state transition.

        READY->RECONNECTING fires both on_disconnected and on_reconnecting.
        """
        keys: list[str] = []
        if old_state == ConnectionState.READY and new_state == ConnectionState.RECONNECTING:
            keys.append("on_disconnected")
        if new_state == ConnectionState.RECONNECTING:
            keys.append("on_reconnecting")
        elif new_state == ConnectionState.READY and old_state == ConnectionState.CONNECTING:
            keys.append("on_connected")
        elif new_state == ConnectionState.READY and old_state == ConnectionState.RECONNECTING:
            keys.append("on_reconnected")
        if new_state == ConnectionState.CLOSED:
            keys.append("on_closed")
        return keys

    def _get_callback_executor(self) -> ThreadPoolExecutor:
        """Lazy-init a single-worker thread pool for sync callback dispatch."""
        if self._callback_executor is None:
            self._callback_executor = ThreadPoolExecutor(
                max_workers=1, thread_name_prefix="kubemq-state-cb"
            )
        return self._callback_executor

    def close(self) -> None:
        """Shut down the callback executor.

        Called after the CLOSED state transition so that on_closed
        callbacks complete before the executor is torn down.
        """
        if self._callback_executor is not None:
            self._callback_executor.shutdown(wait=True, cancel_futures=False)
            self._callback_executor = None

    async def _safe_async_callback(self, cb: AsyncStateCallback) -> None:
        try:
            await cb()
        except Exception:
            if self._logger:
                self._logger.error("State callback raised an exception", exc_info=True)

    def _safe_sync_callback(self, cb: StateCallback) -> None:
        try:
            cb()
        except Exception:
            if self._logger:
                self._logger.error("State callback raised an exception", exc_info=True)

    def on_connected(self, callback: AnyStateCallback) -> None:
        """Register callback for CONNECTING -> READY transition."""
        self._callbacks["on_connected"].append(callback)

    def on_disconnected(self, callback: AnyStateCallback) -> None:
        """Register callback for READY -> RECONNECTING transition."""
        self._callbacks["on_disconnected"].append(callback)

    def on_reconnecting(self, callback: AnyStateCallback) -> None:
        """Register callback for any -> RECONNECTING transition."""
        self._callbacks["on_reconnecting"].append(callback)

    def on_reconnected(self, callback: AnyStateCallback) -> None:
        """Register callback for RECONNECTING -> READY transition."""
        self._callbacks["on_reconnected"].append(callback)

    def on_closed(self, callback: AnyStateCallback) -> None:
        """Register callback for any -> CLOSED transition."""
        self._callbacks["on_closed"].append(callback)
