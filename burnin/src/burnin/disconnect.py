"""Forced disconnect manager: close clients, wait, recreate.

Async version: runs as asyncio.Task instead of threading.Thread.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Protocol

from burnin import metrics_collector as mc

logger = logging.getLogger("burnin")


class AsyncClientRecreator(Protocol):
    """Protocol for engine's async close/recreate interface."""

    async def close_clients_async(self) -> None: ...
    async def recreate_clients_async(self) -> None: ...


class AsyncDisconnectManager:
    """Periodically forces client disconnection to test resilience (async)."""

    def __init__(
        self,
        interval_seconds: float,
        duration_seconds: float,
        recreator: AsyncClientRecreator,
    ) -> None:
        self._interval = interval_seconds
        self._duration = duration_seconds
        self._recreator = recreator
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None

    @property
    def enabled(self) -> bool:
        return self._interval > 0

    def start(self) -> None:
        """Start the disconnect manager as an asyncio task."""
        if not self.enabled:
            return
        self._task = asyncio.create_task(self._run(), name="disconnect-manager")

    def stop(self) -> None:
        """Signal the disconnect manager to stop."""
        self._stop.set()
        if self._task:
            self._task.cancel()

    async def _run(self) -> None:
        """Main loop: wait interval, force disconnect, wait duration, recreate."""
        try:
            while not self._stop.is_set():
                # Wait for interval
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=self._interval)
                    break  # stop was set
                except asyncio.TimeoutError:
                    pass

                logger.info("forced disconnect: closing clients")
                mc.inc_forced_disconnects()

                try:
                    await self._recreator.close_clients_async()
                except Exception as e:
                    logger.error("forced disconnect close error: %s", e)

                # Wait for disconnected duration
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=self._duration)
                    # If stopped during disconnect, try to recreate anyway
                    try:
                        await self._recreator.recreate_clients_async()
                    except Exception:
                        pass
                    break
                except asyncio.TimeoutError:
                    pass

                logger.info("forced disconnect: recreating clients")
                try:
                    await self._recreator.recreate_clients_async()
                except Exception as e:
                    logger.error("forced disconnect recreate error: %s", e)
        except asyncio.CancelledError:
            pass

        logger.info("disconnect manager stopped")
