"""Token-bucket rate limiters for sync threads and async tasks."""

from __future__ import annotations

import asyncio
import threading
import time


class RateLimiter:
    """Simple token-bucket rate limiter (sync, for threads).

    Call wait() before each send operation to enforce rate limits.
    """

    def __init__(self, rate: float) -> None:
        """Initialize with target rate in operations per second."""
        self._rate = rate
        self._interval = 1.0 / rate if rate > 0 else 0.0
        self._next_time = time.monotonic()
        self._lock = threading.Lock()

    def wait(self, stop_event: threading.Event) -> bool:
        """Wait until the next token is available.

        Returns False if stop_event was set (caller should exit).
        """
        if self._rate <= 0:
            return True

        with self._lock:
            now = time.monotonic()
            if now < self._next_time:
                delay = self._next_time - now
                self._next_time += self._interval
            else:
                self._next_time = now + self._interval
                delay = 0.0

        if delay > 0:
            if stop_event.wait(timeout=delay):
                return False
        return True


class AsyncRateLimiter:
    """Async token-bucket rate limiter for asyncio tasks.

    No lock needed — single-threaded cooperative scheduling.
    """

    def __init__(self, rate: float) -> None:
        self._rate = rate
        self._interval = 1.0 / rate if rate > 0 else 0.0
        self._next_time = time.monotonic()

    async def wait(self, stop_event: asyncio.Event) -> bool:
        """Wait until the next token is available.

        Returns False if stop_event was set (caller should exit).
        """
        if self._rate <= 0:
            return True

        now = time.monotonic()
        if now < self._next_time:
            delay = self._next_time - now
            self._next_time += self._interval
        else:
            self._next_time = now + self._interval
            delay = 0.0

        if delay > 0:
            # Use simple sleep instead of wait_for(event.wait(), timeout)
            # to avoid creating Task + timeout handler per call.
            # Check stop_event before/after — sub-interval latency is fine.
            if stop_event.is_set():
                return False
            await asyncio.sleep(delay)

        return not stop_event.is_set()
