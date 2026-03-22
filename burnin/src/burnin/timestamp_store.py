"""Send timestamp store for end-to-end latency measurement."""

from __future__ import annotations

import threading
import time


class SendTimestampStore:
    """Thread-safe mapping of (producer_id, seq) -> monotonic timestamp.

    Used to measure end-to-end latency: time from send to receive.
    """

    def __init__(self) -> None:
        self._store: dict[str, float] = {}
        self._lock = threading.Lock()

    def store(self, producer_id: str, seq: int, ts: float | None = None) -> None:
        """Store send timestamp for a message."""
        key = f"{producer_id}:{seq}"
        with self._lock:
            self._store[key] = ts if ts is not None else time.monotonic()

    def load_and_delete(self, producer_id: str, seq: int) -> float | None:
        """Load and remove timestamp for a message. Returns None if not found."""
        key = f"{producer_id}:{seq}"
        with self._lock:
            return self._store.pop(key, None)

    def purge(self, max_age: float) -> int:
        """Remove entries older than max_age seconds. Returns count removed."""
        cutoff = time.monotonic() - max_age
        removed = 0
        with self._lock:
            keys_to_remove = [k for k, v in self._store.items() if v < cutoff]
            for k in keys_to_remove:
                del self._store[k]
                removed += 1
        return removed

    def __len__(self) -> int:
        with self._lock:
            return len(self._store)
