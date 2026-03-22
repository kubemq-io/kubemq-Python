"""Peak rate tracking and latency accumulation."""

from __future__ import annotations

import threading
import time

from hdrh.histogram import HdrHistogram


class PeakRateTracker:
    """Sliding-window peak rate tracker.

    Uses 10 one-second buckets. Call record() on each event,
    advance() every second, and peak() to get the highest observed rate.
    """

    WINDOW_SIZE = 10

    def __init__(self) -> None:
        self._buckets = [0] * self.WINDOW_SIZE
        self._idx = 0
        self._peak = 0.0
        self._last_tick = time.monotonic()
        self._lock = threading.Lock()

    def record(self) -> None:
        """Record one event in the current bucket."""
        with self._lock:
            self._buckets[self._idx] += 1

    def advance(self) -> None:
        """Advance to next bucket, compute 10-second average, update peak."""
        with self._lock:
            total = sum(self._buckets)
            avg = total / self.WINDOW_SIZE
            if avg > self._peak:
                self._peak = avg
            self._idx = (self._idx + 1) % self.WINDOW_SIZE
            self._buckets[self._idx] = 0
            self._last_tick = time.monotonic()

    def peak(self) -> float:
        """Return the peak observed rate (events/second)."""
        with self._lock:
            return self._peak

    def reset(self) -> None:
        """Reset all state."""
        with self._lock:
            self._buckets = [0] * self.WINDOW_SIZE
            self._peak = 0.0


class SlidingRateTracker:
    """30-second sliding window rate tracker.

    Uses 30 one-second buckets to compute a rolling average rate (events/sec).
    Call record() on each event, advance() every second from the same
    periodic ticker that drives PeakRateTracker.
    """

    WINDOW_SIZE = 30

    def __init__(self) -> None:
        self._buckets = [0] * self.WINDOW_SIZE
        self._idx = 0
        self._filled = 0
        self._lock = threading.Lock()

    def record(self) -> None:
        with self._lock:
            self._buckets[self._idx] += 1

    def advance(self) -> None:
        with self._lock:
            if self._filled < self.WINDOW_SIZE:
                self._filled += 1
            self._idx = (self._idx + 1) % self.WINDOW_SIZE
            self._buckets[self._idx] = 0

    def rate(self) -> float:
        with self._lock:
            if self._filled == 0:
                return 0.0
            window = min(self._filled, self.WINDOW_SIZE)
            return sum(self._buckets) / window

    def reset(self) -> None:
        with self._lock:
            self._buckets = [0] * self.WINDOW_SIZE
            self._idx = 0
            self._filled = 0


class LatencyAccumulator:
    """Thread-safe HdrHistogram-based latency accumulator.

    Records latencies in microseconds, reports in milliseconds.
    Range: 1 microsecond to 60 seconds, 3 significant digits.
    """

    def __init__(self) -> None:
        # 1 µs to 60s, 3 significant digits
        self._hist = HdrHistogram(1, 60_000_000, 3)
        self._lock = threading.Lock()

    def record(self, duration_seconds: float) -> None:
        """Record a latency duration in seconds."""
        micros = int(duration_seconds * 1_000_000)
        if micros < 1:
            micros = 1
        if micros > 60_000_000:
            micros = 60_000_000
        with self._lock:
            self._hist.record_value(micros)

    def percentile_ms(self, p: float) -> float:
        """Get percentile value in milliseconds."""
        with self._lock:
            return self._hist.get_value_at_percentile(p) / 1000.0

    def count(self) -> int:
        """Get total number of recorded values."""
        with self._lock:
            return self._hist.total_count

    def reset(self) -> None:
        """Reset the histogram."""
        with self._lock:
            self._hist.reset()
