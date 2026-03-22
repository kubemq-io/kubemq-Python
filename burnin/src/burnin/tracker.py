"""Bitset-based sequence tracker for detecting loss, duplication, and reordering."""

from __future__ import annotations

import threading
from dataclasses import dataclass, field


@dataclass
class _ProducerState:
    """Per-producer tracking state with sliding window anchored at high_contiguous."""

    high_contiguous: int = 0
    window: list[int] = field(default_factory=list)
    window_size: int = 0
    received: int = 0
    duplicates: int = 0
    out_of_order: int = 0
    confirmed_lost: int = 0
    last_reported_lost: int = 0
    last_seen: int = 0
    initialized: bool = False


class Tracker:
    """Thread-safe sequence tracker with bitset-based sliding reorder window.

    The window is anchored at high_contiguous+1 and slides forward as
    contiguous sequences are confirmed. Gaps pushed out of the window
    are counted as confirmed lost.
    """

    def __init__(self, reorder_window: int = 10_000) -> None:
        self._reorder_window = reorder_window
        self._producers: dict[str, _ProducerState] = {}
        self._lock = threading.Lock()

    def record(self, producer_id: str, seq: int) -> tuple[bool, bool]:
        """Record a received sequence. Returns (is_duplicate, is_out_of_order)."""
        with self._lock:
            state = self._producers.get(producer_id)
            if state is None:
                state = _ProducerState(
                    window_size=self._reorder_window,
                    window=self._make_window(self._reorder_window),
                )
                self._producers[producer_id] = state

            if not state.initialized:
                state.high_contiguous = seq
                state.last_seen = seq
                state.initialized = True
                state.received += 1
                return False, False

            state.received += 1

            # Already fully confirmed — duplicate
            if seq <= state.high_contiguous:
                state.duplicates += 1
                return True, False

            # Position relative to window start (high_contiguous+1)
            offset = seq - state.high_contiguous - 1

            # Beyond window — need to slide
            if offset >= state.window_size:
                self._slide_to(state, seq)
                offset = seq - state.high_contiguous - 1

            # Check duplicate in window
            if self._get_bit(state.window, offset):
                state.duplicates += 1
                return True, False

            # Mark received
            self._set_bit(state.window, offset)

            # Out of order check
            is_ooo = seq < state.last_seen
            if is_ooo:
                state.out_of_order += 1
            state.last_seen = max(state.last_seen, seq)

            # Advance high_contiguous through contiguous set bits
            while self._get_bit(state.window, 0):
                state.high_contiguous += 1
                self._shift_left_one(state.window)

            return False, is_ooo

    def detect_gaps(self) -> dict[str, int]:
        """Return delta of newly confirmed lost messages per producer."""
        result: dict[str, int] = {}
        with self._lock:
            for pid, state in self._producers.items():
                if not state.initialized:
                    continue
                delta = state.confirmed_lost - state.last_reported_lost
                if delta > 0:
                    result[pid] = delta
                    state.last_reported_lost = state.confirmed_lost
        return result

    def total_received(self) -> int:
        with self._lock:
            return sum(s.received for s in self._producers.values())

    def total_duplicates(self) -> int:
        with self._lock:
            return sum(s.duplicates for s in self._producers.values())

    def total_out_of_order(self) -> int:
        with self._lock:
            return sum(s.out_of_order for s in self._producers.values())

    def total_lost(self) -> int:
        with self._lock:
            return sum(s.confirmed_lost for s in self._producers.values())

    def reset(self) -> None:
        with self._lock:
            self._producers.clear()

    def _slide_to(self, state: _ProducerState, new_seq: int) -> None:
        """Slide window so new_seq fits. Count unset bits as lost."""
        target_hc = new_seq - state.window_size
        if target_hc <= state.high_contiguous:
            return

        # Count and shift out bits from window start up to target
        advance = target_hc - state.high_contiguous
        for _ in range(advance):
            if not self._get_bit(state.window, 0):
                state.confirmed_lost += 1
            state.high_contiguous += 1
            self._shift_left_one(state.window)

    @staticmethod
    def _make_window(size: int) -> list[int]:
        return [0] * ((size + 63) // 64)

    @staticmethod
    def _set_bit(window: list[int], offset: int) -> None:
        word = offset // 64
        bit = offset % 64
        if 0 <= word < len(window):
            window[word] |= 1 << bit

    @staticmethod
    def _get_bit(window: list[int], offset: int) -> bool:
        word = offset // 64
        bit = offset % 64
        if 0 <= word < len(window):
            return bool(window[word] & (1 << bit))
        return False

    @staticmethod
    def _shift_left_one(window: list[int]) -> None:
        """Shift entire bitset left by 1 bit (drop bit 0, shift everything down)."""
        carry = 0
        for i in range(len(window) - 1, -1, -1):
            new_carry = window[i] & 1
            window[i] = (window[i] >> 1) | (carry << 63)
            carry = new_carry
