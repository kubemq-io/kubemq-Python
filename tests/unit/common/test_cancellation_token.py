"""Unit tests for kubemq.common.cancellation_token module.

Tests for CancellationToken class.
"""

from __future__ import annotations

import threading
import time

from kubemq.common.cancellation_token import CancellationToken


class TestCancellationToken:
    """Tests for CancellationToken class."""

    def test_initial_state_is_not_cancelled(self):
        """Test that initial state is not cancelled."""
        token = CancellationToken()

        assert token.is_cancelled is False
        assert token.is_set() is False

    def test_cancel_sets_cancelled(self):
        """Test cancel sets the cancelled state."""
        token = CancellationToken()

        token.cancel()

        assert token.is_cancelled is True
        assert token.is_set() is True

    def test_cancel_is_idempotent(self):
        """Test cancel can be called multiple times safely."""
        token = CancellationToken()

        token.cancel()
        token.cancel()
        token.cancel()

        assert token.is_cancelled is True

    def test_thread_safety(self):
        """Test cancellation token is thread-safe."""
        token = CancellationToken()
        results = []

        def worker():
            results.append(token.is_cancelled)
            time.sleep(0.01)
            results.append(token.is_cancelled)

        # Start threads that check token state
        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()

        # Cancel after threads start
        time.sleep(0.005)
        token.cancel()

        for t in threads:
            t.join()

        # At least some results should be True after cancellation
        assert True in results

    def test_has_threading_event(self):
        """Test token has threading.Event for synchronization."""
        token = CancellationToken()

        assert isinstance(token.event, threading.Event)

    def test_is_set_is_deprecated_equivalent(self):
        """Test is_set returns same value as is_cancelled."""
        token = CancellationToken()

        assert token.is_set() == token.is_cancelled

        token.cancel()

        assert token.is_set() == token.is_cancelled
