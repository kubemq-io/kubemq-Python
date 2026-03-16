"""Tests for ConnectionStateManager."""

from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest

from kubemq._internal.transport.state import ConnectionStateManager
from kubemq.core.types import ConnectionState


class TestStateTransitions:
    def test_initial_state_is_idle(self):
        mgr = ConnectionStateManager()
        assert mgr.state == ConnectionState.IDLE

    def test_valid_transition_idle_to_connecting(self):
        mgr = ConnectionStateManager()
        assert mgr.transition_to(ConnectionState.CONNECTING) is True
        assert mgr.state == ConnectionState.CONNECTING

    def test_valid_transition_connecting_to_ready(self):
        mgr = ConnectionStateManager()
        mgr.transition_to(ConnectionState.CONNECTING)
        assert mgr.transition_to(ConnectionState.READY) is True
        assert mgr.state == ConnectionState.READY

    def test_valid_transition_ready_to_reconnecting(self):
        mgr = ConnectionStateManager()
        mgr.transition_to(ConnectionState.CONNECTING)
        mgr.transition_to(ConnectionState.READY)
        assert mgr.transition_to(ConnectionState.RECONNECTING) is True
        assert mgr.state == ConnectionState.RECONNECTING

    def test_valid_transition_reconnecting_to_ready(self):
        mgr = ConnectionStateManager()
        mgr.transition_to(ConnectionState.CONNECTING)
        mgr.transition_to(ConnectionState.READY)
        mgr.transition_to(ConnectionState.RECONNECTING)
        assert mgr.transition_to(ConnectionState.READY) is True
        assert mgr.state == ConnectionState.READY

    def test_invalid_transition_idle_to_ready(self):
        mgr = ConnectionStateManager()
        assert mgr.transition_to(ConnectionState.READY) is False
        assert mgr.state == ConnectionState.IDLE

    def test_invalid_transition_idle_to_reconnecting(self):
        mgr = ConnectionStateManager()
        assert mgr.transition_to(ConnectionState.RECONNECTING) is False

    def test_closed_is_terminal(self):
        mgr = ConnectionStateManager()
        mgr.transition_to(ConnectionState.CLOSED)
        assert mgr.state == ConnectionState.CLOSED
        assert mgr.transition_to(ConnectionState.IDLE) is False
        assert mgr.transition_to(ConnectionState.CONNECTING) is False
        assert mgr.state == ConnectionState.CLOSED

    def test_any_state_can_go_to_closed(self):
        for intermediate in [ConnectionState.CONNECTING, ConnectionState.READY]:
            mgr = ConnectionStateManager()
            mgr.transition_to(ConnectionState.CONNECTING)
            if intermediate == ConnectionState.READY:
                mgr.transition_to(ConnectionState.READY)
            assert mgr.transition_to(ConnectionState.CLOSED) is True


class TestCallbackRegistration:
    def test_on_connected_fires(self):
        mgr = ConnectionStateManager()
        called = []
        mgr.on_connected(lambda: called.append("connected"))

        mgr.transition_to(ConnectionState.CONNECTING)
        mgr.transition_to(ConnectionState.READY)

        time.sleep(0.1)
        mgr.close()
        assert "connected" in called

    def test_on_disconnected_fires_on_ready_to_reconnecting(self):
        mgr = ConnectionStateManager()
        called = []
        mgr.on_disconnected(lambda: called.append("disconnected"))

        mgr.transition_to(ConnectionState.CONNECTING)
        mgr.transition_to(ConnectionState.READY)
        mgr.transition_to(ConnectionState.RECONNECTING)

        time.sleep(0.1)
        mgr.close()
        assert "disconnected" in called

    def test_on_reconnecting_fires(self):
        mgr = ConnectionStateManager()
        called = []
        mgr.on_reconnecting(lambda: called.append("reconnecting"))

        mgr.transition_to(ConnectionState.CONNECTING)
        mgr.transition_to(ConnectionState.READY)
        mgr.transition_to(ConnectionState.RECONNECTING)

        time.sleep(0.1)
        mgr.close()
        assert "reconnecting" in called

    def test_on_reconnected_fires(self):
        mgr = ConnectionStateManager()
        called = []
        mgr.on_reconnected(lambda: called.append("reconnected"))

        mgr.transition_to(ConnectionState.CONNECTING)
        mgr.transition_to(ConnectionState.READY)
        mgr.transition_to(ConnectionState.RECONNECTING)
        mgr.transition_to(ConnectionState.READY)

        time.sleep(0.1)
        mgr.close()
        assert "reconnected" in called

    def test_on_closed_fires(self):
        mgr = ConnectionStateManager()
        called = []
        mgr.on_closed(lambda: called.append("closed"))

        mgr.transition_to(ConnectionState.CLOSED)

        time.sleep(0.1)
        mgr.close()
        assert "closed" in called

    def test_callback_exception_does_not_crash(self):
        mgr = ConnectionStateManager(logger=MagicMock())

        def bad_callback():
            raise RuntimeError("boom")

        mgr.on_connected(bad_callback)

        mgr.transition_to(ConnectionState.CONNECTING)
        mgr.transition_to(ConnectionState.READY)

        time.sleep(0.1)
        mgr.close()

    def test_multiple_callbacks_same_event(self):
        mgr = ConnectionStateManager()
        calls = []
        mgr.on_connected(lambda: calls.append("a"))
        mgr.on_connected(lambda: calls.append("b"))

        mgr.transition_to(ConnectionState.CONNECTING)
        mgr.transition_to(ConnectionState.READY)

        time.sleep(0.1)
        mgr.close()
        assert "a" in calls
        assert "b" in calls

    def test_ready_to_reconnecting_fires_both_disconnected_and_reconnecting(self):
        mgr = ConnectionStateManager()
        called = []
        mgr.on_disconnected(lambda: called.append("disconnected"))
        mgr.on_reconnecting(lambda: called.append("reconnecting"))

        mgr.transition_to(ConnectionState.CONNECTING)
        mgr.transition_to(ConnectionState.READY)
        mgr.transition_to(ConnectionState.RECONNECTING)

        time.sleep(0.1)
        mgr.close()
        assert "disconnected" in called
        assert "reconnecting" in called


class TestCloseManager:
    def test_close_without_callbacks(self):
        mgr = ConnectionStateManager()
        mgr.close()

    def test_close_idempotent(self):
        mgr = ConnectionStateManager()
        mgr.close()
        mgr.close()


class TestStateCallbackAsyncPath:
    """Tests for async callback path (lines 105-109)."""

    @pytest.mark.asyncio
    async def test_async_callback_fires(self):
        import asyncio

        mgr = ConnectionStateManager()
        called = []

        async def on_connected_async():
            called.append("async_connected")

        mgr.on_connected(on_connected_async)
        mgr.transition_to(ConnectionState.CONNECTING)
        mgr.transition_to(ConnectionState.READY)

        await asyncio.sleep(0.1)
        mgr.close()
        assert "async_connected" in called

    @pytest.mark.asyncio
    async def test_async_callback_exception_handled(self):
        import asyncio

        mgr = ConnectionStateManager(logger=MagicMock())

        async def bad_async():
            raise RuntimeError("async boom")

        mgr.on_connected(bad_async)
        mgr.transition_to(ConnectionState.CONNECTING)
        mgr.transition_to(ConnectionState.READY)

        await asyncio.sleep(0.1)
        mgr.close()


class TestTransitionCallbackKeys:
    """Tests for _transition_to_callback_keys (lines 152-156)."""

    def test_idle_to_connecting_no_callbacks(self):
        mgr = ConnectionStateManager()
        keys = mgr._transition_to_callback_keys(ConnectionState.IDLE, ConnectionState.CONNECTING)
        assert keys == []

    def test_connecting_to_reconnecting_fires_reconnecting(self):
        mgr = ConnectionStateManager()
        keys = mgr._transition_to_callback_keys(
            ConnectionState.CONNECTING, ConnectionState.RECONNECTING
        )
        assert "on_reconnecting" in keys

    def test_idle_to_closed_fires_on_closed(self):
        mgr = ConnectionStateManager()
        keys = mgr._transition_to_callback_keys(ConnectionState.IDLE, ConnectionState.CLOSED)
        assert "on_closed" in keys


class TestSyncCallbackSafe:
    """Tests for _safe_sync_callback (lines 162->exit)."""

    def test_sync_callback_exception_logged(self):
        logger = MagicMock()
        mgr = ConnectionStateManager(logger=logger)

        def bad_sync():
            raise ValueError("sync boom")

        mgr._safe_sync_callback(bad_sync)
        logger.error.assert_called()
