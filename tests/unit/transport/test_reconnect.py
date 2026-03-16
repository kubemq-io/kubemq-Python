"""Tests for kubemq._internal.transport.reconnect module.

Covers REQ-CONN-1 (Auto-Reconnection with Buffering).
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from kubemq._internal.retry import BackoffCalculator
from kubemq._internal.transport.reconnect import (
    ReconnectConfig,
    ReconnectionManager,
    _AsyncBoundedByteBuffer,
    _BoundedByteBuffer,
)
from kubemq.core.config import JitterType, RetryPolicy
from kubemq.core.exceptions import KubeMQBufferFullError

# -----------------------------------------------------------------------
# _BoundedByteBuffer (sync, thread-safe)
# -----------------------------------------------------------------------


class TestBoundedByteBuffer:
    def test_put_and_drain(self):
        buf = _BoundedByteBuffer(max_bytes=1024)
        buf.put(b"hello", {"id": "1"})
        buf.put(b"world", {"id": "2"})
        assert buf.count == 2
        assert buf.current_bytes == 10

        items = buf.drain_all()
        assert len(items) == 2
        assert items[0] == (b"hello", {"id": "1"})
        assert items[1] == (b"world", {"id": "2"})
        assert buf.count == 0
        assert buf.current_bytes == 0

    def test_put_overflow_raises(self):
        buf = _BoundedByteBuffer(max_bytes=10)
        buf.put(b"12345")
        with pytest.raises(KubeMQBufferFullError) as exc_info:
            buf.put(b"123456")
        assert exc_info.value.buffer_size == 10

    def test_zero_buffer_rejects_all(self):
        """Q2 decision: reconnect_buffer_size=0 means no buffering."""
        buf = _BoundedByteBuffer(max_bytes=0)
        with pytest.raises(KubeMQBufferFullError):
            buf.put(b"x")

    def test_discard_all(self):
        buf = _BoundedByteBuffer(max_bytes=1024)
        buf.put(b"aaa")
        buf.put(b"bbb")
        count = buf.discard_all()
        assert count == 2
        assert buf.count == 0
        assert buf.current_bytes == 0

    def test_fifo_order(self):
        buf = _BoundedByteBuffer(max_bytes=1024)
        for i in range(5):
            buf.put(f"msg-{i}".encode())
        items = buf.drain_all()
        for i, (data, _meta) in enumerate(items):
            assert data == f"msg-{i}".encode()

    def test_put_with_none_metadata(self):
        buf = _BoundedByteBuffer(max_bytes=1024)
        buf.put(b"data")
        items = buf.drain_all()
        assert items[0][1] == {}


# -----------------------------------------------------------------------
# _AsyncBoundedByteBuffer
# -----------------------------------------------------------------------


class TestAsyncBoundedByteBuffer:
    async def test_put_and_drain(self):
        buf = _AsyncBoundedByteBuffer(max_bytes=1024)
        await buf.put(b"hello", {"id": "1"})
        await buf.put(b"world", {"id": "2"})
        assert buf.count == 2
        assert buf.current_bytes == 10

        items = await buf.drain_all()
        assert len(items) == 2
        assert items[0] == (b"hello", {"id": "1"})
        assert items[1] == (b"world", {"id": "2"})
        assert buf.count == 0

    async def test_put_overflow_error_mode(self):
        buf = _AsyncBoundedByteBuffer(max_bytes=10, overflow_mode="error")
        await buf.put(b"12345")
        with pytest.raises(KubeMQBufferFullError):
            await buf.put(b"123456")

    async def test_zero_buffer_rejects_all(self):
        """Q2 decision: reconnect_buffer_size=0 means no buffering."""
        buf = _AsyncBoundedByteBuffer(max_bytes=0)
        with pytest.raises(KubeMQBufferFullError):
            await buf.put(b"x")

    async def test_discard_all(self):
        buf = _AsyncBoundedByteBuffer(max_bytes=1024)
        await buf.put(b"aaa")
        await buf.put(b"bbb")
        count = await buf.discard_all()
        assert count == 2
        assert buf.count == 0

    async def test_fifo_order(self):
        buf = _AsyncBoundedByteBuffer(max_bytes=1024)
        for i in range(5):
            await buf.put(f"msg-{i}".encode())
        items = await buf.drain_all()
        for i, (data, _meta) in enumerate(items):
            assert data == f"msg-{i}".encode()

    async def test_block_mode_waits_for_space(self):
        buf = _AsyncBoundedByteBuffer(max_bytes=5, overflow_mode="block")
        await buf.put(b"12345")

        async def delayed_drain():
            await asyncio.sleep(0.05)
            await buf.drain_all()

        drain_task = asyncio.create_task(delayed_drain())
        await asyncio.wait_for(buf.put(b"x"), timeout=1.0)
        await drain_task
        assert buf.count == 1


# -----------------------------------------------------------------------
# ReconnectConfig
# -----------------------------------------------------------------------


class TestReconnectConfig:
    def test_defaults(self):
        cfg = ReconnectConfig()
        assert cfg.max_reconnect_attempts == -1
        assert cfg.initial_reconnect_delay_ms == 500
        assert cfg.max_reconnect_delay_ms == 30_000
        assert cfg.reconnect_backoff_multiplier == 2.0
        assert cfg.reconnect_buffer_size == 8 * 1024 * 1024
        assert cfg.auto_reconnect is True

    def test_custom_values(self):
        cfg = ReconnectConfig(
            max_reconnect_attempts=5,
            reconnect_buffer_size=0,
        )
        assert cfg.max_reconnect_attempts == 5
        assert cfg.reconnect_buffer_size == 0


# -----------------------------------------------------------------------
# ReconnectionManager
# -----------------------------------------------------------------------


def _make_backoff() -> BackoffCalculator:
    return BackoffCalculator(
        RetryPolicy(
            initial_backoff_ms=50,
            max_backoff_ms=1000,
            backoff_multiplier=2.0,
            jitter=JitterType.NONE,
        )
    )


class TestReconnectionManager:
    async def test_successful_reconnection(self):
        config = ReconnectConfig(
            reconnect_buffer_size=1024,
            initial_reconnect_delay_ms=50,
        )
        connect_fn = AsyncMock()
        mgr = ReconnectionManager(config, _make_backoff())

        await mgr.start_reconnection(connect_fn)
        await asyncio.sleep(0.2)

        connect_fn.assert_called_once()
        assert not mgr.is_reconnecting

    async def test_reconnection_retries_on_failure(self):
        config = ReconnectConfig(
            max_reconnect_attempts=3,
            reconnect_buffer_size=1024,
            initial_reconnect_delay_ms=50,
        )
        connect_fn = AsyncMock(side_effect=[Exception("fail"), Exception("fail"), None])
        mgr = ReconnectionManager(config, _make_backoff())

        await mgr.start_reconnection(connect_fn)
        # Wait enough for 3 attempts with short delays
        await asyncio.sleep(1.0)

        assert connect_fn.call_count == 3
        assert not mgr.is_reconnecting

    async def test_max_attempts_exhausted(self):
        config = ReconnectConfig(
            max_reconnect_attempts=2,
            reconnect_buffer_size=1024,
            initial_reconnect_delay_ms=50,
        )
        connect_fn = AsyncMock(side_effect=Exception("fail"))
        mgr = ReconnectionManager(config, _make_backoff())

        await mgr.start_reconnection(connect_fn)
        await asyncio.sleep(0.5)

        assert connect_fn.call_count == 2
        assert not mgr.is_reconnecting

    async def test_cancel_stops_reconnection(self):
        config = ReconnectConfig(
            reconnect_buffer_size=1024,
            initial_reconnect_delay_ms=200,
        )
        connect_fn = AsyncMock(side_effect=Exception("fail"))
        mgr = ReconnectionManager(config, _make_backoff())

        await mgr.start_reconnection(connect_fn)
        await asyncio.sleep(0.05)
        await mgr.cancel()

        assert not mgr.is_reconnecting

    async def test_buffer_during_reconnection(self):
        config = ReconnectConfig(reconnect_buffer_size=1024)
        mgr = ReconnectionManager(config, _make_backoff())

        await mgr.buffer_message(b"msg1", {"id": "1"})
        await mgr.buffer_message(b"msg2", {"id": "2"})

        assert mgr.buffer.count == 2

    async def test_buffer_full_raises(self):
        config = ReconnectConfig(reconnect_buffer_size=5)
        mgr = ReconnectionManager(config, _make_backoff())

        await mgr.buffer_message(b"12345")
        with pytest.raises(KubeMQBufferFullError):
            await mgr.buffer_message(b"x")

    async def test_cancel_discards_buffer_and_fires_callback(self):
        drain_cb = MagicMock()
        config = ReconnectConfig(reconnect_buffer_size=1024)
        mgr = ReconnectionManager(config, _make_backoff(), on_buffer_drain=drain_cb)

        await mgr.buffer_message(b"msg1")
        await mgr.buffer_message(b"msg2")
        await mgr.cancel()

        drain_cb.assert_called_once_with(2)
        assert mgr.buffer.count == 0

    async def test_cancel_with_async_callback(self):
        drain_cb = AsyncMock()
        config = ReconnectConfig(reconnect_buffer_size=1024)
        mgr = ReconnectionManager(config, _make_backoff(), on_buffer_drain=drain_cb)

        await mgr.buffer_message(b"msg1")
        await mgr.cancel()

        drain_cb.assert_called_once_with(1)

    async def test_duplicate_start_is_noop(self):
        config = ReconnectConfig(
            reconnect_buffer_size=1024,
            initial_reconnect_delay_ms=500,
        )
        connect_fn = AsyncMock(side_effect=Exception("fail"))
        mgr = ReconnectionManager(config, _make_backoff())

        await mgr.start_reconnection(connect_fn)
        assert mgr.is_reconnecting
        await mgr.start_reconnection(connect_fn)  # should be noop
        await mgr.cancel()

    async def test_subscription_recovery_called(self):
        config = ReconnectConfig(
            reconnect_buffer_size=1024,
            initial_reconnect_delay_ms=50,
        )
        connect_fn = AsyncMock()
        recovery_fn = AsyncMock()
        mgr = ReconnectionManager(config, _make_backoff())

        await mgr.start_reconnection(connect_fn, recovery_fn)
        await asyncio.sleep(0.2)

        recovery_fn.assert_called_once()

    async def test_buffer_drained_on_success(self):
        config = ReconnectConfig(
            reconnect_buffer_size=1024,
            initial_reconnect_delay_ms=50,
        )
        connect_fn = AsyncMock()
        mgr = ReconnectionManager(config, _make_backoff())

        await mgr.buffer_message(b"buffered-msg")
        await mgr.start_reconnection(connect_fn)
        await asyncio.sleep(0.2)

        assert mgr.buffer.count == 0


# -----------------------------------------------------------------------
# CONN-4 new exception types
# -----------------------------------------------------------------------


class TestClientClosedError:
    def test_import(self):
        from kubemq.core.exceptions import KubeMQClientClosedError

        err = KubeMQClientClosedError()
        assert str(err) is not None
        assert err.is_retryable is False

    def test_default_message(self):
        from kubemq.core.exceptions import KubeMQClientClosedError

        err = KubeMQClientClosedError()
        assert "closed" in str(err).lower()

    def test_error_code(self):
        from kubemq.core.exceptions import ErrorCode, KubeMQClientClosedError

        err = KubeMQClientClosedError()
        assert err.code == ErrorCode.CLIENT_CLOSED

    def test_custom_message(self):
        from kubemq.core.exceptions import KubeMQClientClosedError

        err = KubeMQClientClosedError("Transport is closing")
        assert "Transport is closing" in str(err)


class TestConnectionNotReadyError:
    def test_import(self):
        from kubemq.core.exceptions import KubeMQConnectionNotReadyError

        err = KubeMQConnectionNotReadyError()
        assert str(err) is not None
        assert err.is_retryable is False

    def test_default_message(self):
        from kubemq.core.exceptions import KubeMQConnectionNotReadyError

        err = KubeMQConnectionNotReadyError()
        assert "not ready" in str(err).lower()

    def test_error_code(self):
        from kubemq.core.exceptions import (
            ErrorCode,
            KubeMQConnectionNotReadyError,
        )

        err = KubeMQConnectionNotReadyError()
        assert err.code == ErrorCode.CONNECTION_NOT_READY

    def test_custom_message(self):
        from kubemq.core.exceptions import KubeMQConnectionNotReadyError

        err = KubeMQConnectionNotReadyError(
            "Connection state is RECONNECTING, wait_for_ready=False"
        )
        assert "RECONNECTING" in str(err)


class TestReconnectionManagerBufferDrainOnSuccess:
    """Tests for buffer drain callback on successful reconnection (lines 301-307)."""

    async def test_sync_buffer_drain_callback_on_success(self):
        drain_cb = MagicMock()
        config = ReconnectConfig(
            reconnect_buffer_size=1024,
            initial_reconnect_delay_ms=50,
        )
        connect_fn = AsyncMock()
        mgr = ReconnectionManager(config, _make_backoff(), on_buffer_drain=drain_cb)

        await mgr.buffer_message(b"msg1")
        await mgr.start_reconnection(connect_fn)
        await asyncio.sleep(0.3)

        drain_cb.assert_called_once_with(1)

    async def test_async_buffer_drain_callback_on_success(self):
        drain_cb = AsyncMock()
        config = ReconnectConfig(
            reconnect_buffer_size=1024,
            initial_reconnect_delay_ms=50,
        )
        connect_fn = AsyncMock()
        mgr = ReconnectionManager(config, _make_backoff(), on_buffer_drain=drain_cb)

        await mgr.buffer_message(b"msg1")
        await mgr.start_reconnection(connect_fn)
        await asyncio.sleep(0.3)

        drain_cb.assert_called_once_with(1)

    async def test_buffer_drain_callback_exception_handled(self):
        def bad_drain(count):
            raise RuntimeError("drain failed")

        config = ReconnectConfig(
            reconnect_buffer_size=1024,
            initial_reconnect_delay_ms=50,
        )
        connect_fn = AsyncMock()
        mgr = ReconnectionManager(config, _make_backoff(), on_buffer_drain=bad_drain)

        await mgr.buffer_message(b"msg1")
        await mgr.start_reconnection(connect_fn)
        await asyncio.sleep(0.3)


class TestReconnectionManagerCancelEdge:
    """Tests for cancel discard and notify edge cases (lines 356-357)."""

    async def test_cancel_with_failed_drain_callback(self):
        async def bad_drain(count):
            raise RuntimeError("bad drain")

        config = ReconnectConfig(reconnect_buffer_size=1024)
        mgr = ReconnectionManager(config, _make_backoff(), on_buffer_drain=bad_drain)
        await mgr.buffer_message(b"msg")
        await mgr.cancel()


class TestReconnectionManagerMaxAttemptsDiscard:
    """Tests for max attempts reached discards buffer (line 260->exit, 283)."""

    async def test_max_attempts_cancels_loop(self):
        config = ReconnectConfig(
            max_reconnect_attempts=1,
            reconnect_buffer_size=1024,
            initial_reconnect_delay_ms=50,
        )
        connect_fn = AsyncMock(side_effect=Exception("fail"))
        mgr = ReconnectionManager(config, _make_backoff())

        await mgr.start_reconnection(connect_fn)
        await asyncio.sleep(0.5)

        assert not mgr.is_reconnecting
        assert connect_fn.call_count == 1


# -----------------------------------------------------------------------
# CONN-4: AsyncTransport drain behaviour
# -----------------------------------------------------------------------


class TestAsyncTransportDrain:
    async def test_ensure_connected_raises_client_closed_when_draining(self):
        from kubemq.core.config import ClientConfig
        from kubemq.core.exceptions import KubeMQClientClosedError
        from kubemq.transport.async_transport import AsyncTransport

        config = ClientConfig(address="localhost:50000")
        transport = AsyncTransport(config)
        transport._draining = True

        with pytest.raises(KubeMQClientClosedError):
            transport._ensure_connected()

    async def test_ensure_connected_raises_client_closed_when_closing(self):
        from kubemq.core.config import ClientConfig
        from kubemq.core.exceptions import KubeMQClientClosedError
        from kubemq.transport.async_transport import AsyncTransport

        config = ClientConfig(address="localhost:50000")
        transport = AsyncTransport(config)
        transport._closing = True

        with pytest.raises(KubeMQClientClosedError):
            transport._ensure_connected()

    async def test_close_idempotent(self):
        from kubemq.core.config import ClientConfig
        from kubemq.transport.async_transport import AsyncTransport

        config = ClientConfig(address="localhost:50000")
        transport = AsyncTransport(config)
        transport._connected = True

        await transport.close()
        # Second call should be noop
        await transport.close()

    async def test_close_with_drain_timeout(self):
        from kubemq.core.config import ClientConfig
        from kubemq.transport.async_transport import AsyncTransport

        config = ClientConfig(address="localhost:50000")
        transport = AsyncTransport(config)
        transport._connected = True

        await transport.close(drain_timeout=0.1)
        assert not transport._connected
        assert not transport._draining
        assert not transport._closing


# -----------------------------------------------------------------------
# CONN-6: Connection reuse documentation
# -----------------------------------------------------------------------


class TestConnectionReuseDocstrings:
    def test_base_client_has_thread_safety_doc(self):
        from kubemq.core.client import BaseClient

        assert "Thread Safety" in BaseClient.__doc__
        assert "safe to share" in BaseClient.__doc__

    def test_async_base_client_has_thread_safety_doc(self):
        from kubemq.core.client import AsyncBaseClient

        assert "Thread Safety" in AsyncBaseClient.__doc__
        assert "safe to share" in AsyncBaseClient.__doc__

    def test_native_async_base_client_has_thread_safety_doc(self):
        from kubemq.core.client import NativeAsyncBaseClient

        assert "Thread Safety" in NativeAsyncBaseClient.__doc__
        assert "safe to share" in NativeAsyncBaseClient.__doc__
