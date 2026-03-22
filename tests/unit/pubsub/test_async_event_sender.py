"""Tests for AsyncEventSender."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from kubemq.core.exceptions import KubeMQBufferFullError
from kubemq.grpc import Event, Result
from kubemq.pubsub.async_event_sender import _SENTINEL, AsyncEventSender


def _make_sender(*, max_queue_size: int = 100, reconnect_interval: float = 0.01):
    transport = MagicMock()
    sender = AsyncEventSender(
        transport, max_queue_size=max_queue_size, reconnect_interval=reconnect_interval
    )
    return sender, transport


class TestAsyncEventSenderInit:
    def test_defaults(self):
        transport = MagicMock()
        sender = AsyncEventSender(transport)
        assert sender._closed is False
        assert sender._allow_new_messages is True
        assert sender._loop_task is None
        assert sender._reconnect_interval == 1.0

    def test_custom_params(self):
        sender, _ = _make_sender(max_queue_size=5, reconnect_interval=2.0)
        assert sender._send_queue.maxsize == 5
        assert sender._reconnect_interval == 2.0


class TestAsyncEventSenderStart:
    @pytest.mark.asyncio
    async def test_start_creates_task(self):
        sender, _ = _make_sender()
        sender._closed = True
        await sender.start()
        assert sender._loop_task is not None
        sender._loop_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await sender._loop_task

    @pytest.mark.asyncio
    async def test_start_idempotent(self):
        sender, _ = _make_sender()
        sender._closed = True
        await sender.start()
        first_task = sender._loop_task
        await sender.start()
        assert sender._loop_task is first_task
        first_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first_task

    @pytest.mark.asyncio
    async def test_start_replaces_done_task(self):
        sender, _ = _make_sender()
        sender._closed = True
        await sender.start()
        first_task = sender._loop_task
        first_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first_task
        sender._closed = True
        await sender.start()
        assert sender._loop_task is not first_task
        sender._loop_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await sender._loop_task


class TestAsyncEventSenderSend:
    @pytest.mark.asyncio
    async def test_send_fire_and_forget(self):
        sender, _ = _make_sender()
        event = Event(EventID="e1", Store=False)
        result = await sender.send(event)
        assert result is None
        assert sender._send_queue.qsize() == 1
        assert sender._send_queue.get_nowait() is event

    @pytest.mark.asyncio
    async def test_send_store_event(self):
        sender, _ = _make_sender()
        event = Event(EventID="e-store", Store=True)
        expected = Result(EventID="e-store", Sent=True, Error="")

        async def resolve():
            await asyncio.sleep(0.01)
            future = sender._response_tracking.get("e-store")
            if future and not future.done():
                future.set_result(expected)

        task = asyncio.create_task(resolve())
        result = await sender.send(event)
        await task
        assert result is expected
        assert result.Sent is True

    @pytest.mark.asyncio
    async def test_send_when_closed(self):
        sender, _ = _make_sender()
        sender._closed = True
        with pytest.raises(ConnectionError, match="closed"):
            await sender.send(Event(EventID="e1", Store=False))

    @pytest.mark.asyncio
    async def test_send_when_not_accepting(self):
        sender, _ = _make_sender()
        sender._allow_new_messages = False
        with pytest.raises(ConnectionError, match="not ready"):
            await sender.send(Event(EventID="e1", Store=False))

    @pytest.mark.asyncio
    async def test_send_queue_full_fire_and_forget(self):
        sender, _ = _make_sender(max_queue_size=1)
        sender._send_queue.put_nowait(Event(EventID="filler"))
        with pytest.raises(KubeMQBufferFullError):
            await sender.send(Event(EventID="overflow", Store=False))


class TestAsyncEventSenderStreamLoop:
    @pytest.mark.asyncio
    async def test_stream_loop_exits_when_closed(self):
        sender, _ = _make_sender()
        sender._closed = True
        await sender._stream_loop()

    @pytest.mark.asyncio
    async def test_stream_loop_breaks_on_closed_during_run(self):
        sender, _ = _make_sender()

        async def mock_run():
            sender._closed = True

        sender._run_bidi_stream = mock_run
        await sender._stream_loop()

    @pytest.mark.asyncio
    async def test_stream_loop_reconnects_on_error(self):
        sender, _ = _make_sender(reconnect_interval=0.01)
        call_count = 0

        async def mock_run():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("stream broken")
            sender._closed = True

        sender._run_bidi_stream = mock_run
        await sender._stream_loop()
        assert call_count >= 2


class TestAsyncEventSenderRequestGenerator:
    @pytest.mark.asyncio
    async def test_yields_events(self):
        sender, _ = _make_sender()
        event = Event(EventID="e1")
        sender._send_queue.put_nowait(event)
        sender._send_queue.put_nowait(_SENTINEL)

        results = []
        async for item in sender._request_generator():
            results.append(item)
        assert len(results) == 1
        assert results[0] is event

    @pytest.mark.asyncio
    async def test_stops_when_closed(self):
        sender, _ = _make_sender()
        sender._closed = True
        results = []
        async for item in sender._request_generator():
            results.append(item)
        assert results == []

    @pytest.mark.asyncio
    async def test_continues_on_timeout(self):
        sender, _ = _make_sender()

        async def add_after():
            await asyncio.sleep(0.05)
            sender._send_queue.put_nowait(_SENTINEL)

        asyncio.create_task(add_after())
        results = []
        async for item in sender._request_generator():
            results.append(item)
        assert results == []


class TestAsyncEventSenderHandleDisconnection:
    def test_errors_all_pending_futures(self):
        sender, _ = _make_sender()
        loop = asyncio.new_event_loop()
        try:
            f1 = loop.create_future()
            f2 = loop.create_future()
            sender._response_tracking["e1"] = f1
            sender._response_tracking["e2"] = f2

            sender._handle_disconnection()

            assert f1.done()
            assert f2.done()
            assert f1.result().Sent is False
            assert "Disconnected" in f1.result().Error
            assert sender._response_tracking == {}
            assert sender._allow_new_messages is False
        finally:
            loop.close()

    def test_drains_queue(self):
        sender, _ = _make_sender()
        for i in range(5):
            sender._send_queue.put_nowait(Event(EventID=f"e{i}"))
        sender._handle_disconnection()
        assert sender._send_queue.empty()

    def test_skips_done_futures(self):
        sender, _ = _make_sender()
        loop = asyncio.new_event_loop()
        try:
            future = loop.create_future()
            future.set_result(Result(EventID="e1", Sent=True))
            sender._response_tracking["e1"] = future

            sender._handle_disconnection()
            assert future.result().Sent is True
        finally:
            loop.close()


class TestAsyncEventSenderClose:
    @pytest.mark.asyncio
    async def test_close_sets_closed(self):
        sender, _ = _make_sender()
        await sender.close()
        assert sender._closed is True
        assert sender._allow_new_messages is False

    @pytest.mark.asyncio
    async def test_close_idempotent(self):
        sender, _ = _make_sender()
        await sender.close()
        await sender.close()
        assert sender._closed is True

    @pytest.mark.asyncio
    async def test_close_cancels_loop_task(self):
        sender, _ = _make_sender()
        sender._closed = True
        await sender.start()
        task = sender._loop_task
        sender._closed = False
        await sender.close()
        assert task.done()

    @pytest.mark.asyncio
    async def test_close_handles_full_queue(self):
        sender, _ = _make_sender(max_queue_size=1)
        sender._send_queue.put_nowait(Event(EventID="filler"))
        await sender.close()
        assert sender._closed is True

    @pytest.mark.asyncio
    async def test_close_drains_pending(self):
        sender, _ = _make_sender()
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        sender._response_tracking["e1"] = future
        await sender.close()
        assert future.done()
        assert "Disconnected" in future.result().Error
