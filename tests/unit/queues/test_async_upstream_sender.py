"""Tests for AsyncUpstreamSender."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from kubemq.core.exceptions import KubeMQBufferFullError
from kubemq.grpc import (
    QueueMessage as pbQueueMessage,
    QueuesUpstreamRequest,
    QueuesUpstreamResponse,
    SendQueueMessageResult,
)
from kubemq.queues.async_upstream_sender import _SENTINEL, AsyncUpstreamSender


def _make_sender(
    *, max_queue_size: int = 100, send_timeout: float = 2.0, reconnect_interval: float = 0.01
):
    transport = MagicMock()
    sender = AsyncUpstreamSender(
        transport,
        max_queue_size=max_queue_size,
        send_timeout=send_timeout,
        reconnect_interval=reconnect_interval,
    )
    return sender, transport


class TestAsyncUpstreamSenderInit:
    def test_defaults(self):
        transport = MagicMock()
        sender = AsyncUpstreamSender(transport)
        assert sender._closed is False
        assert sender._allow_new_messages is True
        assert sender._stream_task is None
        assert sender._send_timeout == 2.0

    def test_custom_params(self):
        sender, _ = _make_sender(max_queue_size=5, send_timeout=0.5)
        assert sender._send_queue.maxsize == 5
        assert sender._send_timeout == 0.5


class TestAsyncUpstreamSenderStart:
    @pytest.mark.asyncio
    async def test_start_creates_task(self):
        sender, _ = _make_sender()
        sender._closed = True
        await sender.start()
        assert sender._stream_task is not None
        sender._stream_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await sender._stream_task

    @pytest.mark.asyncio
    async def test_start_idempotent(self):
        sender, _ = _make_sender()
        sender._closed = True
        await sender.start()
        first = sender._stream_task
        await sender.start()
        assert sender._stream_task is first
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first

    @pytest.mark.asyncio
    async def test_start_replaces_done_task(self):
        sender, _ = _make_sender()
        sender._closed = True
        await sender.start()
        first = sender._stream_task
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        sender._closed = True
        await sender.start()
        assert sender._stream_task is not first
        sender._stream_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await sender._stream_task


class TestAsyncUpstreamSenderSend:
    @pytest.mark.asyncio
    async def test_send_success(self):
        sender, _ = _make_sender()
        msg = pbQueueMessage(MessageID="m1", Channel="q1")
        send_result_pb = SendQueueMessageResult(MessageID="m1", IsError=False, SentAt=1000)
        upstream_response = QueuesUpstreamResponse(Results=[send_result_pb])

        async def resolve():
            await asyncio.sleep(0.01)
            async with sender._lock:
                for req_id, (future, _) in sender._response_tracking.items():
                    if not future.done():
                        upstream_response.RefRequestID = req_id
                        future.set_result(upstream_response)

        task = asyncio.create_task(resolve())
        result = await sender.send(msg)
        await task
        assert result is not None
        assert result.is_error is False

    @pytest.mark.asyncio
    async def test_send_when_closed(self):
        sender, _ = _make_sender()
        sender._closed = True
        with pytest.raises(ConnectionError, match="closed"):
            await sender.send(pbQueueMessage(MessageID="m1"))

    @pytest.mark.asyncio
    async def test_send_when_not_accepting(self):
        sender, _ = _make_sender()
        sender._allow_new_messages = False
        with pytest.raises(ConnectionError, match="not ready"):
            await sender.send(pbQueueMessage(MessageID="m1"))

    @pytest.mark.asyncio
    async def test_send_queue_full(self):
        sender, _ = _make_sender(max_queue_size=1)
        sender._send_queue.put_nowait(QueuesUpstreamRequest(RequestID="filler"))
        with pytest.raises(KubeMQBufferFullError):
            await sender.send(pbQueueMessage(MessageID="m-full"))

    @pytest.mark.asyncio
    async def test_send_timeout(self):
        sender, _ = _make_sender(send_timeout=0.01)
        msg = pbQueueMessage(MessageID="m-timeout", Channel="q1")
        result = await sender.send(msg)
        assert result.is_error is True
        assert "Timeout" in result.error

    @pytest.mark.asyncio
    async def test_send_empty_response(self):
        sender, _ = _make_sender()
        msg = pbQueueMessage(MessageID="m-empty", Channel="q1")
        empty_response = QueuesUpstreamResponse()

        async def resolve():
            await asyncio.sleep(0.01)
            async with sender._lock:
                for req_id, (future, _) in sender._response_tracking.items():
                    if not future.done():
                        empty_response.RefRequestID = req_id
                        future.set_result(empty_response)

        task = asyncio.create_task(resolve())
        result = await sender.send(msg)
        await task
        assert result.is_error is True
        assert "Empty response" in result.error


class TestAsyncUpstreamSenderStreamLoop:
    @pytest.mark.asyncio
    async def test_stream_loop_exits_when_closed(self):
        sender, _ = _make_sender()
        sender._closed = True
        await sender._stream_loop()

    @pytest.mark.asyncio
    async def test_stream_loop_reconnects_on_exception(self):
        """Test that _stream_loop catches exceptions and retries."""
        sender, _ = _make_sender(reconnect_interval=0.01)
        call_count = 0
        original_run = sender._run_bidi_stream

        async def mock_run():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("stream broken")
            sender._closed = True

        sender._run_bidi_stream = mock_run
        await sender._stream_loop()
        assert call_count >= 2

    @pytest.mark.asyncio
    async def test_stream_loop_breaks_on_closed_during_error(self):
        sender, _ = _make_sender()

        async def mock_run():
            sender._closed = True
            raise RuntimeError("stream broken")

        sender._run_bidi_stream = mock_run
        await sender._stream_loop()

    @pytest.mark.asyncio
    async def test_stream_loop_breaks_on_closed_during_response(self):
        sender, _ = _make_sender()

        async def mock_run():
            sender._closed = True

        sender._run_bidi_stream = mock_run
        await sender._stream_loop()


class TestAsyncUpstreamSenderRequestGenerator:
    @pytest.mark.asyncio
    async def test_yields_requests(self):
        sender, _ = _make_sender()
        req = QueuesUpstreamRequest(RequestID="r1")
        sender._send_queue.put_nowait(req)
        sender._send_queue.put_nowait(_SENTINEL)

        results = []
        async for item in sender._request_generator():
            results.append(item)
        assert len(results) == 1
        assert results[0] is req

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


class TestAsyncUpstreamSenderProcessResponse:
    def test_resolves_future(self):
        sender, _ = _make_sender()
        loop = asyncio.new_event_loop()
        try:
            future = loop.create_future()
            sender._response_tracking["req-1"] = (future, "m1")

            response = QueuesUpstreamResponse(RefRequestID="req-1")
            sender._process_response(response)
            assert future.done()
            assert future.result() is response
        finally:
            loop.close()

    def test_ignores_unknown_request_id(self):
        sender, _ = _make_sender()
        sender._process_response(QueuesUpstreamResponse(RefRequestID="unknown"))

    def test_ignores_done_future(self):
        sender, _ = _make_sender()
        loop = asyncio.new_event_loop()
        try:
            future = loop.create_future()
            future.set_result(QueuesUpstreamResponse(RefRequestID="req-1"))
            sender._response_tracking["req-1"] = (future, "m1")

            new_resp = QueuesUpstreamResponse(RefRequestID="req-1")
            sender._process_response(new_resp)
            assert future.result() is not new_resp
        finally:
            loop.close()


class TestAsyncUpstreamSenderHandleDisconnection:
    def test_errors_all_pending(self):
        sender, _ = _make_sender()
        loop = asyncio.new_event_loop()
        try:
            f1 = loop.create_future()
            f2 = loop.create_future()
            sender._response_tracking["req-1"] = (f1, "m1")
            sender._response_tracking["req-2"] = (f2, "m2")

            sender._handle_disconnection()
            assert f1.done()
            assert f2.done()
            assert f1.result().Results[0].IsError is True
            assert "Disconnected" in f1.result().Results[0].Error
            assert sender._response_tracking == {}
            assert sender._allow_new_messages is False
        finally:
            loop.close()

    def test_drains_queue(self):
        sender, _ = _make_sender()
        for i in range(5):
            sender._send_queue.put_nowait(QueuesUpstreamRequest(RequestID=f"r{i}"))
        sender._handle_disconnection()
        assert sender._send_queue.empty()

    def test_skips_done_futures(self):
        sender, _ = _make_sender()
        loop = asyncio.new_event_loop()
        try:
            future = loop.create_future()
            future.set_result(QueuesUpstreamResponse(RefRequestID="req-1"))
            sender._response_tracking["req-1"] = (future, "m1")

            sender._handle_disconnection()
            assert future.result().RefRequestID == "req-1"
        finally:
            loop.close()


class TestAsyncUpstreamSenderClose:
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

    @pytest.mark.asyncio
    async def test_close_cancels_task(self):
        sender, _ = _make_sender()
        sender._closed = True
        await sender.start()
        task = sender._stream_task
        sender._closed = False
        await sender.close()
        assert task.done()

    @pytest.mark.asyncio
    async def test_close_handles_full_queue(self):
        sender, _ = _make_sender(max_queue_size=1)
        sender._send_queue.put_nowait(QueuesUpstreamRequest(RequestID="filler"))
        await sender.close()
        assert sender._closed is True

    @pytest.mark.asyncio
    async def test_close_drains_pending(self):
        sender, _ = _make_sender()
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        sender._response_tracking["req-1"] = (future, "m1")
        await sender.close()
        assert future.done()
