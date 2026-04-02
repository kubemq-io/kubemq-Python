"""Tests for AsyncUpstreamSender."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

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
        # Task finishes immediately since _closed=True
        await asyncio.wait_for(sender._stream_task, timeout=1.0)

    @pytest.mark.asyncio
    async def test_start_idempotent(self):
        sender, _ = _make_sender()
        # Use a long-running task to test idempotency while still running
        import contextlib

        async def long_loop():
            await asyncio.sleep(10)

        sender._stream_loop = long_loop  # type: ignore[assignment]
        sender._stream_ready.set()  # pre-set so start() doesn't block
        await sender.start()
        first = sender._stream_task
        await sender.start()
        assert sender._stream_task is first
        first.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await first

    @pytest.mark.asyncio
    async def test_start_replaces_done_task(self):
        sender, _ = _make_sender()
        sender._closed = True
        await sender.start()
        first = sender._stream_task
        await asyncio.wait_for(first, timeout=1.0)
        # Now task is done, start should create a new one
        await sender.start()
        assert sender._stream_task is not first
        await asyncio.wait_for(sender._stream_task, timeout=1.0)


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
        async for item in sender._request_generator(asyncio.Event()):
            results.append(item)
        assert len(results) == 1
        assert results[0] is req

    @pytest.mark.asyncio
    async def test_stops_when_closed(self):
        sender, _ = _make_sender()
        sender._closed = True
        results = []
        async for item in sender._request_generator(asyncio.Event()):
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
        async for item in sender._request_generator(asyncio.Event()):
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


# ==============================================================================
# Coverage extension: _run_bidi_stream, _receive_responses,
# _request_generator both-done branch, close() QueueFull
# ==============================================================================


class TestAsyncUpstreamSenderRunBidiStream:
    """Covers _run_bidi_stream() — lines 148-173."""

    @pytest.mark.asyncio
    async def test_run_bidi_stream_registers_and_unregisters(self):
        """_run_bidi_stream registers then unregisters the stream call."""
        sender, transport = _make_sender()

        # Create an async iterator that yields one response then ends
        async def _mock_call_iter(self):
            return
            yield  # make it an async generator

        mock_call = MagicMock()
        mock_call.__aiter__ = _mock_call_iter

        mock_stub = MagicMock()
        mock_stub.QueuesUpstream = MagicMock(return_value=mock_call)
        transport._get_stub = MagicMock(return_value=mock_stub)
        transport._register_stream = AsyncMock()
        transport._unregister_stream = AsyncMock()

        await sender._run_bidi_stream()

        transport._register_stream.assert_called_once_with(mock_call)
        transport._unregister_stream.assert_called_once_with(mock_call)

    @pytest.mark.asyncio
    async def test_run_bidi_stream_sets_stop_event_on_exit(self):
        """stop_event is set when _run_bidi_stream exits normally."""
        sender, transport = _make_sender()

        async def _mock_call_iter(self):
            return
            yield

        mock_call = MagicMock()
        mock_call.__aiter__ = _mock_call_iter

        mock_stub = MagicMock()
        mock_stub.QueuesUpstream = MagicMock(return_value=mock_call)
        transport._get_stub = MagicMock(return_value=mock_stub)
        transport._register_stream = AsyncMock()
        transport._unregister_stream = AsyncMock()

        stop = sender._generator_stop
        assert not stop.is_set()
        await sender._run_bidi_stream()
        assert stop.is_set()

    @pytest.mark.asyncio
    async def test_run_bidi_stream_spawns_receiver_task(self):
        """Verifies _receive_responses is actually invoked via a task."""
        sender, transport = _make_sender()
        receive_called = asyncio.Event()

        original_receive = sender._receive_responses

        async def _tracked_receive(call):
            receive_called.set()
            await original_receive(call)

        sender._receive_responses = _tracked_receive

        async def _mock_call_iter(self):
            return
            yield

        mock_call = MagicMock()
        mock_call.__aiter__ = _mock_call_iter

        mock_stub = MagicMock()
        mock_stub.QueuesUpstream = MagicMock(return_value=mock_call)
        transport._get_stub = MagicMock(return_value=mock_stub)
        transport._register_stream = AsyncMock()
        transport._unregister_stream = AsyncMock()

        await sender._run_bidi_stream()
        assert receive_called.is_set()

    @pytest.mark.asyncio
    async def test_run_bidi_stream_suppresses_grpc_cancelled(self):
        """gRPC CANCELLED error is suppressed (not raised)."""
        import grpc

        sender, transport = _make_sender()

        error = grpc.aio.AioRpcError(
            grpc.StatusCode.CANCELLED,
            initial_metadata=grpc.aio.Metadata(),
            trailing_metadata=grpc.aio.Metadata(),
        )

        async def _mock_call_iter(self):
            raise error
            yield  # noqa: unreachable

        mock_call = MagicMock()
        mock_call.__aiter__ = _mock_call_iter

        mock_stub = MagicMock()
        mock_stub.QueuesUpstream = MagicMock(return_value=mock_call)
        transport._get_stub = MagicMock(return_value=mock_stub)
        transport._register_stream = AsyncMock()
        transport._unregister_stream = AsyncMock()

        # Should NOT raise
        await sender._run_bidi_stream()

    @pytest.mark.asyncio
    async def test_run_bidi_stream_raises_non_cancelled_grpc_error(self):
        """gRPC error with non-CANCELLED code is re-raised."""
        import grpc

        sender, transport = _make_sender()

        error = grpc.aio.AioRpcError(
            grpc.StatusCode.UNAVAILABLE,
            initial_metadata=grpc.aio.Metadata(),
            trailing_metadata=grpc.aio.Metadata(),
        )

        # The receiver task raises the error
        async def _exploding_receive(call):
            raise error

        sender._receive_responses = _exploding_receive

        async def _mock_call_iter(self):
            return
            yield

        mock_call = MagicMock()
        mock_call.__aiter__ = _mock_call_iter

        mock_stub = MagicMock()
        mock_stub.QueuesUpstream = MagicMock(return_value=mock_call)
        transport._get_stub = MagicMock(return_value=mock_stub)
        transport._register_stream = AsyncMock()
        transport._unregister_stream = AsyncMock()

        with pytest.raises(Exception):
            await sender._run_bidi_stream()

    @pytest.mark.asyncio
    async def test_run_bidi_stream_handles_cancelled_error_from_receiver(self):
        """asyncio.CancelledError from receiver task is caught silently."""
        sender, transport = _make_sender()

        async def _cancel_receive(call):
            raise asyncio.CancelledError()

        sender._receive_responses = _cancel_receive

        async def _mock_call_iter(self):
            return
            yield

        mock_call = MagicMock()
        mock_call.__aiter__ = _mock_call_iter

        mock_stub = MagicMock()
        mock_stub.QueuesUpstream = MagicMock(return_value=mock_call)
        transport._get_stub = MagicMock(return_value=mock_stub)
        transport._register_stream = AsyncMock()
        transport._unregister_stream = AsyncMock()

        # Should NOT raise
        await sender._run_bidi_stream()


class TestAsyncUpstreamSenderReceiveResponses:
    """Covers _receive_responses() — lines 175-188."""

    @pytest.mark.asyncio
    async def test_resolves_futures_from_stream(self):
        """Responses from the stream resolve matching futures."""
        sender, _ = _make_sender()
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        sender._response_tracking["req-42"] = (future, "m42")

        resp = QueuesUpstreamResponse(RefRequestID="req-42")

        async def _mock_iter(self):
            yield resp

        mock_call = MagicMock()
        mock_call.__aiter__ = _mock_iter

        await sender._receive_responses(mock_call)

        assert future.done()
        assert future.result() is resp

    @pytest.mark.asyncio
    async def test_skips_unknown_request_ids(self):
        """Responses with unknown RefRequestID are silently skipped."""
        sender, _ = _make_sender()

        resp = QueuesUpstreamResponse(RefRequestID="unknown-id")

        async def _mock_iter(self):
            yield resp

        mock_call = MagicMock()
        mock_call.__aiter__ = _mock_iter

        # Should not raise
        await sender._receive_responses(mock_call)

    @pytest.mark.asyncio
    async def test_breaks_when_closed(self):
        """Loop breaks when sender._closed is True."""
        sender, _ = _make_sender()

        resp1 = QueuesUpstreamResponse(RefRequestID="r1")
        resp2 = QueuesUpstreamResponse(RefRequestID="r2")

        async def _mock_iter(self):
            yield resp1
            sender._closed = True
            yield resp2  # Should NOT be processed

        mock_call = MagicMock()
        mock_call.__aiter__ = _mock_iter

        loop = asyncio.get_running_loop()
        f1 = loop.create_future()
        f2 = loop.create_future()
        sender._response_tracking["r1"] = (f1, "m1")
        sender._response_tracking["r2"] = (f2, "m2")

        await sender._receive_responses(mock_call)

        assert f1.done()
        assert not f2.done()
        # Clean up un-awaited future
        f2.cancel()

    @pytest.mark.asyncio
    async def test_handles_cancelled_error(self):
        """asyncio.CancelledError from stream iteration is caught."""
        sender, _ = _make_sender()

        async def _mock_iter(self):
            raise asyncio.CancelledError()
            yield  # noqa

        mock_call = MagicMock()
        mock_call.__aiter__ = _mock_iter

        # Should not raise
        await sender._receive_responses(mock_call)

    @pytest.mark.asyncio
    async def test_handles_grpc_cancelled_error(self):
        """gRPC CANCELLED error from iteration is suppressed."""
        import grpc

        sender, _ = _make_sender()

        error = grpc.aio.AioRpcError(
            grpc.StatusCode.CANCELLED,
            initial_metadata=grpc.aio.Metadata(),
            trailing_metadata=grpc.aio.Metadata(),
        )

        async def _mock_iter(self):
            raise error
            yield  # noqa

        mock_call = MagicMock()
        mock_call.__aiter__ = _mock_iter

        # Should not raise
        await sender._receive_responses(mock_call)

    @pytest.mark.asyncio
    async def test_raises_grpc_non_cancelled_error(self):
        """gRPC error with non-CANCELLED code is re-raised."""
        import grpc

        sender, _ = _make_sender()

        error = grpc.aio.AioRpcError(
            grpc.StatusCode.INTERNAL,
            initial_metadata=grpc.aio.Metadata(),
            trailing_metadata=grpc.aio.Metadata(),
        )

        async def _mock_iter(self):
            raise error
            yield  # noqa

        mock_call = MagicMock()
        mock_call.__aiter__ = _mock_iter

        with pytest.raises(Exception):
            await sender._receive_responses(mock_call)


class TestAsyncUpstreamSenderRequestGeneratorBothDone:
    """Covers the both-done branch in _request_generator — lines 216-221."""

    @pytest.mark.asyncio
    async def test_stop_event_and_get_task_both_done_puts_back(self):
        """When stop_event fires and get_task also completed, message is put back."""
        sender, _ = _make_sender()
        stop_event = asyncio.Event()

        req = QueuesUpstreamRequest(RequestID="r-putback")
        sender._send_queue.put_nowait(req)

        # Set the stop event immediately so both tasks complete together
        stop_event.set()

        results = []
        async for item in sender._request_generator(stop_event):
            results.append(item)

        # Generator should have exited without yielding (stop_event was set)
        assert results == []
        # The message should have been put back into the queue
        assert not sender._send_queue.empty()
        retrieved = sender._send_queue.get_nowait()
        assert retrieved.RequestID == "r-putback"

    @pytest.mark.asyncio
    async def test_stop_event_and_sentinel_both_done_no_putback(self):
        """When stop_event fires while get_task got _SENTINEL, sentinel is not put back."""
        sender, _ = _make_sender()
        stop_event = asyncio.Event()

        # Pre-load a sentinel — the generator will start, see the queue has
        # data, so both get_task and stop_task will fire nearly at once.
        sender._send_queue.put_nowait(_SENTINEL)

        # Set stop after a very short delay so the generator enters the loop
        # and starts the wait, then both tasks complete.
        async def _set_soon():
            await asyncio.sleep(0.01)
            stop_event.set()

        asyncio.create_task(_set_soon())

        results = []
        async for item in sender._request_generator(stop_event):
            results.append(item)

        # The generator should not yield the sentinel
        assert results == []

    @pytest.mark.asyncio
    async def test_stop_event_fires_while_waiting(self):
        """stop_event fires while generator is waiting on empty queue."""
        sender, _ = _make_sender()
        stop_event = asyncio.Event()

        async def set_later():
            await asyncio.sleep(0.02)
            stop_event.set()

        asyncio.create_task(set_later())

        results = []
        async for item in sender._request_generator(stop_event):
            results.append(item)

        assert results == []


class TestAsyncUpstreamSenderCloseQueueFull:
    """Covers close() when send_queue is full — lines 257-258 (QueueFull suppress)."""

    @pytest.mark.asyncio
    async def test_close_when_queue_full_suppresses_queue_full(self):
        """close() suppresses QueueFull when send_queue is at capacity."""
        sender, _ = _make_sender(max_queue_size=1)
        # Fill the queue so put_nowait(_SENTINEL) would raise QueueFull
        sender._send_queue.put_nowait(QueuesUpstreamRequest(RequestID="filler"))
        assert sender._send_queue.full()

        # Should not raise — QueueFull is suppressed
        await sender.close()
        assert sender._closed is True
        assert sender._allow_new_messages is False

    @pytest.mark.asyncio
    async def test_close_with_full_queue_and_pending_futures(self):
        """close() with full queue still drains pending futures."""
        sender, _ = _make_sender(max_queue_size=1)
        sender._send_queue.put_nowait(QueuesUpstreamRequest(RequestID="fill"))
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        sender._response_tracking["req-x"] = (future, "mx")

        await sender.close()
        assert sender._closed is True
        assert future.done()
        # The disconnection error should be set
        assert future.result().Results[0].IsError is True
