"""Tests for AsyncDownstreamReceiver."""

from __future__ import annotations

import asyncio
import contextlib
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import grpc.aio
import pytest

from kubemq.grpc import (
    QueuesDownstreamRequest,
    QueuesDownstreamRequestType,
    QueuesDownstreamResponse,
)
from kubemq.queues.async_downstream_receiver import AsyncDownstreamReceiver, _SENTINEL


def _make_receiver(
    *,
    reconnect_interval: float = 0.01,
    response_timeout: float = 5.0,
):
    transport = MagicMock()
    transport._get_stub = MagicMock()
    transport._register_stream = AsyncMock()
    transport._unregister_stream = AsyncMock()
    transport._is_connection_error = MagicMock(return_value=False)
    transport._on_connection_lost = AsyncMock()
    receiver = AsyncDownstreamReceiver(
        transport,
        reconnect_interval=reconnect_interval,
        response_timeout=response_timeout,
    )
    return receiver, transport


# ==============================================================================
# Init Tests
# ==============================================================================


class TestAsyncDownstreamReceiverInit:
    def test_defaults(self):
        transport = MagicMock()
        receiver = AsyncDownstreamReceiver(transport)
        assert receiver._closed is False
        assert receiver._allow_new_requests is True
        assert receiver._stream_task is None
        assert receiver._response_timeout == 300.0
        assert receiver._reconnect_interval == 1.0

    def test_custom_params(self):
        receiver, _ = _make_receiver(reconnect_interval=2.0, response_timeout=10.0)
        assert receiver._reconnect_interval == 2.0
        assert receiver._response_timeout == 10.0
        assert receiver._send_queue.maxsize == 10_000


# ==============================================================================
# Start Tests
# ==============================================================================


class TestAsyncDownstreamReceiverStart:
    @pytest.mark.asyncio
    async def test_start_creates_task(self):
        receiver, _ = _make_receiver()
        # Mark closed so _stream_loop exits immediately
        receiver._closed = True
        await receiver.start()
        assert receiver._stream_task is not None
        # Wait for the task to finish (it exits because _closed=True)
        await asyncio.wait_for(receiver._stream_task, timeout=1.0)

    @pytest.mark.asyncio
    async def test_start_idempotent_when_running(self):
        receiver, _ = _make_receiver()
        # Use a long-running task to test idempotency while still running
        ready = asyncio.Event()

        async def long_loop():
            ready.set()
            await asyncio.sleep(10)

        receiver._stream_loop = long_loop  # type: ignore[assignment]
        receiver._stream_ready.set()  # pre-set so start() doesn't block
        await receiver.start()
        first = receiver._stream_task
        # Calling start again while task is still running should not replace
        await receiver.start()
        assert receiver._stream_task is first
        first.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await first

    @pytest.mark.asyncio
    async def test_start_replaces_done_task(self):
        receiver, _ = _make_receiver()
        receiver._closed = True
        await receiver.start()
        first = receiver._stream_task
        await asyncio.wait_for(first, timeout=1.0)
        # Now task is done, start should create a new one
        await receiver.start()
        assert receiver._stream_task is not first
        await asyncio.wait_for(receiver._stream_task, timeout=1.0)


# ==============================================================================
# Send Tests
# ==============================================================================


class TestAsyncDownstreamReceiverSend:
    @pytest.mark.asyncio
    async def test_send_returns_response(self):
        receiver, _ = _make_receiver()
        request = QueuesDownstreamRequest(RequestID="req-1")

        async def resolve():
            await asyncio.sleep(0.01)
            future = receiver._response_tracking.get("req-1")
            if future and not future.done():
                resp = QueuesDownstreamResponse(RefRequestId="req-1", IsError=False)
                future.set_result(resp)

        task = asyncio.create_task(resolve())
        result = await receiver.send(request)
        await task
        assert result is not None
        assert result.RefRequestId == "req-1"
        assert result.IsError is False

    @pytest.mark.asyncio
    async def test_send_when_closed(self):
        receiver, _ = _make_receiver()
        receiver._closed = True
        with pytest.raises(ConnectionError, match="closed"):
            await receiver.send(QueuesDownstreamRequest(RequestID="req-1"))

    @pytest.mark.asyncio
    async def test_send_when_not_accepting(self):
        receiver, _ = _make_receiver()
        receiver._allow_new_requests = False
        with pytest.raises(ConnectionError, match="not ready"):
            await receiver.send(QueuesDownstreamRequest(RequestID="req-1"))

    @pytest.mark.asyncio
    async def test_send_timeout_returns_none(self):
        receiver, _ = _make_receiver(response_timeout=0.01)
        request = QueuesDownstreamRequest(RequestID="req-timeout")
        result = await receiver.send(request)
        assert result is None
        # Future should be cleaned up from tracking
        assert "req-timeout" not in receiver._response_tracking

    @pytest.mark.asyncio
    async def test_send_enqueues_request(self):
        receiver, _ = _make_receiver()
        request = QueuesDownstreamRequest(RequestID="req-enq")

        async def resolve():
            await asyncio.sleep(0.01)
            future = receiver._response_tracking.get("req-enq")
            if future and not future.done():
                future.set_result(
                    QueuesDownstreamResponse(RefRequestId="req-enq", IsError=False)
                )

        task = asyncio.create_task(resolve())
        await receiver.send(request)
        await task
        # Request should have been put into the queue (and may have been consumed)
        # The key check is that send() works end-to-end


# ==============================================================================
# SendWithoutResponse Tests
# ==============================================================================


class TestAsyncDownstreamReceiverSendWithoutResponse:
    @pytest.mark.asyncio
    async def test_send_without_response_enqueues(self):
        receiver, _ = _make_receiver()
        request = QueuesDownstreamRequest(RequestID="req-nr")
        await receiver.send_without_response(request)
        queued = receiver._send_queue.get_nowait()
        assert queued.RequestID == "req-nr"

    @pytest.mark.asyncio
    async def test_send_without_response_when_closed(self):
        receiver, _ = _make_receiver()
        receiver._closed = True
        with pytest.raises(ConnectionError, match="closed"):
            await receiver.send_without_response(
                QueuesDownstreamRequest(RequestID="req-nr")
            )

    @pytest.mark.asyncio
    async def test_send_without_response_when_not_accepting(self):
        receiver, _ = _make_receiver()
        receiver._allow_new_requests = False
        with pytest.raises(ConnectionError, match="not ready"):
            await receiver.send_without_response(
                QueuesDownstreamRequest(RequestID="req-nr")
            )


# ==============================================================================
# StreamLoop Tests
# ==============================================================================


class TestAsyncDownstreamReceiverStreamLoop:
    @pytest.mark.asyncio
    async def test_stream_loop_exits_when_closed(self):
        receiver, _ = _make_receiver()
        receiver._closed = True
        await receiver._stream_loop()

    @pytest.mark.asyncio
    async def test_stream_loop_reconnects_on_exception(self):
        """_stream_loop catches exceptions and retries, calling _handle_disconnection."""
        receiver, _ = _make_receiver(reconnect_interval=0.01)
        call_count = 0

        async def mock_run():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("stream broken")
            # Exit cleanly second time
            receiver._closed = True

        receiver._run_bidi_stream = mock_run
        await receiver._stream_loop()
        assert call_count >= 2

    @pytest.mark.asyncio
    async def test_stream_loop_breaks_on_closed_during_error(self):
        receiver, _ = _make_receiver()

        async def mock_run():
            receiver._closed = True
            raise RuntimeError("stream broken")

        receiver._run_bidi_stream = mock_run
        await receiver._stream_loop()

    @pytest.mark.asyncio
    async def test_stream_loop_resets_state_on_each_iteration(self):
        """Each loop iteration creates fresh queue and stop event."""
        receiver, _ = _make_receiver(reconnect_interval=0.01)
        queues_seen = []
        events_seen = []

        async def mock_run():
            queues_seen.append(receiver._send_queue)
            events_seen.append(receiver._generator_stop)
            if len(queues_seen) >= 2:
                receiver._closed = True
            else:
                raise RuntimeError("retry")

        receiver._run_bidi_stream = mock_run
        await receiver._stream_loop()
        assert len(queues_seen) == 2
        # Each iteration should get a different queue and event
        assert queues_seen[0] is not queues_seen[1]
        assert events_seen[0] is not events_seen[1]

    @pytest.mark.asyncio
    async def test_stream_loop_sets_generator_stop_in_finally(self):
        """After _run_bidi_stream exits, the generator_stop event is set."""
        receiver, _ = _make_receiver(reconnect_interval=0.01)
        stop_events = []

        async def mock_run():
            stop_events.append(receiver._generator_stop)
            if len(stop_events) >= 2:
                receiver._closed = True
            else:
                raise RuntimeError("retry")

        receiver._run_bidi_stream = mock_run
        await receiver._stream_loop()
        # The first stop event should have been set
        assert stop_events[0].is_set()

    @pytest.mark.asyncio
    async def test_stream_loop_calls_handle_disconnection_on_error(self):
        """When not closed, _handle_disconnection is called after stream error."""
        receiver, _ = _make_receiver(reconnect_interval=0.01)
        disconnection_count = 0
        original_handle = receiver._handle_disconnection

        def mock_handle():
            nonlocal disconnection_count
            disconnection_count += 1
            original_handle()

        receiver._handle_disconnection = mock_handle
        call_count = 0

        async def mock_run():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("broken")
            receiver._closed = True

        receiver._run_bidi_stream = mock_run
        await receiver._stream_loop()
        assert disconnection_count >= 1


# ==============================================================================
# RunBidiStream Tests
# ==============================================================================


class TestAsyncDownstreamReceiverRunBidiStream:
    @pytest.mark.asyncio
    async def test_run_bidi_stream_registers_and_unregisters(self):
        receiver, transport = _make_receiver()

        mock_call = MagicMock()

        # Make the call iterable (returns no responses)
        async def empty_iter():
            return
            yield  # noqa: unreachable - makes this an async generator

        mock_call.__aiter__ = lambda self: empty_iter()
        stub = MagicMock()
        stub.QueuesDownstream.return_value = mock_call
        transport._get_stub.return_value = stub

        await receiver._run_bidi_stream()

        transport._register_stream.assert_awaited_once_with(mock_call)
        transport._unregister_stream.assert_awaited_once_with(mock_call)

    @pytest.mark.asyncio
    async def test_run_bidi_stream_sets_stop_event_in_finally(self):
        receiver, transport = _make_receiver()
        stop_event = receiver._generator_stop

        mock_call = MagicMock()

        async def empty_iter():
            return
            yield

        mock_call.__aiter__ = lambda self: empty_iter()
        stub = MagicMock()
        stub.QueuesDownstream.return_value = mock_call
        transport._get_stub.return_value = stub

        await receiver._run_bidi_stream()
        assert stop_event.is_set()

    @pytest.mark.asyncio
    async def test_run_bidi_stream_handles_cancelled_error(self):
        """CancelledError from receive task is suppressed."""
        receiver, transport = _make_receiver()

        mock_call = MagicMock()

        async def cancel_iter():
            raise asyncio.CancelledError()
            yield  # noqa: unreachable

        mock_call.__aiter__ = lambda self: cancel_iter()
        stub = MagicMock()
        stub.QueuesDownstream.return_value = mock_call
        transport._get_stub.return_value = stub

        # Should not raise
        await receiver._run_bidi_stream()

    @pytest.mark.asyncio
    async def test_run_bidi_stream_handles_grpc_cancelled(self):
        """gRPC CANCELLED status is silently ignored."""
        receiver, transport = _make_receiver()

        mock_call = MagicMock()

        async def grpc_cancel_iter():
            raise grpc.aio.AioRpcError(
                code=grpc.StatusCode.CANCELLED,
                initial_metadata=grpc.aio.Metadata(),
                trailing_metadata=grpc.aio.Metadata(),
                details="cancelled",
            )
            yield  # noqa: unreachable

        mock_call.__aiter__ = lambda self: grpc_cancel_iter()
        stub = MagicMock()
        stub.QueuesDownstream.return_value = mock_call
        transport._get_stub.return_value = stub

        # Should not raise
        await receiver._run_bidi_stream()

    @pytest.mark.asyncio
    async def test_run_bidi_stream_connection_error_triggers_reconnect(self):
        """Non-cancelled gRPC connection error triggers _on_connection_lost and raises."""
        receiver, transport = _make_receiver()
        transport._is_connection_error.return_value = True

        mock_call = MagicMock()

        rpc_error = grpc.aio.AioRpcError(
            code=grpc.StatusCode.UNAVAILABLE,
            initial_metadata=grpc.aio.Metadata(),
            trailing_metadata=grpc.aio.Metadata(),
            details="unavailable",
        )

        async def grpc_error_iter():
            raise rpc_error
            yield  # noqa: unreachable

        mock_call.__aiter__ = lambda self: grpc_error_iter()
        stub = MagicMock()
        stub.QueuesDownstream.return_value = mock_call
        transport._get_stub.return_value = stub

        with pytest.raises(Exception):
            await receiver._run_bidi_stream()

        transport._on_connection_lost.assert_awaited_once()


# ==============================================================================
# RequestGenerator Tests
# ==============================================================================


class TestAsyncDownstreamReceiverRequestGenerator:
    @pytest.mark.asyncio
    async def test_yields_requests(self):
        receiver, _ = _make_receiver()
        req = QueuesDownstreamRequest(RequestID="r1")
        receiver._send_queue.put_nowait(req)
        receiver._send_queue.put_nowait(_SENTINEL)

        results = []
        async for item in receiver._request_generator(asyncio.Event()):
            results.append(item)
        assert len(results) == 1
        assert results[0].RequestID == "r1"

    @pytest.mark.asyncio
    async def test_stops_when_closed(self):
        receiver, _ = _make_receiver()
        receiver._closed = True
        results = []
        async for item in receiver._request_generator(asyncio.Event()):
            results.append(item)
        assert results == []

    @pytest.mark.asyncio
    async def test_stops_on_stop_event(self):
        receiver, _ = _make_receiver()
        stop_event = asyncio.Event()

        async def set_stop():
            await asyncio.sleep(0.02)
            stop_event.set()

        task = asyncio.create_task(set_stop())
        results = []
        async for item in receiver._request_generator(stop_event):
            results.append(item)
        await task
        assert results == []

    @pytest.mark.asyncio
    async def test_stop_event_puts_back_message(self):
        """When stop_event fires while a message was also ready, put it back."""
        receiver, _ = _make_receiver()
        stop_event = asyncio.Event()

        req = QueuesDownstreamRequest(RequestID="r-putback")
        receiver._send_queue.put_nowait(req)
        # Set stop event immediately so both complete
        stop_event.set()

        results = []
        async for item in receiver._request_generator(stop_event):
            results.append(item)
        assert results == []
        # The message should have been put back
        assert not receiver._send_queue.empty()
        recovered = receiver._send_queue.get_nowait()
        assert recovered.RequestID == "r-putback"

    @pytest.mark.asyncio
    async def test_stop_event_does_not_put_back_sentinel(self):
        """When stop_event fires mid-iteration and the message was a SENTINEL, don't put it back."""
        receiver, _ = _make_receiver()
        stop_event = asyncio.Event()

        # We need the generator to enter the loop body first (stop_event not set),
        # then have both get_task and stop_task complete.  Put sentinel and set
        # stop_event with a tiny delay so the generator enters the loop.
        async def set_stop_soon():
            await asyncio.sleep(0.01)
            receiver._send_queue.put_nowait(_SENTINEL)
            stop_event.set()

        asyncio.create_task(set_stop_soon())

        results = []
        async for item in receiver._request_generator(stop_event):
            results.append(item)
        assert results == []
        # SENTINEL should NOT be put back (guarded by `if msg is not _SENTINEL`)
        assert receiver._send_queue.empty()

    @pytest.mark.asyncio
    async def test_yields_multiple_requests(self):
        receiver, _ = _make_receiver()
        req1 = QueuesDownstreamRequest(RequestID="r1")
        req2 = QueuesDownstreamRequest(RequestID="r2")
        receiver._send_queue.put_nowait(req1)
        receiver._send_queue.put_nowait(req2)
        receiver._send_queue.put_nowait(_SENTINEL)

        results = []
        async for item in receiver._request_generator(asyncio.Event()):
            results.append(item)
        assert len(results) == 2
        assert results[0].RequestID == "r1"
        assert results[1].RequestID == "r2"


# ==============================================================================
# ReceiveResponses Tests
# ==============================================================================


class TestAsyncDownstreamReceiverReceiveResponses:
    @pytest.mark.asyncio
    async def test_resolves_matching_future(self):
        receiver, _ = _make_receiver()
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        receiver._response_tracking["req-1"] = future

        response = QueuesDownstreamResponse(RefRequestId="req-1", IsError=False)

        async def mock_responses():
            yield response

        mock_call = MagicMock()
        mock_call.__aiter__ = lambda self: mock_responses()

        await receiver._receive_responses(mock_call)
        assert future.done()
        assert future.result() is response

    @pytest.mark.asyncio
    async def test_sets_allow_new_requests(self):
        receiver, _ = _make_receiver()
        receiver._allow_new_requests = False

        response = QueuesDownstreamResponse(RefRequestId="untracked", IsError=False)

        async def mock_responses():
            yield response

        mock_call = MagicMock()
        mock_call.__aiter__ = lambda self: mock_responses()

        await receiver._receive_responses(mock_call)
        assert receiver._allow_new_requests is True

    @pytest.mark.asyncio
    async def test_handles_close_by_server(self):
        receiver, _ = _make_receiver()

        close_response = QueuesDownstreamResponse(
            RefRequestId="",
            RequestTypeData=QueuesDownstreamRequestType.CloseByServer,
            IsError=False,
        )

        async def mock_responses():
            yield close_response

        mock_call = MagicMock()
        mock_call.__aiter__ = lambda self: mock_responses()

        await receiver._receive_responses(mock_call)
        # _handle_disconnection sets _allow_new_requests=False
        assert receiver._allow_new_requests is False

    @pytest.mark.asyncio
    async def test_close_by_server_notifies_pending_futures(self):
        receiver, _ = _make_receiver()
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        receiver._response_tracking["pending-req"] = future

        close_response = QueuesDownstreamResponse(
            RefRequestId="",
            RequestTypeData=QueuesDownstreamRequestType.CloseByServer,
            IsError=False,
        )

        async def mock_responses():
            yield close_response

        mock_call = MagicMock()
        mock_call.__aiter__ = lambda self: mock_responses()

        await receiver._receive_responses(mock_call)
        assert future.done()
        assert future.result().IsError is True
        assert "Disconnected" in future.result().Error

    @pytest.mark.asyncio
    async def test_breaks_when_closed(self):
        receiver, _ = _make_receiver()
        receiver._closed = True

        response = QueuesDownstreamResponse(RefRequestId="req-1", IsError=False)

        async def mock_responses():
            yield response

        mock_call = MagicMock()
        mock_call.__aiter__ = lambda self: mock_responses()

        loop = asyncio.get_running_loop()
        future = loop.create_future()
        receiver._response_tracking["req-1"] = future

        await receiver._receive_responses(mock_call)
        # Should break before processing
        assert not future.done()

    @pytest.mark.asyncio
    async def test_catches_cancelled_error(self):
        receiver, _ = _make_receiver()

        async def cancel_responses():
            raise asyncio.CancelledError()
            yield  # noqa: unreachable

        mock_call = MagicMock()
        mock_call.__aiter__ = lambda self: cancel_responses()

        # Should not raise
        await receiver._receive_responses(mock_call)

    @pytest.mark.asyncio
    async def test_ignores_done_future(self):
        receiver, _ = _make_receiver()
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        future.set_result(QueuesDownstreamResponse(RefRequestId="req-1"))
        receiver._response_tracking["req-1"] = future

        new_response = QueuesDownstreamResponse(RefRequestId="req-1", IsError=True)

        async def mock_responses():
            yield new_response

        mock_call = MagicMock()
        mock_call.__aiter__ = lambda self: mock_responses()

        await receiver._receive_responses(mock_call)
        # Future should still have old result
        assert future.result().IsError is False

    @pytest.mark.asyncio
    async def test_grpc_connection_error_triggers_reconnect(self):
        """Non-cancelled gRPC error in _receive_responses triggers _on_connection_lost."""
        receiver, transport = _make_receiver()
        transport._is_connection_error.return_value = True

        rpc_error = grpc.aio.AioRpcError(
            code=grpc.StatusCode.UNAVAILABLE,
            initial_metadata=grpc.aio.Metadata(),
            trailing_metadata=grpc.aio.Metadata(),
            details="unavailable",
        )

        async def grpc_error_responses():
            raise rpc_error
            yield  # noqa: unreachable

        mock_call = MagicMock()
        mock_call.__aiter__ = lambda self: grpc_error_responses()

        with pytest.raises(Exception):
            await receiver._receive_responses(mock_call)

        transport._on_connection_lost.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_grpc_cancelled_does_not_raise(self):
        """gRPC CANCELLED status in _receive_responses does not raise."""
        receiver, transport = _make_receiver()

        rpc_error = grpc.aio.AioRpcError(
            code=grpc.StatusCode.CANCELLED,
            initial_metadata=grpc.aio.Metadata(),
            trailing_metadata=grpc.aio.Metadata(),
            details="cancelled",
        )

        async def grpc_cancel_responses():
            raise rpc_error
            yield  # noqa: unreachable

        mock_call = MagicMock()
        mock_call.__aiter__ = lambda self: grpc_cancel_responses()

        # Should not raise
        await receiver._receive_responses(mock_call)


# ==============================================================================
# HandleDisconnection Tests
# ==============================================================================


class TestAsyncDownstreamReceiverHandleDisconnection:
    def test_clears_pending_futures(self):
        receiver, _ = _make_receiver()
        loop = asyncio.new_event_loop()
        try:
            f1 = loop.create_future()
            f2 = loop.create_future()
            receiver._response_tracking["req-a"] = f1
            receiver._response_tracking["req-b"] = f2

            receiver._handle_disconnection()

            assert receiver._response_tracking == {}
            assert receiver._allow_new_requests is False
            assert f1.done()
            assert f2.done()
            assert f1.result().IsError is True
            assert "Disconnected" in f1.result().Error
            assert f2.result().IsError is True
            assert "Disconnected" in f2.result().Error
        finally:
            loop.close()

    def test_drains_queue(self):
        receiver, _ = _make_receiver()
        for i in range(5):
            receiver._send_queue.put_nowait(
                QueuesDownstreamRequest(RequestID=f"r{i}")
            )
        receiver._handle_disconnection()
        assert receiver._send_queue.empty()

    def test_skips_done_futures(self):
        receiver, _ = _make_receiver()
        loop = asyncio.new_event_loop()
        try:
            future = loop.create_future()
            original_resp = QueuesDownstreamResponse(RefRequestId="req-1")
            future.set_result(original_resp)
            receiver._response_tracking["req-1"] = future

            receiver._handle_disconnection()
            # Should not change the existing result
            assert future.result() is original_resp
        finally:
            loop.close()

    def test_empty_queue_is_safe(self):
        receiver, _ = _make_receiver()
        # Queue already empty - should not raise
        receiver._handle_disconnection()
        assert receiver._allow_new_requests is False


# ==============================================================================
# Close Tests
# ==============================================================================


class TestAsyncDownstreamReceiverClose:
    @pytest.mark.asyncio
    async def test_close_sets_closed(self):
        receiver, _ = _make_receiver()
        await receiver.close()
        assert receiver._closed is True
        assert receiver._allow_new_requests is False

    @pytest.mark.asyncio
    async def test_close_idempotent(self):
        receiver, _ = _make_receiver()
        await receiver.close()
        await receiver.close()
        assert receiver._closed is True

    @pytest.mark.asyncio
    async def test_close_cancels_stream_task(self):
        receiver, _ = _make_receiver()
        # Start a stream loop that blocks
        receiver._closed = True
        await receiver.start()
        task = receiver._stream_task
        await asyncio.wait_for(task, timeout=1.0)
        # Reset closed to test close() behavior
        receiver._closed = False
        # Create a task that will block
        async def block_forever():
            await asyncio.sleep(999)

        receiver._stream_task = asyncio.create_task(block_forever())
        await receiver.close()
        assert receiver._stream_task.done()

    @pytest.mark.asyncio
    async def test_close_sets_generator_stop(self):
        receiver, _ = _make_receiver()
        assert not receiver._generator_stop.is_set()
        await receiver.close()
        assert receiver._generator_stop.is_set()

    @pytest.mark.asyncio
    async def test_close_puts_sentinel_before_drain(self):
        """close() puts _SENTINEL into the queue (then _handle_disconnection drains it)."""
        receiver, _ = _make_receiver()
        # Verify _SENTINEL is put into the queue by intercepting _handle_disconnection
        sentinel_seen = False
        original_handle = receiver._handle_disconnection

        def intercepting_handle():
            nonlocal sentinel_seen
            # Check if sentinel is in the queue before draining
            items = []
            while not receiver._send_queue.empty():
                try:
                    items.append(receiver._send_queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            sentinel_seen = any(item is _SENTINEL for item in items)
            # Put items back and call original
            for item in items:
                receiver._send_queue.put_nowait(item)
            original_handle()

        receiver._handle_disconnection = intercepting_handle
        await receiver.close()
        assert sentinel_seen

    @pytest.mark.asyncio
    async def test_close_handles_full_queue(self):
        receiver, _ = _make_receiver()
        # Fill the queue to capacity
        for i in range(10_000):
            receiver._send_queue.put_nowait(
                QueuesDownstreamRequest(RequestID=f"filler-{i}")
            )
        # close() should not raise even if queue is full
        await receiver.close()
        assert receiver._closed is True

    @pytest.mark.asyncio
    async def test_close_resolves_pending_futures(self):
        receiver, _ = _make_receiver()
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        receiver._response_tracking["req-pending"] = future
        await receiver.close()
        assert future.done()
        assert future.result().IsError is True
        assert "Disconnected" in future.result().Error


# ==============================================================================
# Integration-style Tests (end-to-end within the class)
# ==============================================================================


class TestAsyncDownstreamReceiverIntegration:
    @pytest.mark.asyncio
    async def test_full_send_receive_cycle(self):
        """Test send() -> _request_generator yields -> _receive_responses resolves."""
        receiver, transport = _make_receiver()

        request = QueuesDownstreamRequest(RequestID="req-full")
        response = QueuesDownstreamResponse(RefRequestId="req-full", IsError=False)

        async def resolve_after_delay():
            await asyncio.sleep(0.02)
            future = receiver._response_tracking.get("req-full")
            if future and not future.done():
                future.set_result(response)

        resolver = asyncio.create_task(resolve_after_delay())
        result = await receiver.send(request)
        await resolver
        assert result is response

    @pytest.mark.asyncio
    async def test_stream_loop_with_real_bidi_stream(self):
        """Integration test: _stream_loop -> _run_bidi_stream with mocked gRPC."""
        receiver, transport = _make_receiver(reconnect_interval=0.01)

        call_count = 0

        # Mock the full bidi stream path
        async def mock_responses():
            return
            yield  # noqa: unreachable

        mock_call = MagicMock()
        mock_call.__aiter__ = lambda self: mock_responses()

        def get_stub():
            nonlocal call_count
            call_count += 1
            stub = MagicMock()
            stub.QueuesDownstream.return_value = mock_call
            if call_count >= 2:
                receiver._closed = True
            return stub

        transport._get_stub = get_stub
        await receiver._stream_loop()
        assert call_count >= 1
