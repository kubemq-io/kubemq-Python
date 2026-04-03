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


# ==============================================================================
# Additional coverage tests for uncovered lines
# ==============================================================================


class AsyncIteratorMock:
    """Simple async iterator for testing."""

    def __init__(self, items):
        self._items = iter(items)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._items)
        except StopIteration:
            raise StopAsyncIteration


class TestAsyncEventSenderStreamLoopClosedDuringError:
    """Cover line 110: _stream_loop breaks when _closed is True during error handling."""

    @pytest.mark.asyncio
    async def test_stream_loop_breaks_when_closed_set_during_exception(self):
        """_run_bidi_stream raises and _closed is already True -> break."""
        sender, _ = _make_sender(reconnect_interval=0.01)

        async def mock_run():
            sender._closed = True
            raise RuntimeError("stream died")

        sender._run_bidi_stream = mock_run
        await sender._stream_loop()
        assert sender._closed is True


class TestAsyncEventSenderRunBidiStream:
    """Cover lines 117-138: _run_bidi_stream."""

    @pytest.mark.asyncio
    async def test_run_bidi_stream_setup_and_teardown(self):
        """Verify stub, stream registration, receiver task, and unregistration."""
        sender, transport = _make_sender()

        # Mock stub and its SendEventsStream
        mock_call = MagicMock()
        # Make call async-iterable (for _receive_responses) — empty stream
        mock_call.__aiter__ = MagicMock(return_value=AsyncIteratorMock([]))

        mock_stub = MagicMock()
        mock_stub.SendEventsStream = MagicMock(return_value=mock_call)
        transport._get_stub = MagicMock(return_value=mock_stub)

        # Make registration/unregistration async
        register_called = False
        unregister_called = False

        async def mock_register(call):
            nonlocal register_called
            register_called = True

        async def mock_unregister(call):
            nonlocal unregister_called
            unregister_called = True

        transport._register_stream = mock_register
        transport._unregister_stream = mock_unregister

        # Close sender immediately so _request_generator stops
        sender._closed = True

        await sender._run_bidi_stream()

        assert register_called
        assert unregister_called
        mock_stub.SendEventsStream.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_bidi_stream_cancelled_receiver(self):
        """Receiver task cancelled -> CancelledError suppressed (line 131-132)."""
        sender, transport = _make_sender()

        mock_call = MagicMock()

        # Make the call aiter raise CancelledError
        class CancellingIterator:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise asyncio.CancelledError()

        mock_call.__aiter__ = MagicMock(return_value=CancellingIterator())

        mock_stub = MagicMock()
        mock_stub.SendEventsStream = MagicMock(return_value=mock_call)
        transport._get_stub = MagicMock(return_value=mock_stub)

        async def noop(call):
            pass

        transport._register_stream = noop
        transport._unregister_stream = noop

        sender._closed = True

        # Should not raise
        await sender._run_bidi_stream()

    @pytest.mark.asyncio
    async def test_run_bidi_stream_grpc_error_not_cancelled(self):
        """AioRpcError with non-CANCELLED code -> from_grpc_error raised (lines 133-136)."""
        import grpc

        sender, transport = _make_sender()

        # Create a real AioRpcError instance (subclass of Exception)
        real_error = grpc.aio.AioRpcError(
            code=grpc.StatusCode.UNAVAILABLE,
            initial_metadata=grpc.aio.Metadata(),
            trailing_metadata=grpc.aio.Metadata(),
            details="server unavailable",
        )

        mock_call = MagicMock()

        class ErrorIterator:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise real_error

        mock_call.__aiter__ = MagicMock(return_value=ErrorIterator())

        mock_stub = MagicMock()
        mock_stub.SendEventsStream = MagicMock(return_value=mock_call)
        transport._get_stub = MagicMock(return_value=mock_stub)

        async def noop(call):
            pass

        transport._register_stream = noop
        transport._unregister_stream = noop

        sender._closed = True

        from kubemq.core.exceptions import KubeMQError

        with pytest.raises(KubeMQError):
            await sender._run_bidi_stream()

    @pytest.mark.asyncio
    async def test_run_bidi_stream_grpc_cancelled_suppressed(self):
        """AioRpcError with CANCELLED code -> suppressed (lines 133-136)."""
        import grpc

        sender, transport = _make_sender()

        real_error = grpc.aio.AioRpcError(
            code=grpc.StatusCode.CANCELLED,
            initial_metadata=grpc.aio.Metadata(),
            trailing_metadata=grpc.aio.Metadata(),
            details="cancelled",
        )

        mock_call = MagicMock()

        class ErrorIterator:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise real_error

        mock_call.__aiter__ = MagicMock(return_value=ErrorIterator())

        mock_stub = MagicMock()
        mock_stub.SendEventsStream = MagicMock(return_value=mock_call)
        transport._get_stub = MagicMock(return_value=mock_stub)

        async def noop(call):
            pass

        transport._register_stream = noop
        transport._unregister_stream = noop

        sender._closed = True

        # Should not raise
        await sender._run_bidi_stream()


class TestAsyncEventSenderReceiveResponses:
    """Cover lines 158-168: _receive_responses."""

    @pytest.mark.asyncio
    async def test_receive_resolves_futures(self):
        """Response matches tracked EventID -> future resolved (lines 158-164)."""
        sender, _ = _make_sender()
        loop = asyncio.get_running_loop()

        future = loop.create_future()
        sender._response_tracking["resp-1"] = future

        response = MagicMock()
        response.EventID = "resp-1"

        mock_call = AsyncIteratorMock([response])

        await sender._receive_responses(mock_call)

        assert future.done()
        assert future.result() is response

    @pytest.mark.asyncio
    async def test_receive_skips_unknown_event_id(self):
        """Response with unknown EventID -> no error (line 162)."""
        sender, _ = _make_sender()

        response = MagicMock()
        response.EventID = "unknown-id"

        mock_call = AsyncIteratorMock([response])

        # Should not raise
        await sender._receive_responses(mock_call)

    @pytest.mark.asyncio
    async def test_receive_stops_when_closed(self):
        """Closes during iteration -> breaks (line 160-161).

        The _receive_responses loop checks `if self._closed: break` BEFORE
        processing the response.  So if _closed is set before a response
        is yielded, that response is NOT processed.
        """
        sender, _ = _make_sender()
        loop = asyncio.get_running_loop()

        future1 = loop.create_future()
        sender._response_tracking["resp-1"] = future1

        future2 = loop.create_future()
        sender._response_tracking["resp-2"] = future2

        response1 = MagicMock()
        response1.EventID = "resp-1"

        response2 = MagicMock()
        response2.EventID = "resp-2"

        class CloseAfterFirstProcessedIterator:
            """Yields first item normally; sets _closed=True before yielding second."""
            def __init__(self, items, sender_ref):
                self._items = iter(items)
                self._sender = sender_ref
                self._count = 0

            def __aiter__(self):
                return self

            async def __anext__(self):
                try:
                    item = next(self._items)
                    self._count += 1
                    if self._count == 2:
                        # Set closed before yielding second response
                        self._sender._closed = True
                    return item
                except StopIteration:
                    raise StopAsyncIteration

        mock_call = CloseAfterFirstProcessedIterator([response1, response2], sender)

        await sender._receive_responses(mock_call)

        # First response was processed normally
        assert future1.done()
        assert future1.result() is response1
        # Second response was NOT processed (closed check broke loop)
        assert not future2.done()

    @pytest.mark.asyncio
    async def test_receive_grpc_error_not_cancelled(self):
        """AioRpcError with non-CANCELLED code -> from_grpc_error raised (lines 165-168)."""
        import grpc

        sender, _ = _make_sender()

        real_error = grpc.aio.AioRpcError(
            code=grpc.StatusCode.UNAVAILABLE,
            initial_metadata=grpc.aio.Metadata(),
            trailing_metadata=grpc.aio.Metadata(),
            details="unavailable",
        )

        class ErrorIterator:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise real_error

        mock_call = ErrorIterator()

        from kubemq.core.exceptions import KubeMQError

        with pytest.raises(KubeMQError):
            await sender._receive_responses(mock_call)

    @pytest.mark.asyncio
    async def test_receive_grpc_cancelled_suppressed(self):
        """AioRpcError with CANCELLED code -> suppressed (lines 165-168)."""
        import grpc

        sender, _ = _make_sender()

        real_error = grpc.aio.AioRpcError(
            code=grpc.StatusCode.CANCELLED,
            initial_metadata=grpc.aio.Metadata(),
            trailing_metadata=grpc.aio.Metadata(),
            details="cancelled",
        )

        class ErrorIterator:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise real_error

        mock_call = ErrorIterator()

        # Should not raise
        await sender._receive_responses(mock_call)

    @pytest.mark.asyncio
    async def test_receive_skips_done_future(self):
        """Response for already-done future -> no error (line 163)."""
        sender, _ = _make_sender()
        loop = asyncio.get_running_loop()

        future = loop.create_future()
        future.set_result(Result(EventID="resp-1", Sent=True))
        sender._response_tracking["resp-1"] = future

        response = MagicMock()
        response.EventID = "resp-1"

        mock_call = AsyncIteratorMock([response])

        # Should not raise
        await sender._receive_responses(mock_call)
        # Original result should be unchanged
        assert future.result().Sent is True


class TestAsyncEventSenderStreamLoopReconnection:
    """Cover _stream_loop reconnection behavior with _handle_disconnection."""

    @pytest.mark.asyncio
    async def test_stream_loop_calls_handle_disconnection_on_error(self):
        """Error -> _handle_disconnection called, then reconnects (lines 111-113)."""
        sender, _ = _make_sender(reconnect_interval=0.001)

        disconnection_called = [0]
        original_handle = sender._handle_disconnection

        def tracking_handle():
            disconnection_called[0] += 1
            original_handle()

        sender._handle_disconnection = tracking_handle

        call_count = [0]

        async def mock_run():
            call_count[0] += 1
            if call_count[0] == 1:
                raise RuntimeError("stream died")
            sender._closed = True

        sender._run_bidi_stream = mock_run
        await sender._stream_loop()

        assert disconnection_called[0] == 1
        assert call_count[0] == 2


# ==============================================================================
# Additional tests for remaining uncovered lines (134-136, 186-187)
# ==============================================================================


class TestAsyncEventSenderRunBidiStreamOuterGrpcError:
    """Cover lines 133-136: outer AioRpcError handler in _run_bidi_stream.

    The inner _receive_responses catches AioRpcError, but the outer handler
    is reachable if _receive_responses is mocked to raise AioRpcError directly
    (simulating a path where the error propagates differently).
    """

    @pytest.mark.asyncio
    async def test_outer_grpc_error_not_cancelled_raises(self):
        """AioRpcError with non-CANCELLED code from receiver task -> from_grpc_error."""

        import grpc

        sender, transport = _make_sender()

        real_error = grpc.aio.AioRpcError(
            code=grpc.StatusCode.UNAVAILABLE,
            initial_metadata=grpc.aio.Metadata(),
            trailing_metadata=grpc.aio.Metadata(),
            details="server unavailable",
        )

        mock_call = MagicMock()
        mock_call.__aiter__ = MagicMock(return_value=AsyncIteratorMock([]))

        mock_stub = MagicMock()
        mock_stub.SendEventsStream = MagicMock(return_value=mock_call)
        transport._get_stub = MagicMock(return_value=mock_stub)

        async def noop(call):
            pass

        transport._register_stream = noop
        transport._unregister_stream = noop

        sender._closed = True

        # Mock _receive_responses to raise AioRpcError directly, which will
        # propagate through `await receiver_task` into the outer except block
        async def raise_grpc_error(call):
            raise real_error

        sender._receive_responses = raise_grpc_error

        from kubemq.core.exceptions import KubeMQError

        with pytest.raises(KubeMQError):
            await sender._run_bidi_stream()

    @pytest.mark.asyncio
    async def test_outer_grpc_cancelled_suppressed(self):
        """AioRpcError with CANCELLED code from receiver task -> suppressed."""
        import grpc

        sender, transport = _make_sender()

        real_error = grpc.aio.AioRpcError(
            code=grpc.StatusCode.CANCELLED,
            initial_metadata=grpc.aio.Metadata(),
            trailing_metadata=grpc.aio.Metadata(),
            details="cancelled",
        )

        mock_call = MagicMock()
        mock_call.__aiter__ = MagicMock(return_value=AsyncIteratorMock([]))

        mock_stub = MagicMock()
        mock_stub.SendEventsStream = MagicMock(return_value=mock_call)
        transport._get_stub = MagicMock(return_value=mock_stub)

        async def noop(call):
            pass

        transport._register_stream = noop
        transport._unregister_stream = noop

        sender._closed = True

        async def raise_grpc_cancelled(call):
            raise real_error

        sender._receive_responses = raise_grpc_cancelled

        # Should not raise
        await sender._run_bidi_stream()


class TestAsyncEventSenderHandleDisconnectionQueueRace:
    """Cover lines 186-187: asyncio.QueueEmpty caught during drain.

    The drain loop uses `while not self._send_queue.empty(): get_nowait()`.
    A race condition could make empty() return False but get_nowait() raise
    QueueEmpty. We simulate this by mocking the queue.
    """

    def test_queue_empty_caught_during_drain(self):
        """QueueEmpty exception during drain -> break (lines 186-187).

        Simulate a race: empty() returns False but get_nowait() raises QueueEmpty.
        """
        from unittest.mock import patch

        sender, _ = _make_sender()

        # Mock the queue to simulate the race condition:
        # - empty() returns False (looks like there are items)
        # - get_nowait() raises QueueEmpty (items were consumed by another coroutine)
        original_queue = sender._send_queue

        with patch.object(original_queue, "empty", return_value=False):
            with patch.object(original_queue, "get_nowait", side_effect=asyncio.QueueEmpty):
                sender._handle_disconnection()

        assert sender._allow_new_messages is False
