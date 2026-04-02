"""Async bidirectional streaming upstream sender for high-throughput queue sending."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from kubemq.common.helpers import fast_id
from kubemq.grpc import (
    QueueMessage as pbQueueMessage,
    QueuesUpstreamRequest,
    QueuesUpstreamResponse,
    SendQueueMessageResult,
)
from kubemq.queues.queues_send_result import QueueSendResult

if TYPE_CHECKING:
    from kubemq.transport.async_transport import AsyncTransport

DEFAULT_SEND_QUEUE_SIZE = 10_000
_SENTINEL = object()

_logger = logging.getLogger("kubemq.queues.async_upstream_sender")


class AsyncUpstreamSender:
    """Async counterpart of ``UpstreamSender`` using ``QueuesUpstream`` bidi RPC.

    Manages a bounded asyncio.Queue that feeds a background task driving
    the bidirectional gRPC stream. Each request gets a Future resolved
    when the server echoes the ``RefRequestID``.
    """

    def __init__(
        self,
        transport: AsyncTransport,
        *,
        max_queue_size: int = DEFAULT_SEND_QUEUE_SIZE,
        send_timeout: float = 2.0,
        reconnect_interval: float = 1.0,
    ) -> None:
        self._transport = transport
        self._send_queue: asyncio.Queue[QueuesUpstreamRequest | object] = asyncio.Queue(
            maxsize=max_queue_size
        )
        self._response_tracking: dict[str, tuple[asyncio.Future[QueuesUpstreamResponse], str]] = {}
        self._lock = asyncio.Lock()
        self._closed = False
        self._allow_new_messages = True
        self._send_timeout = send_timeout
        self._reconnect_interval = reconnect_interval
        self._stream_task: asyncio.Task[None] | None = None
        # Per-stream-iteration stop event.  Set when _run_bidi_stream exits
        # so the old _request_generator wakes up and terminates instead of
        # stealing messages from the next stream's generator.
        self._generator_stop: asyncio.Event = asyncio.Event()
        # Readiness signal: set once the bidi stream is established and the
        # request generator is active.  Prevents send() from timing out
        # because the stream hasn't been created yet.
        self._stream_ready: asyncio.Event = asyncio.Event()

    async def start(self) -> None:
        """Start the background stream loop and wait for the stream to be ready."""
        if self._stream_task is None or self._stream_task.done():
            self._stream_ready.clear()
            self._stream_task = asyncio.create_task(self._stream_loop())
            # Wait for the bidi stream to be established before returning,
            # so the first send() doesn't race against stream creation.
            try:
                await asyncio.wait_for(self._stream_ready.wait(), timeout=10.0)
            except TimeoutError:
                _logger.warning("Timed out waiting for upstream stream to become ready")

    async def send(self, message: pbQueueMessage) -> QueueSendResult:
        """Enqueue a queue message for sending via the bidi stream.

        Blocks until a response is received or send_timeout expires.

        Returns:
            QueueSendResult with send confirmation or error.

        Raises:
            ConnectionError: If the sender is closed or not accepting messages.
        """
        if self._closed:
            raise ConnectionError("AsyncUpstreamSender is closed.")
        if not self._allow_new_messages:
            raise ConnectionError("Sender is not ready to accept new messages.")

        message_id = message.MessageID
        request = QueuesUpstreamRequest()
        request.RequestID = fast_id()
        request.Messages.append(message)

        loop = asyncio.get_running_loop()
        future: asyncio.Future[QueuesUpstreamResponse] = loop.create_future()

        self._response_tracking[request.RequestID] = (future, message_id)

        try:
            self._send_queue.put_nowait(request)
        except asyncio.QueueFull:
            self._response_tracking.pop(request.RequestID, None)
            from kubemq.core.exceptions import KubeMQBufferFullError

            raise KubeMQBufferFullError(
                "Queue send queue is full. The server may be slow or "
                "disconnected. Reduce send rate or increase max_send_queue_size.",
                buffer_size=self._send_queue.maxsize,
            ) from None

        try:
            response = await asyncio.wait_for(future, timeout=self._send_timeout)
        except TimeoutError:
            self._response_tracking.pop(request.RequestID, None)
            return QueueSendResult(
                id=message_id,
                is_error=True,
                error="Error: Timeout waiting for response",
            )
        finally:
            self._response_tracking.pop(request.RequestID, None)

        if response.Results:
            return QueueSendResult.decode(response.Results[0])
        return QueueSendResult(id=message_id, is_error=True, error="Empty response from server")

    async def _stream_loop(self) -> None:
        """Outer reconnection loop wrapping the bidi stream."""
        if self._closed:
            self._stream_ready.set()
            return
        while not self._closed:
            try:
                # Reset the generator stop event and create a fresh send queue
                # for this stream iteration.  Any old _request_generator still
                # blocked on the previous queue will see its stop event set
                # (below in finally) and exit cleanly.
                self._generator_stop = asyncio.Event()
                self._send_queue = asyncio.Queue(maxsize=DEFAULT_SEND_QUEUE_SIZE)
                self._allow_new_messages = True
                _logger.debug("Upstream stream (re)connecting...")
                await self._run_bidi_stream()
            except Exception as e:
                if self._closed:
                    break
                _logger.warning("Queue upstream stream error: %s", e)
            finally:
                # Signal the current generator to stop so it does not linger
                # and steal messages from the next stream's generator.
                self._generator_stop.set()
                if not self._closed:
                    # Always clean up pending futures on stream end (error OR
                    # clean exit) so callers blocked on sender.send() are
                    # unblocked and can retry on the next stream iteration.
                    self._handle_disconnection()
                    await asyncio.sleep(self._reconnect_interval)

    async def _run_bidi_stream(self) -> None:
        """Open bidi stream with concurrent send/receive."""
        import grpc
        stub = self._transport._get_stub()

        # Capture the stop event for THIS stream iteration so the generator
        # is bound to the correct event even after _stream_loop replaces it.
        stop_event = self._generator_stop
        call = stub.QueuesUpstream(self._request_generator(stop_event))
        await self._transport._register_stream(call)

        try:
            receiver_task = asyncio.create_task(self._receive_responses(call))
            # Signal that the stream is ready for sending.
            self._stream_ready.set()
            try:
                await receiver_task
            except asyncio.CancelledError:
                pass
        except grpc.aio.AioRpcError as e:
            if e.code() != grpc.StatusCode.CANCELLED:
                from kubemq.core.exceptions import from_grpc_error
                raise from_grpc_error(e) from e
        finally:
            # Signal the generator bound to this call to stop before
            # unregistering the stream.
            stop_event.set()
            await self._transport._unregister_stream(call)

    async def _receive_responses(self, call) -> None:
        """Read responses from the bidi stream and resolve futures."""
        import grpc
        try:
            async for response in call:
                if self._closed:
                    break
                self._process_response(response)
        except asyncio.CancelledError:
            _logger.debug("Upstream response reader cancelled")
        except grpc.aio.AioRpcError as e:
            if e.code() != grpc.StatusCode.CANCELLED:
                from kubemq.core.exceptions import from_grpc_error
                raise from_grpc_error(e) from e

    async def _request_generator(
        self, stop_event: asyncio.Event,
    ) -> AsyncIterator[QueuesUpstreamRequest]:
        """Drain the send queue and yield requests to the bidi stream.

        The *stop_event* is set when the owning bidi stream ends.  This
        ensures the generator exits promptly instead of blocking forever
        on ``_send_queue.get()`` and stealing messages from the next
        stream iteration's generator.
        """
        while not self._closed and not stop_event.is_set():
            # Race between a new message arriving and the stop signal.
            get_task = asyncio.ensure_future(self._send_queue.get())
            stop_task = asyncio.ensure_future(stop_event.wait())
            done, pending = await asyncio.wait(
                {get_task, stop_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for p in pending:
                p.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await p

            if stop_task in done:
                # Stream ended — exit generator.  If get_task also
                # completed, put the message back so it isn't lost.
                if get_task in done:
                    msg = get_task.result()
                    if msg is not _SENTINEL:
                        with contextlib.suppress(asyncio.QueueFull):
                            self._send_queue.put_nowait(msg)
                break

            msg = get_task.result()
            if msg is _SENTINEL:
                break
            yield msg  # type: ignore[misc]

    def _process_response(self, response: QueuesUpstreamResponse) -> None:
        """Resolve the Future for a matched RefRequestID (lock-free)."""
        entry = self._response_tracking.get(response.RefRequestID)
        if entry:
            future, _ = entry
            if not future.done():
                future.set_result(response)

    def _handle_disconnection(self) -> None:
        """Signal error to all pending Futures and drain the queue."""
        self._allow_new_messages = False
        for request_id, (future, message_id) in self._response_tracking.items():
            if not future.done():
                error_response = QueuesUpstreamResponse(
                    RefRequestID=request_id,
                    Results=[
                        SendQueueMessageResult(
                            MessageID=message_id,
                            IsError=True,
                            Error="Error: Disconnected from server",
                        )
                    ],
                )
                future.set_result(error_response)
        self._response_tracking.clear()

        while not self._send_queue.empty():
            try:
                self._send_queue.get_nowait()
            except asyncio.QueueEmpty:
                break

    async def close(self) -> None:
        """Shut down the sender and cancel the background task."""
        if self._closed:
            return
        self._closed = True
        self._allow_new_messages = False

        # Stop any active generator.
        self._generator_stop.set()

        with contextlib.suppress(asyncio.QueueFull):
            self._send_queue.put_nowait(_SENTINEL)

        if self._stream_task and not self._stream_task.done():
            self._stream_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._stream_task

        self._handle_disconnection()
