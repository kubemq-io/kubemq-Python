"""Async downstream receiver: persistent bidi stream for queue receive + ack/nack.

Mirrors the sync DownstreamReceiver pattern:
1. Maintains a single persistent QueuesDownstream bidi stream
2. Sends Get requests to receive messages (server returns TransactionId)
3. Sends Ack/Nack/ReQueue on the SAME stream with RefTransactionId set
4. Separate send/receive tasks for true concurrency
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

import grpc

from kubemq.grpc import (
    QueuesDownstreamRequest,
    QueuesDownstreamRequestType,
    QueuesDownstreamResponse,
)

if TYPE_CHECKING:
    from kubemq.transport.async_transport import AsyncTransport

_SENTINEL = object()

_logger = logging.getLogger("kubemq.queues.async_downstream_receiver")


class AsyncDownstreamReceiver:
    """Async persistent bidi stream for queue downstream operations.

    Maintains a single QueuesDownstream bidi stream. Get requests and
    Ack/Nack/ReQueue operations all go through the same stream, preserving
    the TransactionId context that the server requires.
    """

    # Safety-net timeout for awaiting a response from the bidi stream.
    # Prevents callers from blocking forever if disconnection handling
    # fails to resolve the future for any reason.
    _DEFAULT_RESPONSE_TIMEOUT: float = 300.0  # 5 minutes

    def __init__(
        self,
        transport: AsyncTransport,
        *,
        reconnect_interval: float = 1.0,
        response_timeout: float = _DEFAULT_RESPONSE_TIMEOUT,
    ) -> None:
        self._transport = transport
        self._send_queue: asyncio.Queue[QueuesDownstreamRequest | object] = asyncio.Queue(
            maxsize=10_000,
        )
        self._response_tracking: dict[str, asyncio.Future[QueuesDownstreamResponse]] = {}
        self._closed = False
        self._allow_new_requests = True
        self._reconnect_interval = reconnect_interval
        self._response_timeout = response_timeout
        self._stream_task: asyncio.Task[None] | None = None
        # Per-stream-iteration stop event.  Set when _run_bidi_stream exits
        # so the old _request_generator wakes up and terminates instead of
        # stealing messages from the next stream's generator.
        self._generator_stop: asyncio.Event = asyncio.Event()
        # Readiness signal: set once the bidi stream is established.
        self._stream_ready: asyncio.Event = asyncio.Event()

    async def start(self) -> None:
        """Start the background stream loop and wait for the stream to be ready."""
        if self._stream_task is None or self._stream_task.done():
            self._stream_ready.clear()
            self._stream_task = asyncio.create_task(self._stream_loop())
            try:
                await asyncio.wait_for(self._stream_ready.wait(), timeout=10.0)
            except TimeoutError:
                _logger.warning("Timed out waiting for downstream stream to become ready")

    async def send(self, request: QueuesDownstreamRequest) -> QueuesDownstreamResponse | None:
        """Send a request and wait for its response.

        Used for Get requests — caller awaits the response which contains
        TransactionId and messages.
        """
        if self._closed:
            raise ConnectionError("AsyncDownstreamReceiver is closed.")
        if not self._allow_new_requests:
            raise ConnectionError("Receiver is not ready to accept new requests.")

        loop = asyncio.get_running_loop()
        future: asyncio.Future[QueuesDownstreamResponse] = loop.create_future()
        self._response_tracking[request.RequestID] = future

        await self._send_queue.put(request)
        try:
            return await asyncio.wait_for(future, timeout=self._response_timeout)
        except TimeoutError:
            _logger.warning(
                "Downstream response timeout after %.0fs for request %s",
                self._response_timeout,
                request.RequestID,
            )
            return None
        finally:
            self._response_tracking.pop(request.RequestID, None)

    async def send_without_response(self, request: QueuesDownstreamRequest) -> None:
        """Send a request without waiting for a response.

        Used for Ack/Nack/ReQueue — fire-and-forget on the same stream.
        """
        if self._closed:
            raise ConnectionError("AsyncDownstreamReceiver is closed.")
        if not self._allow_new_requests:
            raise ConnectionError("Receiver is not ready to accept new requests.")

        await self._send_queue.put(request)

    async def _stream_loop(self) -> None:
        """Outer reconnection loop."""
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
                self._send_queue = asyncio.Queue(maxsize=10_000)
                self._allow_new_requests = True
                _logger.debug("Downstream stream (re)connecting...")
                await self._run_bidi_stream()
            except Exception as e:
                if self._closed:
                    break
                _logger.warning("Downstream stream error: %s", e)
            finally:
                # Signal the current generator to stop so it does not linger
                # and steal messages from the next stream's generator.
                self._generator_stop.set()
                if not self._closed:
                    # Always clean up pending futures on stream end (error OR
                    # clean exit) so callers blocked on receiver.send() are
                    # unblocked and can retry on the next stream iteration.
                    self._handle_disconnection()
                    await asyncio.sleep(self._reconnect_interval)

    async def _run_bidi_stream(self) -> None:
        """Open bidi stream with concurrent send/receive."""
        stub = self._transport._get_stub()

        # Capture the stop event for THIS stream iteration so the generator
        # is bound to the correct event even after _stream_loop replaces it.
        stop_event = self._generator_stop
        call: grpc.aio.StreamStreamCall = stub.QueuesDownstream(
            self._request_generator(stop_event),
        )
        await self._transport._register_stream(call)

        try:
            receiver_task = asyncio.create_task(self._receive_responses(call))
            # Signal that the stream is ready for requests.
            self._stream_ready.set()
            try:
                await receiver_task
            except asyncio.CancelledError:
                pass
        except grpc.aio.AioRpcError as e:
            if e.code() != grpc.StatusCode.CANCELLED:
                if self._transport._is_connection_error(e):
                    await self._transport._on_connection_lost()
                from kubemq.core.exceptions import from_grpc_error
                raise from_grpc_error(e) from e
        finally:
            # Signal the generator bound to this call to stop before
            # unregistering the stream, so it won't keep pulling from
            # the queue after the gRPC call is dead.
            stop_event.set()
            await self._transport._unregister_stream(call)

    async def _request_generator(
        self, stop_event: asyncio.Event,
    ) -> AsyncIterator[QueuesDownstreamRequest]:
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

    async def _receive_responses(self, call: grpc.aio.StreamStreamCall) -> None:
        """Read responses and resolve matching futures."""
        try:
            async for response in call:
                if self._closed:
                    break
                if response.RequestTypeData == QueuesDownstreamRequestType.CloseByServer:
                    _logger.warning("Server initiated stream close")
                    self._handle_disconnection()
                    break
                self._allow_new_requests = True
                request_id = response.RefRequestId
                future = self._response_tracking.get(request_id)
                if future and not future.done():
                    future.set_result(response)
        except asyncio.CancelledError:
            _logger.debug("Downstream response reader cancelled")
        except grpc.aio.AioRpcError as e:
            if e.code() != grpc.StatusCode.CANCELLED:
                if self._transport._is_connection_error(e):
                    await self._transport._on_connection_lost()
                from kubemq.core.exceptions import from_grpc_error
                raise from_grpc_error(e) from e

    def _handle_disconnection(self) -> None:
        """Signal error to all pending futures."""
        self._allow_new_requests = False
        for request_id, future in self._response_tracking.items():
            if not future.done():
                error_resp = QueuesDownstreamResponse(
                    RefRequestId=request_id,
                    IsError=True,
                    Error="Error: Disconnected from server",
                )
                future.set_result(error_resp)
        self._response_tracking.clear()

        while not self._send_queue.empty():
            try:
                self._send_queue.get_nowait()
            except asyncio.QueueEmpty:
                break

    async def close(self) -> None:
        """Shut down the receiver."""
        if self._closed:
            return
        self._closed = True
        self._allow_new_requests = False

        # Stop any active generator.
        self._generator_stop.set()

        with contextlib.suppress(asyncio.QueueFull):
            self._send_queue.put_nowait(_SENTINEL)

        if self._stream_task and not self._stream_task.done():
            self._stream_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._stream_task

        self._handle_disconnection()
