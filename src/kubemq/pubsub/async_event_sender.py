"""Async bidirectional streaming event sender for high-throughput publishing.

Architecture mirrors Go SDK: truly separate send and receive tasks running
concurrently on the same bidi gRPC stream. No locks needed — all operations
run on the single asyncio event loop thread.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

import grpc

from kubemq.grpc import Event, Result

if TYPE_CHECKING:
    from kubemq.transport.async_transport import AsyncTransport

DEFAULT_SEND_QUEUE_SIZE = 50_000
_SENTINEL = object()

_logger = logging.getLogger("kubemq.pubsub.async_event_sender")


class AsyncEventSender:
    """Async bidi streaming event sender with truly concurrent send/receive.

    Two independent background tasks:
    - _sender_task: drains queue → writes to gRPC stream
    - _receiver_task: reads responses from gRPC stream → resolves futures

    This ensures sends are never blocked waiting for response processing
    and vice versa, matching the Go SDK's goroutine-based architecture.
    """

    def __init__(
        self,
        transport: AsyncTransport,
        *,
        max_queue_size: int = DEFAULT_SEND_QUEUE_SIZE,
        reconnect_interval: float = 1.0,
    ) -> None:
        self._transport = transport
        self._send_queue: asyncio.Queue[Event | object] = asyncio.Queue(maxsize=max_queue_size)
        self._response_tracking: dict[str, asyncio.Future[Result]] = {}
        self._closed = False
        self._allow_new_messages = True
        self._reconnect_interval = reconnect_interval
        self._loop_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Start the background stream loop."""
        if self._loop_task is None or self._loop_task.done():
            self._loop_task = asyncio.create_task(self._stream_loop())

    async def send(self, event: Event) -> Result | None:
        """Enqueue an event for sending.

        Fire-and-forget (Store=False): enqueue and return immediately.
        Store (Store=True): enqueue, await server confirmation via Future.
        """
        if self._closed:
            raise ConnectionError("AsyncEventSender is closed.")
        if not self._allow_new_messages:
            raise ConnectionError("Sender is not ready to accept new messages.")

        if not event.Store:
            # Fire-and-forget: non-blocking enqueue.
            # MUST NOT use await put() — that blocks the event loop when the
            # queue is full, starving ALL other tasks (events_store, queues, etc.)
            # Instead, use put_nowait and let the caller handle buffer-full.
            try:
                self._send_queue.put_nowait(event)
            except asyncio.QueueFull:
                # Yield control briefly to let the stream drain, then retry once
                await asyncio.sleep(0)
                try:
                    self._send_queue.put_nowait(event)
                except asyncio.QueueFull:
                    from kubemq.core.exceptions import KubeMQBufferFullError

                    raise KubeMQBufferFullError(
                        "Event send queue is full.",
                        buffer_size=self._send_queue.maxsize,
                    ) from None
            return None

        # Store event: track via Future
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Result] = loop.create_future()
        self._response_tracking[event.EventID] = future

        await self._send_queue.put(event)
        try:
            return await future
        finally:
            self._response_tracking.pop(event.EventID, None)

    async def _stream_loop(self) -> None:
        """Outer reconnection loop."""
        while not self._closed:
            try:
                self._allow_new_messages = True
                await self._run_bidi_stream()
            except Exception as e:
                if self._closed:
                    break
                _logger.warning("Event stream error: %s", e)
                self._handle_disconnection()
                await asyncio.sleep(self._reconnect_interval)

    async def _run_bidi_stream(self) -> None:
        """Open bidi stream and run sender + receiver as separate tasks."""
        stub = self._transport._get_stub()

        # Create the bidi stream with our request generator
        call: grpc.aio.StreamStreamCall = stub.SendEventsStream(self._request_generator())
        await self._transport._register_stream(call)

        try:
            # Run receiver as a separate task — reads responses concurrently
            # while the generator feeds requests on the send side
            receiver_task = asyncio.create_task(self._receive_responses(call))

            # Wait for receiver to finish (stream closed or error)
            with contextlib.suppress(asyncio.CancelledError):
                await receiver_task
        except grpc.aio.AioRpcError as e:
            if e.code() != grpc.StatusCode.CANCELLED:
                from kubemq.core.exceptions import from_grpc_error

                raise from_grpc_error(e) from e
        finally:
            await self._transport._unregister_stream(call)

    async def _request_generator(self) -> AsyncIterator[Event]:
        """Drain the send queue and yield events to the bidi stream.

        Blocks on queue.get() — wakes only when data is available.
        Shutdown via _SENTINEL in the queue (put in close()).
        """
        while not self._closed:
            msg = await self._send_queue.get()
            if msg is _SENTINEL:
                break
            yield msg  # type: ignore[misc]

    async def _receive_responses(self, call: grpc.aio.StreamStreamCall) -> None:
        """Read responses from the bidi stream and resolve futures.

        Runs as a separate task from the send generator, ensuring true
        concurrency between sends and receives.
        """
        try:
            async for response in call:
                if self._closed:
                    break
                future = self._response_tracking.get(response.EventID)
                if future and not future.done():
                    future.set_result(response)
        except grpc.aio.AioRpcError as e:
            if e.code() != grpc.StatusCode.CANCELLED:
                from kubemq.core.exceptions import from_grpc_error

                raise from_grpc_error(e) from e

    def _handle_disconnection(self) -> None:
        """Signal error to all pending Futures and drain the queue."""
        self._allow_new_messages = False
        for event_id, future in self._response_tracking.items():
            if not future.done():
                error_result = Result(
                    EventID=event_id,
                    Sent=False,
                    Error="Error: Disconnected from server",
                )
                future.set_result(error_result)
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

        with contextlib.suppress(asyncio.QueueFull):
            self._send_queue.put_nowait(_SENTINEL)

        if self._loop_task and not self._loop_task.done():
            self._loop_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._loop_task

        self._handle_disconnection()
