"""Async bidirectional streaming event sender for high-throughput publishing."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from kubemq.grpc import Event, Result

if TYPE_CHECKING:
    from kubemq.transport.async_transport import AsyncTransport

DEFAULT_SEND_QUEUE_SIZE = 10_000
_SENTINEL = object()

_logger = logging.getLogger("kubemq.pubsub.async_event_sender")


class AsyncEventSender:
    """Async counterpart of ``EventSender`` using ``SendEventsStream`` bidi RPC.

    Manages a single background task that drains a bounded asyncio.Queue
    and feeds an async bidirectional stream. Store-flagged events get
    their Result resolved via a tracking Future.
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
        self._lock = asyncio.Lock()
        self._closed = False
        self._allow_new_messages = True
        self._reconnect_interval = reconnect_interval
        self._stream_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Start the background stream loop."""
        if self._stream_task is None or self._stream_task.done():
            self._stream_task = asyncio.create_task(self._stream_loop())

    async def send(self, event: Event) -> Result | None:
        """Enqueue an event for sending.

        For fire-and-forget events (``Store=False``), returns ``None`` immediately.
        For store events (``Store=True``), blocks until a ``Result`` is received.

        Raises:
            ConnectionError: If the sender is not accepting messages.
            asyncio.QueueFull: If the send queue is full.
        """
        if self._closed:
            raise ConnectionError("AsyncEventSender is closed.")
        if not self._allow_new_messages:
            raise ConnectionError("Sender is not ready to accept new messages.")

        if not event.Store:
            try:
                self._send_queue.put_nowait(event)
            except asyncio.QueueFull:
                from kubemq.core.exceptions import KubeMQBufferFullError

                raise KubeMQBufferFullError(
                    "Event send queue is full. The server may be slow or "
                    "disconnected. Reduce send rate or increase max_send_queue_size.",
                    buffer_size=self._send_queue.maxsize,
                ) from None
            return None

        loop = asyncio.get_running_loop()
        future: asyncio.Future[Result] = loop.create_future()

        async with self._lock:
            self._response_tracking[event.EventID] = future

        await self._send_queue.put(event)
        try:
            return await future
        finally:
            async with self._lock:
                self._response_tracking.pop(event.EventID, None)

    async def _stream_loop(self) -> None:
        """Outer reconnection loop wrapping the bidi stream."""
        while not self._closed:
            try:
                self._allow_new_messages = True
                async for response in self._transport.send_events_stream(self._request_generator()):
                    if self._closed:
                        break
                    await self._process_response(response)
            except Exception as e:
                if self._closed:
                    break
                _logger.warning("Event stream error: %s", e)
                await self._handle_disconnection()
                await asyncio.sleep(self._reconnect_interval)

    async def _request_generator(self) -> AsyncIterator[Event]:
        """Drain the send queue and yield events to the bidi stream."""
        while not self._closed:
            try:
                msg = await asyncio.wait_for(self._send_queue.get(), timeout=1.0)
            except TimeoutError:
                continue
            if msg is _SENTINEL:
                break
            yield msg  # type: ignore[misc]

    async def _process_response(self, response: Result) -> None:
        """Resolve the Future for a store-event response."""
        async with self._lock:
            future = self._response_tracking.get(response.EventID)
            if future and not future.done():
                future.set_result(response)

    async def _handle_disconnection(self) -> None:
        """Signal error to all pending store-event Futures and drain the queue."""
        async with self._lock:
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

        if self._stream_task and not self._stream_task.done():
            self._stream_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._stream_task

        await self._handle_disconnection()
