"""Async bidirectional streaming upstream sender for high-throughput queue sending."""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Optional

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

    async def start(self) -> None:
        """Start the background stream loop."""
        if self._stream_task is None or self._stream_task.done():
            self._stream_task = asyncio.create_task(self._stream_loop())

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
        request.RequestID = str(uuid.uuid4())
        request.Messages.append(message)

        loop = asyncio.get_running_loop()
        future: asyncio.Future[QueuesUpstreamResponse] = loop.create_future()

        async with self._lock:
            self._response_tracking[request.RequestID] = (future, message_id)

        try:
            self._send_queue.put_nowait(request)
        except asyncio.QueueFull:
            async with self._lock:
                self._response_tracking.pop(request.RequestID, None)
            from kubemq.core.exceptions import KubeMQBufferFullError

            raise KubeMQBufferFullError(
                "Queue send queue is full. The server may be slow or "
                "disconnected. Reduce send rate or increase max_send_queue_size.",
                buffer_size=self._send_queue.maxsize,
            ) from None

        try:
            response = await asyncio.wait_for(future, timeout=self._send_timeout)
        except asyncio.TimeoutError:
            async with self._lock:
                self._response_tracking.pop(request.RequestID, None)
            return QueueSendResult(
                id=message_id,
                is_error=True,
                error="Error: Timeout waiting for response",
            )
        finally:
            async with self._lock:
                self._response_tracking.pop(request.RequestID, None)

        if response.Results:
            return QueueSendResult.decode(response.Results[0])
        return QueueSendResult(id=message_id, is_error=True, error="Empty response from server")

    async def _stream_loop(self) -> None:
        """Outer reconnection loop wrapping the bidi stream."""
        while not self._closed:
            try:
                self._allow_new_messages = True
                async for response in self._transport.queues_upstream(self._request_generator()):
                    if self._closed:
                        break
                    await self._process_response(response)
            except Exception as e:
                if self._closed:
                    break
                _logger.warning("Queue upstream stream error: %s", e)
                await self._handle_disconnection()
                await asyncio.sleep(self._reconnect_interval)

    async def _request_generator(self) -> AsyncIterator[QueuesUpstreamRequest]:
        """Drain the send queue and yield requests to the bidi stream."""
        while not self._closed:
            try:
                msg = await asyncio.wait_for(self._send_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            if msg is _SENTINEL:
                break
            yield msg  # type: ignore[misc]

    async def _process_response(self, response: QueuesUpstreamResponse) -> None:
        """Resolve the Future for a matched RefRequestID."""
        async with self._lock:
            entry = self._response_tracking.get(response.RefRequestID)
            if entry:
                future, _ = entry
                if not future.done():
                    future.set_result(response)

    async def _handle_disconnection(self) -> None:
        """Signal error to all pending Futures and drain the queue."""
        async with self._lock:
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

        try:
            self._send_queue.put_nowait(_SENTINEL)
        except asyncio.QueueFull:
            pass

        if self._stream_task and not self._stream_task.done():
            self._stream_task.cancel()
            try:
                await self._stream_task
            except asyncio.CancelledError:
                pass

        await self._handle_disconnection()
