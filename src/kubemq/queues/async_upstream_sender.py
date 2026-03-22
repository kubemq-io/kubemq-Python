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
        while not self._closed:
            try:
                self._allow_new_messages = True
                await self._run_bidi_stream()
            except Exception as e:
                if self._closed:
                    break
                _logger.warning("Queue upstream stream error: %s", e)
                self._handle_disconnection()
                await asyncio.sleep(self._reconnect_interval)

    async def _run_bidi_stream(self) -> None:
        """Open bidi stream with concurrent send/receive."""
        import grpc
        stub = self._transport._get_stub()
        call = stub.QueuesUpstream(self._request_generator())
        await self._transport._register_stream(call)

        try:
            receiver_task = asyncio.create_task(self._receive_responses(call))
            try:
                await receiver_task
            except asyncio.CancelledError:
                pass
        except grpc.aio.AioRpcError as e:
            if e.code() != grpc.StatusCode.CANCELLED:
                from kubemq.core.exceptions import from_grpc_error
                raise from_grpc_error(e) from e
        finally:
            await self._transport._unregister_stream(call)

    async def _receive_responses(self, call) -> None:
        """Read responses from the bidi stream and resolve futures."""
        import grpc
        try:
            async for response in call:
                if self._closed:
                    break
                self._process_response(response)
        except grpc.aio.AioRpcError as e:
            if e.code() != grpc.StatusCode.CANCELLED:
                from kubemq.core.exceptions import from_grpc_error
                raise from_grpc_error(e) from e

    async def _request_generator(self) -> AsyncIterator[QueuesUpstreamRequest]:
        """Drain the send queue and yield requests to the bidi stream."""
        while not self._closed:
            msg = await self._send_queue.get()
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

        with contextlib.suppress(asyncio.QueueFull):
            self._send_queue.put_nowait(_SENTINEL)

        if self._stream_task and not self._stream_task.done():
            self._stream_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._stream_task

        self._handle_disconnection()
