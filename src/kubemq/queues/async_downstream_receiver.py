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

    def __init__(
        self,
        transport: AsyncTransport,
        *,
        reconnect_interval: float = 1.0,
    ) -> None:
        self._transport = transport
        self._send_queue: asyncio.Queue[QueuesDownstreamRequest | object] = asyncio.Queue(
            maxsize=10_000,
        )
        self._response_tracking: dict[str, asyncio.Future[QueuesDownstreamResponse]] = {}
        self._closed = False
        self._allow_new_requests = True
        self._reconnect_interval = reconnect_interval
        self._stream_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Start the background stream loop."""
        if self._stream_task is None or self._stream_task.done():
            self._stream_task = asyncio.create_task(self._stream_loop())

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
            return await future
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
        while not self._closed:
            try:
                self._allow_new_requests = True
                await self._run_bidi_stream()
            except Exception as e:
                if self._closed:
                    break
                _logger.warning("Downstream stream error: %s", e)
                self._handle_disconnection()
                await asyncio.sleep(self._reconnect_interval)

    async def _run_bidi_stream(self) -> None:
        """Open bidi stream with concurrent send/receive."""
        stub = self._transport._get_stub()

        call: grpc.aio.StreamStreamCall = stub.QueuesDownstream(self._request_generator())
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

    async def _request_generator(self) -> AsyncIterator[QueuesDownstreamRequest]:
        """Drain the send queue and yield requests to the bidi stream."""
        while not self._closed:
            msg = await self._send_queue.get()
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
        except grpc.aio.AioRpcError as e:
            if e.code() != grpc.StatusCode.CANCELLED:
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

        with contextlib.suppress(asyncio.QueueFull):
            self._send_queue.put_nowait(_SENTINEL)

        if self._stream_task and not self._stream_task.done():
            self._stream_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._stream_task

        self._handle_disconnection()
