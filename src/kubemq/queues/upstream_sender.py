from __future__ import annotations

import logging
import queue
import threading
import time
import uuid
from collections.abc import Generator, Iterator

import grpc

from kubemq.common.helpers import decode_grpc_error, is_channel_error
from kubemq.core.config import ClientConfig
from kubemq.grpc import (
    QueueMessage as pbQueueMessage,
    QueuesUpstreamRequest,
    QueuesUpstreamResponse,
    SendQueueMessageResult,
)
from kubemq.queues.queues_send_result import QueueSendResult
from kubemq.transport import SyncTransport

DEFAULT_SEND_QUEUE_SIZE = 10_000


class UpstreamSender:
    """Class representing an upstream sender for sending messages to a KubeMQ server.

    This class manages a continuous stream of messages to the server and processes
    responses asynchronously using a background thread. It provides methods for
    sending messages and handling responses.

    Thread Safety:
        - All shared state is protected by locks to ensure thread safety
        - The background thread is started in __init__ and runs until close() is called
        - Response tracking is managed through a thread-safe dictionary

    Error Handling:
        - Connection errors trigger automatic reconnection attempts
        - Errors in send() are returned as error responses

    Attributes:
        transport (SyncTransport): The transport object for channel management.
        clientStub: The transport client stub.
        connection (Connection): The connection to the server.
        shutdown_event (threading.Event): The event used to indicate shutdown.
        logger (logging.Logger): The logger for logging messages.
        lock (threading.Lock): The lock used for thread safety.
        response_tracking (dict): A dictionary for tracking response containers and result events.
        sending_queue (queue.Queue): The queue used for storing messages to be sent.
        allow_new_messages (bool): Flag indicating whether new messages are allowed.
        queue_timeout (float): Timeout in seconds for queue polling.
        request_sleep_interval (float): Sleep interval in seconds between requests.
        send_timeout (float): Timeout in seconds for waiting for a send response.
    """

    def __init__(
        self,
        transport: SyncTransport,
        logger: logging.Logger,
        config: ClientConfig,
        send_timeout: float = 2.0,
        *,
        max_queue_size: int = DEFAULT_SEND_QUEUE_SIZE,
    ):
        """Initialize a new UpstreamSender.

        Args:
            transport: The transport object for channel management
            logger: The logger for logging messages
            config: The client configuration
            send_timeout: Timeout in seconds for waiting for a send response
            max_queue_size: Maximum size of the sending queue.
        """
        self.transport = transport
        self.clientStub = transport.kubemq_client()
        self._config = config
        self.shutdown_event = threading.Event()
        self.logger = logger
        self.lock = threading.Lock()
        self.response_tracking: dict[str, tuple[dict[str, object], threading.Event, str]] = {}
        self.sending_queue: queue.Queue[QueuesUpstreamRequest | None] = queue.Queue(
            maxsize=max_queue_size
        )
        self.allow_new_messages = True
        self.send_timeout = send_timeout
        threading.Thread(target=self._send_queue_stream, args=(), daemon=True).start()

    def send(self, message: pbQueueMessage) -> QueueSendResult | None:
        """Send a message to the server.

        Args:
            message: The message to send to the server

        Returns:
            The result of the send operation, or None if an exception occurred

        Raises:
            ConnectionError: If the client is not connected or not ready to accept messages
        """
        try:
            if not self.transport.is_connected():
                raise ConnectionError(
                    "Client is not connected to the server and cannot send messages."
                )
            if not self.allow_new_messages:
                raise ConnectionError("Sender is not ready to accept new messages.")

            response_result = threading.Event()
            response_container: dict[str, object] = {}
            message_id = message.MessageID
            queue_upstream_request = QueuesUpstreamRequest()
            queue_upstream_request.RequestID = str(uuid.uuid4())
            queue_upstream_request.Messages.append(message)
            with self.lock:
                self.response_tracking[queue_upstream_request.RequestID] = (
                    response_container,
                    response_result,
                    message_id,
                )
            maxsize = self.sending_queue.maxsize
            if maxsize > 0:
                current = self.sending_queue.qsize()
                utilization = current / maxsize
                if utilization >= 0.9:
                    self.logger.warning(
                        "Send queue at %.0f%% capacity (%d/%d)",
                        utilization * 100,
                        current,
                        maxsize,
                    )
            try:
                self.sending_queue.put_nowait(queue_upstream_request)
            except queue.Full:
                from kubemq.core.exceptions import KubeMQBufferFullError

                raise KubeMQBufferFullError(
                    "Queue send queue is full. The server may be slow or "
                    "disconnected. Reduce send rate or increase max_send_queue_size.",
                    buffer_size=self.sending_queue.maxsize,
                ) from None
            response_result.wait(self.send_timeout)
            response_raw = response_container.get("response")
            response: QueuesUpstreamResponse | None = (
                response_raw if isinstance(response_raw, QueuesUpstreamResponse) else None
            )
            with self.lock:
                if self.response_tracking.get(queue_upstream_request.RequestID):
                    del self.response_tracking[queue_upstream_request.RequestID]
                if response is None:
                    return QueueSendResult(
                        id=message_id,
                        is_error=True,
                        error="Error: Timeout waiting for response",
                    )
            send_result = response.Results[0]
            return QueueSendResult().decode(send_result)
        except Exception as e:
            self.logger.error(f"Error sending message: {str(e)}")
            return QueueSendResult(id=message.MessageID, is_error=True, error=str(e))

    def _handle_disconnection(self) -> None:
        """Handle disconnection from the server.

        Sets error responses for all pending requests and clears the tracking dictionary.
        """
        with self.lock:
            self.allow_new_messages = False
            for request_id, (
                response_container,
                response_result,
                message_id,
            ) in self.response_tracking.items():
                response_container["response"] = QueuesUpstreamResponse(
                    RefRequestID=request_id,
                    Results=[
                        SendQueueMessageResult(
                            MessageID=message_id,
                            IsError=True,
                            Error="Error: Disconnected from server",
                        )
                    ],
                )
                response_result.set()  # Signal that the response has been processed
            self.response_tracking.clear()

    def _generate_requests(self) -> Generator[QueuesUpstreamRequest, None, None]:
        """Generate requests from the queue to send to the server.

        Uses timeout on queue.get() to allow clean shutdown.
        Checks for None sentinel to exit cleanly.

        Yields:
            The next request to send
        """
        while not self.shutdown_event.is_set():
            try:
                msg = self.sending_queue.get(timeout=1.0)
                if msg is None:  # Sentinel value for shutdown
                    break
                yield msg
            except queue.Empty:
                continue

    def _process_responses(self, responses: Iterator[QueuesUpstreamResponse]) -> None:
        """Process responses from the server.

        Args:
            responses: Iterator of responses from the server
        """
        for response in responses:
            if self.shutdown_event.is_set():
                break
            response_request_id = response.RefRequestID
            with self.lock:
                self.allow_new_messages = True
                if response_request_id in self.response_tracking:
                    response_container, response_result, message_id = self.response_tracking[
                        response_request_id
                    ]
                    response_container["response"] = response
                    response_result.set()

    def _recreate_channel(self) -> bool:
        """Attempt to recreate the gRPC channel after a connection failure.

        Returns:
            True if channel recreation was successful, False otherwise
        """
        try:
            self.clientStub = self.transport.recreate_channel()
            self.logger.info("Successfully recreated gRPC channel")
            with self.lock:
                self.allow_new_messages = True
            return True
        except ConnectionError as conn_ex:
            if not self._config.auto_reconnect:
                self.logger.warning("Auto-reconnect is disabled, not attempting to reconnect")
                with self.lock:
                    self.allow_new_messages = False
                return False
            else:
                # PY-4: When auto-reconnect is enabled, return True so the
                # outer loop continues retrying instead of giving up.
                self.logger.error(f"Connection error: {str(conn_ex)}")
                return True
        except Exception as channel_ex:
            self.logger.error(
                f"Failed to recreate channel: {str(channel_ex)}, type: {type(channel_ex).__name__}"
            )
            return False

    def _handle_error(self, error: Exception, is_grpc_error: bool = False) -> bool:
        """Handle connection errors and attempt recovery.

        Args:
            error: The exception that occurred
            is_grpc_error: Whether the error is a gRPC-specific error

        Returns:
            True if processing should continue, False if the thread should exit
        """
        if is_grpc_error:
            error_details = decode_grpc_error(error)
            self.logger.error(f"gRPC error type: {type(error).__name__}, error: {error_details}")
            if hasattr(error, "code"):
                self.logger.error(
                    f"gRPC error details: code={error.code()}, details={error.details() if hasattr(error, 'details') else 'N/A'}"
                )
        elif is_channel_error(error):
            self.logger.error(f"Channel error: {str(error)}")
            self.logger.info("Detected channel error in generic exception handler")
        else:
            self.logger.error(
                f"Generic exception type: {type(error).__name__}, error: {str(error)}"
            )

        self._handle_disconnection()

        if is_grpc_error or is_channel_error(error):
            if not self._recreate_channel() and not self._config.auto_reconnect:
                return False  # Exit thread
        else:
            self.logger.error("Non-channel error detected, clearing affected messages only")

        time.sleep(self._config.reconnect_interval_seconds)
        return True  # Continue processing

    def _send_queue_stream(self) -> None:
        """Continuously sends messages from the queue to the server and handles responses."""
        while not self.shutdown_event.is_set():
            try:
                self.clientStub = self.transport.kubemq_client()
                responses = self.clientStub.QueuesUpstream(self._generate_requests())
                self._process_responses(responses)
            except grpc.RpcError as e:
                if not self._handle_error(e, is_grpc_error=True):
                    return  # Exit thread if handling indicates we should stop
                continue
            except Exception as e:
                if not self._handle_error(e):
                    return  # Exit thread if handling indicates we should stop
                continue

    def close(self) -> None:
        """Close the sender and release resources.

        This method stops the background thread and cleans up resources.
        """
        with self.lock:
            self.allow_new_messages = False
        self.shutdown_event.set()
        self.sending_queue.put(None)
