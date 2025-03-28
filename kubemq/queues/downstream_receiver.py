import logging
import threading
import queue
import time
from time import sleep
from typing import Optional, Iterator, Generator

import grpc
from kubemq.transport import Transport, Connection
from kubemq.grpc import (
    QueuesDownstreamRequest,
    QueuesDownstreamResponse,
)
from kubemq.common import *
from kubemq.common.helpers import decode_grpc_error, is_channel_error


class DownstreamReceiver:
    """
    Class representing a downstream receiver for sending requests to a KubeMQ server.

    This class manages a continuous stream of requests to the server and processes
    responses asynchronously using a background thread. It provides methods for
    sending requests with or without waiting for responses.

    Thread Safety:
        - All shared state is protected by locks to ensure thread safety
        - The background thread is started in __init__ and runs until close() is called
        - Response tracking is managed through a thread-safe dictionary

    Error Handling:
        - Connection errors trigger automatic reconnection attempts
        - Errors in send() are returned as error responses
        - Errors in send_without_response() are raised as exceptions

    Attributes:
        transport (Transport): The transport object for channel management.
        clientStub (Transport): The transport client stub.
        connection (Connection): The connection to the server.
        shutdown_event (threading.Event): The event used to indicate shutdown.
        logger (logging.Logger): The logger for logging messages.
        lock (threading.Lock): The lock used for thread safety.
        response_tracking (dict): A dictionary for tracking response containers and result events.
        queue (queue.Queue): The queue used for storing requests.
        allow_new_requests (bool): Flag indicating whether new requests are allowed.
        queue_timeout (float): Timeout in seconds for queue polling.
        request_sleep_interval (float): Sleep interval in seconds between requests.
    """

    def __init__(
        self,
        transport: Transport,
        logger: logging.Logger,
        connection: Connection,
        queue_size: int = 0,
        queue_timeout: float = 0.1,
        request_sleep_interval: float = 0.1,
    ):
        """Initialize a new DownstreamReceiver.

        Args:
            transport: The transport object for channel management
            logger: The logger for logging messages
            connection: The connection to the server
            queue_size: Maximum size of the request queue (0 for unlimited)
            queue_timeout: Timeout in seconds for queue polling
            request_sleep_interval: Sleep interval in seconds between requests (0 to disable)
        """
        self.transport = transport
        self.clientStub = transport.kubemq_client()
        self.connection = connection
        self.shutdown_event = threading.Event()
        self.logger = logger
        self.lock = threading.Lock()
        self.response_tracking = {}
        self.queue = queue.Queue(maxsize=queue_size)
        self.allow_new_requests = True
        self.queue_timeout = queue_timeout
        self.request_sleep_interval = request_sleep_interval
        threading.Thread(target=self._send_queue_stream, args=(), daemon=True).start()

    def send(
        self, request: QueuesDownstreamRequest
    ) -> Optional[QueuesDownstreamResponse]:
        """Send a request to the server and wait for a response.

        Args:
            request: The request to send to the server

        Returns:
            The response from the server, or None if an exception occurred

        Raises:
            ConnectionError: If the client is not connected or not ready to accept requests
        """
        try:
            if not self.transport.is_connected():
                raise ConnectionError(
                    "Client is not connected to the server and cannot accept new requests"
                )
            if not self.allow_new_requests:
                raise ConnectionError("Receiver is not ready to accept new requests")

            response_result = threading.Event()
            response_container = {}
            with self.lock:
                self.response_tracking[request.RequestID] = (
                    response_container,
                    response_result,
                )
            self.queue.put(request)
            response_result.wait(request.WaitTimeout / 1000)
            response: QueuesDownstreamResponse = response_container.get("response")
            with self.lock:
                if self.response_tracking.get(request.RequestID):
                    del self.response_tracking[request.RequestID]
            if response is None:
                return QueuesDownstreamResponse(
                    RefRequestId=request.RequestID,
                    IsError=True,
                    Error="Error: Timeout waiting for response",
                )
            return response
        except Exception as e:
            return QueuesDownstreamResponse(
                RefRequestId=request.RequestID, IsError=True, Error=str(e)
            )

    def send_without_response(self, request: QueuesDownstreamRequest) -> None:
        """Send a request to the server without waiting for a response.

        Args:
            request: The request to send to the server

        Raises:
            ConnectionError: If the client is not connected or not ready to accept requests
        """
        if not self.transport.is_connected():
            self.logger.error(
                "Client is not connected to the server and cannot accept new requests"
            )
            raise ConnectionError(
                "Client is not connected to the server and cannot accept new requests"
            )
        if not self.allow_new_requests:
            self.logger.error("Receiver is not ready to accept new requests")
            raise ConnectionError("Receiver is not ready to accept new requests")
        self.queue.put(request)

    def _handle_disconnection(self) -> None:
        """Handle disconnection from the server.

        Sets error responses for all pending requests and clears the tracking dictionary.
        """
        with self.lock:
            self.allow_new_requests = False
            for request_id, (
                response_container,
                response_result,
            ) in self.response_tracking.items():
                response_container["response"] = QueuesDownstreamResponse(
                    RefRequestId=request_id,
                    IsError=True,
                    Error="Error: Disconnected from server",
                )
                response_result.set()  # Signal that the response has been processed
            self.response_tracking.clear()

    def _generate_requests(self) -> Generator[QueuesDownstreamRequest, None, None]:
        """Generate requests from the queue to send to the server.

        Yields:
            The next request to send
        """
        while not self.shutdown_event.is_set():
            try:
                req = self.queue.get(timeout=self.queue_timeout)
                yield req
            except queue.Empty:
                continue
            finally:
                if self.request_sleep_interval > 0:
                    sleep(self.request_sleep_interval)

    def _process_responses(self, responses: Iterator[QueuesDownstreamResponse]) -> None:
        """Process responses from the server.

        Args:
            responses: Iterator of responses from the server
        """
        for response in responses:
            if self.shutdown_event.is_set():
                break
            response_request_id = response.RefRequestId
            with self.lock:
                self.allow_new_requests = True
                if response_request_id in self.response_tracking:
                    response_container, response_result = self.response_tracking[
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
                self.allow_new_requests = True
            return True
        except ConnectionError as conn_ex:
            if self.connection.disable_auto_reconnect:
                self.logger.warning(
                    "Auto-reconnect is disabled, not attempting to reconnect"
                )
                with self.lock:
                    self.allow_new_requests = False
                return False
            else:
                self.logger.error(f"Connection error: {str(conn_ex)}")
                return False
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
            self.logger.error(
                f"gRPC error type: {type(error).__name__}, error: {error_details}"
            )
            if hasattr(error, "code"):
                self.logger.error(
                    f"gRPC error details: code={error.code()}, details={error.details() if hasattr(error, 'details') else 'N/A'}"
                )
        elif is_channel_error(error):
            self.logger.error(f"Channel error: {str(error)}")
        else:
            self.logger.error(
                f"Generic exception type: {type(error).__name__}, error: {str(error)}"
            )

        self._handle_disconnection()

        if is_grpc_error or is_channel_error(error):
            if not self._recreate_channel():
                if self.connection.disable_auto_reconnect:
                    return False  # Exit thread
        else:
            self.logger.error(
                "Non-channel error detected, clearing affected requests only"
            )

        time.sleep(self.connection.get_reconnect_delay())
        return True  # Continue processing

    def _send_queue_stream(self) -> None:
        """Continuously send requests from the queue to the server and handle responses."""
        while not self.shutdown_event.is_set():
            try:
                self.clientStub = self.transport.kubemq_client()
                responses = self.clientStub.QueuesDownstream(self._generate_requests())
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
        """Close the receiver and release resources.

        This method stops the background thread and cleans up resources.
        """
        with self.lock:
            self.allow_new_requests = False
        self.shutdown_event.set()
        self.queue.put(None)
        self.logger.debug("Downstream receiver shutdown")
