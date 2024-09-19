import logging
import threading
import queue
import time
from time import sleep

import grpc
from kubemq.transport import Transport, Connection
from kubemq.grpc import QueuesDownstreamRequest, QueuesDownstreamResponse
from kubemq.common import *

class DownstreamReceiver:
    """
    Class representing a downstream receiver for sending requests to a server.

    Attributes:
        clientStub (Transport): The transport client stub.
        connection (Connection): The connection to the server.
        shutdown_event (threading.Event): The event used to indicate shutdown.
        logger (logging.Logger): The logger for logging messages.
        lock (threading.Lock): The lock used for thread safety.
        response_tracking (dict): A dictionary for tracking response containers and result events.
        queue (queue.Queue): The queue used for storing requests.
        allow_new_requests (bool): Flag indicating whether new requests are allowed.

    Methods:
        send(request: QueuesDownstreamRequest) -> [QueuesDownstreamResponse, None]:
            Sends a request to the server and waits for a response.

            Args:
                request (QueuesDownstreamRequest): The request to send.

            Returns:
                [QueuesDownstreamResponse, None]: The response from the server, or None if an exception occurred.

        send_without_response(request: QueuesDownstreamRequest):
            Sends a request to the server without waiting for a response.

            Args:
                request (QueuesDownstreamRequest): The request to send.

        _handle_disconnection():
            Handles disconnection from the server by clearing the queue and setting error on response containers.

        _send_queue_stream():
            Continuously sends requests from the queue to the server and handles responses.

    """
    def __init__(self, transport: Transport, logger: logging.Logger,
                 connection: Connection):
        self.clientStub = transport.kubemq_client()
        self.connection = connection
        self.shutdown_event = threading.Event()
        self.logger = logger
        self.lock = threading.Lock()
        self.response_tracking = {}
        self.queue = queue.Queue()
        self.allow_new_requests = True
        threading.Thread(target=self._send_queue_stream, args=(), daemon=True).start()

    def send(self, request: QueuesDownstreamRequest) -> [QueuesDownstreamResponse, None]:
        try:
            if not self.allow_new_requests:
                raise ConnectionError("Client is not connected to the server and cannot accept new requests")
            response_result = threading.Event()
            response_container = {}
            with self.lock:
                self.response_tracking[request.RequestID] = (response_container, response_result)
            self.queue.put(request)
            response_result.wait()
            response: QueuesDownstreamResponse = response_container.get('response')
            with self.lock:
                if response is not None:
                    if self.response_tracking.get(request.RequestID):
                        del self.response_tracking[request.RequestID]

            return response
        except Exception as e:
            return QueuesDownstreamResponse(
                RefRequestId=request.RequestID,
                IsError=True,
                Error=str(e)
            )

    def send_without_response(self, request: QueuesDownstreamRequest):
        if not self.allow_new_requests:
            raise ConnectionError("Client is not connected to the server and cannot accept new requests")
        self.queue.put(request)

    def _handle_disconnection(self):
        with self.lock:
            self.allow_new_requests = False
            while not self.queue.empty():
                try:
                    self.queue.get_nowait()  # Clear the queue
                except queue.Empty:
                    continue

            # Set error on all response containers
            for request_id, (response_container, response_result) in self.response_tracking.items():
                response_container['response'] = QueuesDownstreamResponse(
                    RefRequestId=request_id,
                    IsError=True,
                    Error="Error: Disconnected from server"
                )
                response_result.set()  # Signal that the response has been processed
            self.response_tracking.clear()

    def _send_queue_stream(self):
        def send_requests():
            while not self.shutdown_event.is_set():
                try:
                    msg = self.queue.get(timeout=1)  # timeout to check for shutdown event periodically
                    yield msg
                except queue.Empty:
                    continue

        while not self.shutdown_event.is_set():
            try:
                with self.lock:
                    self.allow_new_requests = True
                responses = self.clientStub.QueuesDownstream(send_requests())
                for response in responses:
                    if self.shutdown_event.is_set():
                        break
                    response_request_id = response.RefRequestId
                    with self.lock:
                        if response_request_id in self.response_tracking:
                            response_container, response_result = self.response_tracking[
                                response_request_id]
                            response_container['response'] = response
                            response_result.set()
            except grpc.RpcError as e:
                self.logger.debug(decode_grpc_error(e))
                self._handle_disconnection()
                time.sleep(self.connection.reconnect_interval_seconds)
                continue
            except Exception as e:
                self.logger.debug(f"Error: {str(e)}")
                self._handle_disconnection()
                time.sleep(self.connection.reconnect_interval_seconds)
                continue
    def close(self):
        self.allow_new_requests = False
        self.shutdown_event.set()
        self.queue.put(None)
        self.logger.debug("Downstream receiver shutdown")