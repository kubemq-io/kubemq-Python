import logging
import threading
import queue
import time
from time import sleep

import grpc
from kubemq.transport import Transport, Connection
from kubemq.grpc import QueuesDownstreamRequest, QueuesDownstreamResponse, QueuesDownstreamRequestType
from kubemq.common import *
from kubemq.common.helpers import decode_grpc_error, is_channel_error

class DownstreamReceiver:
    """
    Class representing a downstream receiver for sending requests to a server.

    Attributes:
        clientStub (Transport): The transport client stub.
        connection (Connection): The connection to the server.
        transport (Transport): The transport object for channel management.
        shutdown_event (threading.Event): The event used to indicate shutdown.
        logger (logging.Logger): The logger for logging messages.
        lock (threading.Lock): The lock used for thread safety.
        response_tracking (dict): A dictionary for tracking response containers and result events.
        queue (queue.Queue): The queue used for storing requests.
        allow_new_requests (bool): Flag indicating whether new requests are allowed.

    Methods:
        send(request: QueuesDownstreamRequest) -> [QueuesDownstreamResponse, None]:
            Sends a request to the server and waits for a response.
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
        self.transport = transport
        self.clientStub = transport.kubemq_client()
        self.connection = connection
        self.shutdown_event = threading.Event()
        self.logger = logger
        self.lock = threading.Lock()
        self.response_tracking = {}
        self.queue = queue.Queue()
        self.allow_new_requests = True
        threading.Thread(target=self._send_queue_stream, args=(), daemon=True).start()

    def send(self, request: QueuesDownstreamRequest) -> Union[QueuesDownstreamResponse, None]:
        try:
            if not self.transport.is_connected():
                raise ConnectionError("Client is not connected to the server and cannot accept new requests")
            if not self.allow_new_requests:
                raise ConnectionError("Receiver is not ready to accept new requests")
                
            response_result = threading.Event()
            response_container = {}
            with self.lock:
                self.response_tracking[request.RequestID] = (response_container, response_result)
            self.queue.put(request)
            response_result.wait(request.WaitTimeout/1000)
            response: QueuesDownstreamResponse = response_container.get('response')
            with self.lock:
                if self.response_tracking.get(request.RequestID):
                    del self.response_tracking[request.RequestID]
            if response  is None:
                return QueuesDownstreamResponse(
                    RefRequestId=request.RequestID,
                    IsError=True,
                    Error="Error: Timeout waiting for response"
                )
            return response
        except Exception as e:
            return QueuesDownstreamResponse(
                RefRequestId=request.RequestID,
                IsError=True,
                Error=str(e)
            )

    def send_without_response(self, request: QueuesDownstreamRequest):
        if not self.transport.is_connected():
            self.logger.error("Client is not connected to the server and cannot accept new requests")
            raise ConnectionError("Client is not connected to the server and cannot accept new requests")
        if not self.allow_new_requests:
            self.logger.error("Receiver is not ready to accept new requests")
            raise ConnectionError("Receiver is not ready to accept new requests")
        self.queue.put(request)

    def _handle_disconnection(self):
        with self.lock:
            self.allow_new_requests = False
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
                    req= self.queue.get()
                    yield req
                except queue.Empty:
                    continue
                

        while not self.shutdown_event.is_set():
            try:
                self.clientStub = self.transport.kubemq_client()
                responses = self.clientStub.QueuesDownstream(send_requests())
                for response in responses:
                    if self.shutdown_event.is_set():
                        break
                    response_request_id = response.RefRequestId
                    with self.lock:
                        self.allow_new_requests = True
                        if response_request_id in self.response_tracking:
                            response_container, response_result = self.response_tracking[
                                response_request_id]
                            response_container['response'] = response
                            response_result.set()

            except grpc.RpcError as e:
                error_details = decode_grpc_error(e)
                self.logger.error(f"gRPC error type: {type(e).__name__}, error: {error_details}")
                self.logger.error(f"gRPC error details: code={e.code() if hasattr(e, 'code') else 'N/A'}, details={e.details() if hasattr(e, 'details') else 'N/A'}")
                self._handle_disconnection()
                
                # Attempt to recreate channel
                try:
                    self.clientStub = self.transport.recreate_channel()
                    self.logger.info("Successfully recreated gRPC channel")
                    with self.lock:
                        self.allow_new_requests = True
                except ConnectionError as conn_ex:
                    if self.connection.disable_auto_reconnect:
                        self.logger.warning("Auto-reconnect is disabled, not attempting to reconnect")
                        with self.lock:
                            self.allow_new_requests = False
                        return  # Exit the thread
                    else:
                        self.logger.error(f"Connection error: {str(conn_ex)}")
                        time.sleep(self.connection.get_reconnect_delay())
                        continue
                except Exception as channel_ex:
                    self.logger.error(f"Failed to recreate channel: {str(channel_ex)}, type: {type(channel_ex).__name__}")
                    time.sleep(self.connection.get_reconnect_delay())
                    continue
                time.sleep(self.connection.get_reconnect_delay())
                continue
                
            except Exception as e:
                self.logger.error(f"Generic exception type: {type(e).__name__}, error: {str(e)}")
                if is_channel_error(e):
                    self._handle_disconnection()
                    try:
                        self.clientStub = self.transport.recreate_channel()
                        self.logger.info("Successfully recreated gRPC channel from generic handler")
                    except ConnectionError as conn_ex:
                        if self.connection.disable_auto_reconnect:
                            self.logger.warning("Auto-reconnect is disabled, not attempting to reconnect")
                            with self.lock:
                                self.allow_new_requests = False
                            return  # Exit the thread
                        else:
                            self.logger.error(f"Connection error: {str(conn_ex)}")
                    except Exception as channel_ex:
                        self.logger.error(f"Failed to recreate channel from generic handler: {str(channel_ex)}")

                    time.sleep(self.connection.get_reconnect_delay())
                else:
                    self.logger.error("Non-channel error detected, clearing affected requests only")
                    self._handle_disconnection()
                    time.sleep(self.connection.get_reconnect_delay())

                continue

    def close(self):
        with self.lock:
            self.allow_new_requests = False
        self.shutdown_event.set()
        self.queue.put(None)
        self.logger.debug("Downstream receiver shutdown")