import logging
import threading
import queue
import time
import grpc
from kubemq.transport import Transport, Connection
from kubemq.grpc import QueuesDownstreamRequest, QueuesDownstreamResponse
from kubemq.common import *

class DownstreamReceiver:
    def __init__(self, transport: Transport, shutdown_event: threading.Event, logger: logging.Logger,
                 connection: Connection):
        self.clientStub = transport.kubemq_client()
        self.connection = connection
        self.shutdown_event = shutdown_event
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
