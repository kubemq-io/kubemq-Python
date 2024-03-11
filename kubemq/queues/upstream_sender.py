import logging
import threading
import queue
import time
import grpc
from kubemq.transport import Transport, Connection
from kubemq.grpc import QueuesUpstreamRequest, QueuesUpstreamResponse, SendQueueMessageResult
from kubemq.common import *
from kubemq.queues import *
class UpstreamSender:
    def __init__(self, transport: Transport, shutdown_event: threading.Event, logger: logging.Logger,
                 connection: Connection):
        self.clientStub = transport.kubemq_client()
        self.connection = connection
        self.shutdown_event = shutdown_event
        self.logger = logger
        self.lock = threading.Lock()
        self.response_tracking = {}
        self.sending_queue = queue.Queue()
        self.allow_new_messages = True
        threading.Thread(target=self._send_queue_stream, args=(), daemon=True).start()

    def send(self, message: QueueMessage) -> [QueueSendResult, None]:
        if not self.allow_new_messages:
            raise ConnectionError("Client is not connected to the server and cannot send messages.")
        response_result = threading.Event()
        response_container = {}
        message_id = message.id
        queue_upstream_request = message.encode(self.connection.client_id)
        with self.lock:
            self.response_tracking[queue_upstream_request.RequestID] = (
                response_container, response_result, message_id)
        self.sending_queue.put(queue_upstream_request)
        response_result.wait()
        response: QueuesUpstreamResponse = response_container.get('response')
        with self.lock:
            if response is not None:
                del self.response_tracking[queue_upstream_request.RequestID]
        if response is None or len(response.Results) == 0:
            return None
        send_result = response.Results[0]
        return QueueSendResult().decode(send_result)

    def _handle_disconnection(self):
        with self.lock:
            self.allow_new_messages = False
            while not self.sending_queue.empty():
                try:
                    self.sending_queue.get_nowait()  # Clear the queue
                except queue.Empty:
                    continue

            # Set error on all response containers
            for request_id, (response_container, response_result, message_id) in self.response_tracking.items():
                response_container['response'] = QueuesUpstreamResponse(
                    RefRequestID=request_id,
                    Results=[SendQueueMessageResult(
                        MessageID=message_id,
                        IsError=True,
                        Error="Error: Disconnected from server"
                    )]
                )
                response_result.set()  # Signal that the response has been processed
            self.response_tracking.clear()

    def _send_queue_stream(self):
        def send_requests():
            while not self.shutdown_event.is_set():
                try:
                    msg = self.sending_queue.get(timeout=1)  # timeout to check for shutdown event periodically
                    yield msg
                except queue.Empty:
                    continue

        while not self.shutdown_event.is_set():
            try:
                with self.lock:
                    self.allow_new_messages = True
                responses = self.clientStub.QueuesUpstream(send_requests())
                for response in responses:
                    if self.shutdown_event.is_set():
                        break
                    response_request_id = response.RefRequestID
                    with self.lock:
                        if response_request_id in self.response_tracking:
                            response_container, response_result, message_od = self.response_tracking[
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
