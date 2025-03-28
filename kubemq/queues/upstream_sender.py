import logging
import threading
import queue
import time
import grpc
from kubemq.transport import Transport, Connection
from kubemq.grpc import QueuesUpstreamRequest, QueuesUpstreamResponse, SendQueueMessageResult, QueueMessage
from kubemq.common import *
from kubemq.queues import *
from kubemq.common.helpers import decode_grpc_error, is_channel_error

class UpstreamSender:
    """

    UpstreamSender

    Class representing an upstream sender for sending messages to a server.

    Attributes:
        transport (Transport): The transport object for channel management.
        clientStub (Transport): The transport object used for communication with the server.
        connection (Connection): The connection object representing the client's connection to the server.
        shutdown_event (threading.Event): The event object used to signal the shutdown of the sender.
        logger (logging.Logger): The logger object used for logging.
        lock (threading.Lock): The lock used to synchronize access to shared resources.
        response_tracking (dict): A dictionary mapping request IDs to response containers, response result events, and message IDs.
        sending_queue (queue.Queue): The queue used for storing messages to be sent.
        allow_new_messages (bool): A flag indicating if new messages are allowed to be sent.

    Methods:
        send(message: QueueMessage) -> [QueueSendResult, None]:
            Sends a message to the server.

        _handle_disconnection():
            Handles the disconnection from the server.

        _send_queue_stream():
            Sends messages from the sending queue to the server.

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
        self.sending_queue = queue.Queue()
        self.allow_new_messages = True
        threading.Thread(target=self._send_queue_stream, args=(), daemon=True).start()

    def send(self, message: QueueMessage) -> [QueueSendResult, None]:
        try:
            if not self.transport.is_connected():
                raise ConnectionError("Client is not connected to the server and cannot send messages.")
            if not self.allow_new_messages:
                raise ConnectionError("Sender is not ready to accept new messages.")

            response_result = threading.Event()
            response_container = {}
            message_id = message.id
            queue_upstream_request = message.encode(self.connection.client_id)
            with self.lock:
                self.response_tracking[queue_upstream_request.RequestID] = (
                    response_container, response_result, message_id)
            self.sending_queue.put(queue_upstream_request)
            response_result.wait(2)
            response: QueuesUpstreamResponse = response_container.get('response')
            with self.lock:
                if self.response_tracking.get(queue_upstream_request.RequestID):
                    del self.response_tracking[queue_upstream_request.RequestID]
                if response is None:
                    return QueueSendResult(
                        id=message_id,
                        is_error=True,
                        error="Error: Timeout waiting for response"
                    )
            send_result = response.Results[0]
            return QueueSendResult().decode(send_result)
        except Exception as e:
            self.logger.error(f"Error sending message: {str(e)}")
            return QueueSendResult(
                id=message.id,
                is_error=True,
                error=str(e)
            )

    def _handle_disconnection(self):
        with self.lock:
            self.allow_new_messages = False
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
                finally:
                    sleep(0.1)

        while not self.shutdown_event.is_set():
            try:
                self.clientStub = self.transport.kubemq_client()
                responses = self.clientStub.QueuesUpstream(send_requests())
                for response in responses:
                    if self.shutdown_event.is_set():
                        break
                    response_request_id = response.RefRequestID
                    with self.lock:
                        self.allow_new_messages = True
                        if response_request_id in self.response_tracking:
                            response_container, response_result, message_od = self.response_tracking[
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
                        self.allow_new_messages = True
                except ConnectionError as conn_ex:
                    if self.connection.disable_auto_reconnect:
                        self.logger.warning("Auto-reconnect is disabled, not attempting to reconnect")
                        # Set permanent error state
                        with self.lock:
                            self.allow_new_messages = False
                        return  # Exit the thread
                    else:
                        self.logger.error(f"Connection error: {str(conn_ex)}")
                        time.sleep(self.connection.get_reconnect_delay())
                        continue
                except Exception as channel_ex:
                    self.logger.error(f"Failed to recreate channel: {str(channel_ex)}, type: {type(channel_ex).__name__}")
                    # Use fixed reconnect interval, default to 1 second if not specified
                    time.sleep(self.connection.get_reconnect_delay())
                    continue
                
                time.sleep(self.connection.get_reconnect_delay())
                continue
            except Exception as e:
                self.logger.error(f"Generic exception type: {type(e).__name__}, error: {str(e)}")
                if is_channel_error(e):
                    self.logger.info("Detected channel error in generic exception handler")
                    self._handle_disconnection()
                    
                    try:
                        self.clientStub = self.transport.recreate_channel()
                        self.logger.info("Successfully recreated gRPC channel from generic handler")
                    except ConnectionError as conn_ex:
                        if self.connection.disable_auto_reconnect:
                            self.logger.warning("Auto-reconnect is disabled, not attempting to reconnect")
                            # Set permanent error state
                            with self.lock:
                                self.allow_new_messages = False
                            return  # Exit the thread
                        else:
                            self.logger.error(f"Connection error: {str(conn_ex)}")
                    except Exception as channel_ex:
                        self.logger.error(f"Failed to recreate channel from generic handler: {str(channel_ex)}")
                    
                    time.sleep(self.connection.get_reconnect_delay())
                else:
                    # Handle non-channel errors differently
                    self.logger.error("Non-channel error detected, clearing affected messages only")
                    self._handle_disconnection()
                    time.sleep(self.connection.get_reconnect_delay())
                continue

    def close(self):
        self.allow_new_messages = False
        self.shutdown_event.set()
        self.sending_queue.put(None)