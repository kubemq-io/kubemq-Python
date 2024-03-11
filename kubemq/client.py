import logging
import threading
import asyncio
import queue
import time
import uuid
import grpc
from kubemq.entities import *
from kubemq.transport import *
from kubemq.grpc import (Event,
                         Result,
                         QueuesUpstreamResponse,
                         SendQueueMessageResult,
                         QueuesDownstreamRequest,
                         QueuesDownstreamRequestType,
                         QueuesDownstreamResponse)


class Client:
    def __init__(self, address: str = "",
                 client_id: str = "",
                 auth_token: str = "",
                 tls: bool = False,
                 tls_cert_file: str = "",
                 tls_key_file: str = "",
                 tls_ca_file: str = "",
                 max_send_size: int = 0,
                 max_receive_size: int = 0,
                 disable_auto_reconnect: bool = False,
                 reconnect_interval_seconds: int = 0,
                 keep_alive: bool = False,
                 ping_interval_in_seconds: int = 0,
                 ping_timeout_in_seconds: int = 0,
                 log_level: int = None) -> None:
        self._connection: Connection = Connection(
            address=address,
            client_id=client_id,
            auth_token=auth_token,
            tls=TlsConfig(
                ca_file=tls_ca_file,
                cert_file=tls_cert_file,
                key_file=tls_key_file,
                enabled=tls,
            ),
            max_send_size=max_send_size,
            max_receive_size=max_receive_size,
            disable_auto_reconnect=disable_auto_reconnect,
            reconnect_interval_seconds=reconnect_interval_seconds,
            keep_alive=KeepAliveConfig(
                enabled=keep_alive,
                ping_interval_in_seconds=ping_interval_in_seconds,
                ping_timeout_in_seconds=ping_timeout_in_seconds
            ),
            log_level=log_level)
        self._logger = logging.getLogger("KubeMQ")
        if log_level is not None:
            self._logger.setLevel(log_level)
        else:
            self._logger.setLevel(logging.CRITICAL + 1)
        try:
            self._connection.validate()
            self._transport: Transport = Transport(self._connection).initialize()
            self._logger.info(f"Client connected to {self._connection.address}")
            self._shutdown_event: threading.Event = threading.Event()
            self._event_sender = None
            self._queue_upstream_sender = None
            self._queue_downstream_receiver = None
        except ValueError as e:
            ex = ValidationError(e)
            self._logger.error(str(ex))
            raise ex
        except Exception as e:
            ex = GRPCError(e)
            self._logger.error(str(ex))
            raise ex

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def connect(self):
        try:
            self._logger.debug(f"Client connecting to {self._connection.address}")
            self._transport: Transport = Transport(self._connection).initialize()
            self._logger.debug(f"Client connected to {self._connection.address}")
        except Exception as e:
            ex = GRPCError(e)
            self._logger.error(str(ex))
            raise ex

    def close(self):
        try:
            self._logger.debug(f"Client disconnecting from {self._connection.address}")
            self._transport.close()
            self._logger.debug(f"Client disconnected from {self._connection.address}")
            self._shutdown_event.set()
        except Exception as e:
            ex = GRPCError(e)
            self._logger.error(str(ex))
            raise ex

    def ping(self) -> ServerInfo:
        try:
            self._logger.debug(f"Client pinging {self._connection.address}")
            return self._transport.ping()
        except Exception as e:
            ex = GRPCError(e)
            self._logger.error(str(ex))
            raise ex

    def send(self, message: [EventMessage,
                             EventStoreMessage,
                             CommandMessage,
                             QueryMessage,
                             CommandResponseMessage,
                             QueryResponseMessage,
                             QueueMessage]) -> [CommandResponseMessage,
                                                QueryResponseMessage,
                                                EventSendResult,
                                                QueueSendResult,
                                                None]:
        message.validate()
        if isinstance(message, EventMessage) or isinstance(message, EventStoreMessage):
            if self._event_sender is None:
                self._event_sender = _EventSender(self._transport, self._shutdown_event, self._logger, self._connection)
            result = self._event_sender._send(message.encode(self._connection.client_id))
            if result is not None:
                return EventSendResult().decode(result)
            return None
        if isinstance(message, CommandMessage):
            response = self._transport.kubemq_client().SendRequest(
                message.encode(self._connection.client_id))
            return CommandResponseMessage().decode(response)
        if isinstance(message, CommandResponseMessage) or isinstance(message, QueryResponseMessage):
            self._transport.kubemq_client().SendResponse(
                message.encode(self._connection.client_id))
        if isinstance(message, QueryMessage):
            response = self._transport.kubemq_client().SendRequest(
                message.encode(self._connection.client_id))
            return QueryResponseMessage().decode(response)
        if isinstance(message, QueueMessage):
            if self._queue_upstream_sender is None:
                self._queue_upstream_sender = _QueueUpstreamSender(self._transport, self._shutdown_event, self._logger,
                                                                   self._connection)
            return self._queue_upstream_sender._send(message)
        return None

    def subscribe(self,
                  subscription: [EventsSubscription,
                                 EventsStoreSubscription,
                                 CommandsSubscription,
                                 QueriesSubscription],
                  cancel: [CancellationToken, None]):
        if cancel is None:
            cancel = CancellationToken()
        cancel_token_event = cancel.event
        subscription.validate()
        args = ()
        if isinstance(subscription, EventsStoreSubscription):
            args = (lambda: self._transport.kubemq_client().SubscribeToEvents(
                subscription.encode(self._connection.client_id)),
                    lambda message: subscription.raise_on_receive_message(
                        EventStoreMessageReceived().decode(message)),
                    lambda error: subscription.raise_on_error(error),
                    cancel_token_event)
        if isinstance(subscription, EventsSubscription):
            args = (lambda: self._transport.kubemq_client().SubscribeToEvents(
                subscription.encode(self._connection.client_id)),
                    lambda message: subscription.raise_on_receive_message(
                        EventMessageReceived().decode(message)),
                    lambda error: subscription.raise_on_error(error),
                    cancel_token_event)
        if isinstance(subscription, CommandsSubscription):
            args = (lambda: self._transport.kubemq_client().SubscribeToRequests(
                subscription.decode(self._connection.client_id)),
                    lambda message: subscription.raise_on_receive_message(
                        CommandMessageReceived().decode(message)),
                    lambda error: subscription.raise_on_error(error),
                    cancel_token_event)
        if isinstance(subscription, QueriesSubscription):
            args = (lambda: self._transport.kubemq_client().SubscribeToRequests(
                subscription.encode(self._connection.client_id)),
                    lambda message: subscription.raise_on_receive_message(
                        QueryMessageReceived().decode(message)),
                    lambda error: subscription.raise_on_error(error),
                    cancel_token_event)

        threading.Thread(
            target=self._subscribe_task,
            args=args,
            daemon=True).start()

    def _subscribe_task(self, stream_callable, decode_callable, error_callable, cancel_token: threading.Event):
        while not cancel_token.is_set() and not self._shutdown_event.is_set():
            try:
                response = stream_callable()
                for message in response:
                    if cancel_token.is_set():
                        break
                    decode_callable(message)
            except grpc.RpcError as e:
                error_callable(_decode_grpc_error(e))
                time.sleep(self._connection.reconnect_interval_seconds)
                continue
            except Exception as e:
                error_callable(_decode_grpc_error(e))
                time.sleep(self._connection.reconnect_interval_seconds)
                continue

    def poll(self, channel: str = None,
             max_messages: int = 1,
             wait_timeout_in_seconds: int = 60,
             auto_ack: bool = False,
             ) -> QueuesPollResponse:
        if channel is None:
            raise ValueError("channel cannot be None.")
        if max_messages < 1:
            raise ValueError("poll_max_messages must be greater than 0.")
        if wait_timeout_in_seconds < 1:
            raise ValueError("poll_wait_timeout_in_seconds must be greater than 0.")

        if self._queue_downstream_receiver is None:
            self._queue_downstream_receiver = (
                _QueueDownstreamReceiver(self._transport,
                                         self._shutdown_event,
                                         self._logger,
                                         self._connection))

        request = QueuesDownstreamRequest()
        request.RequestID = str(uuid.uuid4())
        request.ClientID = self._connection.client_id
        request.Channel = channel
        request.MaxItems = max_messages
        request.WaitTimeout = wait_timeout_in_seconds * 1000
        request.AutoAck = auto_ack
        request.RequestTypeData = QueuesDownstreamRequestType.Get
        kubemq_response: QueuesDownstreamResponse = self._queue_downstream_receiver._send(request)
        response = QueuesPollResponse().decode(
            response=kubemq_response,
            receiver_client_id=self._connection.client_id,
            response_handler=self._queue_downstream_receiver._send_without_response)

        return response


class _EventSender:
    def __init__(self, transport: Transport, shutdown_event: threading.Event, logger: logging.Logger,
                 connection: Connection):
        self._clientStub = transport.kubemq_client()
        self._connection = connection
        self._shutdown_event = shutdown_event
        self._logger = logger
        self._lock = threading.Lock()
        self._response_tracking = {}
        self._sending_queue = queue.Queue()
        self._allow_new_messages = True
        threading.Thread(target=self._send_events_stream, args=(), daemon=True).start()

    def _send(self, event: Event) -> [Result, None]:
        if not self._allow_new_messages:
            raise ConnectionError("Client is not connected to the server and cannot send messages.")

        if not event.Store:
            self._sending_queue.put(event)
            return None
        response_event = threading.Event()
        response_container = {}

        with self._lock:
            self._response_tracking[event.EventID] = (response_container, response_event)
        self._sending_queue.put(event)
        response_event.wait()
        response = response_container.get('response')
        with self._lock:
            del self._response_tracking[event.EventID]
        return response

    def _handle_disconnection(self):
        with self._lock:
            self._allow_new_messages = False
            while not self._sending_queue.empty():
                try:
                    self._sending_queue.get_nowait()  # Clear the queue
                except queue.Empty:
                    continue

            # Set error on all response containers
            for event_id, (response_container, response_event) in self._response_tracking.items():
                response_container['response'] = Result(
                    EventID=event_id,
                    Sent=False,
                    Error='Error: Disconnected from server'
                )
                response_event.set()  # Signal that the response has been processed
            self._response_tracking.clear()

    def _send_events_stream(self):
        def send_requests():
            while not self._shutdown_event.is_set():
                try:
                    msg = self._sending_queue.get(timeout=1)  # timeout to check for shutdown event periodically
                    yield msg
                except queue.Empty:
                    continue

        while not self._shutdown_event.is_set():
            try:
                with self._lock:
                    self._allow_new_messages = True
                responses = self._clientStub.SendEventsStream(send_requests())
                for response in responses:
                    if self._shutdown_event.is_set():
                        break
                    response_event_id = response.EventID
                    with self._lock:
                        if response_event_id in self._response_tracking:
                            response_container, response_event = self._response_tracking[response_event_id]
                            response_container['response'] = response
                            response_event.set()
            except grpc.RpcError as e:
                self._logger.debug(_decode_grpc_error(e))
                self._handle_disconnection()
                time.sleep(self._connection.reconnect_interval_seconds)
                continue
            except Exception as e:
                self._logger.debug(f"Error: {str(e)}")
                self._handle_disconnection()
                time.sleep(self._connection.reconnect_interval_seconds)
                continue




class _QueueDownstreamReceiver:
    def __init__(self, transport: Transport, shutdown_event: threading.Event, logger: logging.Logger,
                 connection: Connection):
        self._clientStub = transport.kubemq_client()
        self._connection = connection
        self._shutdown_event = shutdown_event
        self._logger = logger
        self._lock = threading.Lock()
        self._response_tracking = {}
        self._queue = queue.Queue()
        self._allow_new_requests = True
        threading.Thread(target=self._send_queue_stream, args=(), daemon=True).start()

    def _send(self, request: QueuesDownstreamRequest) -> [QueuesDownstreamResponse, None]:
        try:
            if not self._allow_new_requests:
                raise ConnectionError("Client is not connected to the server and cannot accept new requests")
            response_result = threading.Event()
            response_container = {}
            with self._lock:
                self._response_tracking[request.RequestID] = (response_container, response_result)
            self._queue.put(request)
            response_result.wait()
            response: QueuesDownstreamResponse = response_container.get('response')
            with self._lock:
                if response is not None:
                    if self._response_tracking.get(request.RequestID):
                        del self._response_tracking[request.RequestID]

            return response
        except Exception as e:
            return QueuesDownstreamResponse(
                RefRequestId=request.RequestID,
                IsError=True,
                Error=str(e)
            )

    def _send_without_response(self, request: QueuesDownstreamRequest):
        if not self._allow_new_requests:
            raise ConnectionError("Client is not connected to the server and cannot accept new requests")
        self._queue.put(request)

    def _handle_disconnection(self):
        with self._lock:
            self._allow_new_requests = False
            while not self._queue.empty():
                try:
                    self._queue.get_nowait()  # Clear the queue
                except queue.Empty:
                    continue

            # Set error on all response containers
            for request_id, (response_container, response_result) in self._response_tracking.items():
                response_container['response'] = QueuesDownstreamResponse(
                    RefRequestId=request_id,
                    IsError=True,
                    Error="Error: Disconnected from server"
                )
                response_result.set()  # Signal that the response has been processed
            self._response_tracking.clear()

    def _send_queue_stream(self):
        def send_requests():
            while not self._shutdown_event.is_set():
                try:
                    msg = self._queue.get(timeout=1)  # timeout to check for shutdown event periodically
                    yield msg
                except queue.Empty:
                    continue

        while not self._shutdown_event.is_set():
            try:
                with self._lock:
                    self._allow_new_requests = True
                responses = self._clientStub.QueuesDownstream(send_requests())
                for response in responses:
                    if self._shutdown_event.is_set():
                        break
                    response_request_id = response.RefRequestId
                    with self._lock:
                        if response_request_id in self._response_tracking:
                            response_container, response_result = self._response_tracking[
                                response_request_id]
                            response_container['response'] = response
                            response_result.set()
            except grpc.RpcError as e:
                self._logger.debug(_decode_grpc_error(e))
                self._handle_disconnection()
                time.sleep(self._connection.reconnect_interval_seconds)
                continue
            except Exception as e:
                self._logger.debug(f"Error: {str(e)}")
                self._handle_disconnection()
                time.sleep(self._connection.reconnect_interval_seconds)
                continue


def _decode_grpc_error(exc) -> str:
    message = str(exc)

    # Check if the exception has 'code' (status) and 'details' methods
    if hasattr(exc, 'code') and callable(exc.code) and hasattr(exc, 'details') and callable(exc.details):
        status = exc.code()
        details = exc.details()

        # Ensure that status and details are not None
        if status is not None and details is not None:
            message = f"KubeMQ Connection Error - Status: {status} Details: {details}"

    return message
