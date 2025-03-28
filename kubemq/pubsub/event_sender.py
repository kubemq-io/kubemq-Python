import logging
import time
import threading
import queue
import grpc
from kubemq.transport import Transport, Connection
from kubemq.grpc import Event, Result
from kubemq.common import *


class EventSender:
    """
    EventSender is a class that is responsible for sending events to a server using a transport, tracking the response of each event, and handling disconnections.

    Attributes:
    - clientStub: A client stub object for communicating with the server.
    - connection: A Connection object containing connection information.
    - shutdown_event: A threading.Event object to signal whether the sender should shutdown.
    - logger: A logging.Logger object for logging messages.
    - lock: A threading.Lock object for thread safety.
    - response_tracking: A dictionary to track the response of each event.
    - sending_queue: A queue.Queue object for storing events to be sent.
    - allow_new_messages: A flag indicating whether new messages are allowed to be sent.

    Methods:
    - __init__(transport: Transport, shutdown_event: threading.Event, logger: logging.Logger,
             connection: Connection): Initializes the EventSender object with the given transport, shutdown event, logger, and connection. Starts a new thread to send events.
    - send(event: Event) -> Optional[Result]: Sends an event to the server. If the event is not set to be stored, it queues the event. If it is set to be stored, it waits for the response
    * and returns it. Raises a ConnectionError if the client is not connected.
    - handle_disconnection(): Handles the disconnection from the server. Clears the sending queue and sets an error on all response containers.
    - send_events_stream(): Continuously sends events from the sending queue to the server. Handles disconnections and tracks responses.
    """

    def __init__(
        self,
        transport: Transport,
        shutdown_event: threading.Event,
        logger: logging.Logger,
        connection: Connection,
    ):
        self.clientStub = transport.kubemq_client()
        self.connection = connection
        self.shutdown_event = shutdown_event
        self.logger = logger
        self.lock = threading.Lock()
        self.response_tracking = {}
        self.sending_queue = queue.Queue()
        self.allow_new_messages = True
        threading.Thread(target=self.send_events_stream, args=(), daemon=True).start()

    def send(self, event: Event) -> [Result, None]:
        if not self.allow_new_messages:
            raise ConnectionError(
                "Client is not connected to the server and cannot send messages."
            )

        if not event.Store:
            self.sending_queue.put(event)
            return None
        response_event = threading.Event()
        response_container = {}

        with self.lock:
            self.response_tracking[event.EventID] = (response_container, response_event)
        self.sending_queue.put(event)
        response_event.wait()
        response = response_container.get("response")
        with self.lock:
            del self.response_tracking[event.EventID]
        return response

    def handle_disconnection(self):
        with self.lock:
            self.allow_new_messages = False
            while not self.sending_queue.empty():
                try:
                    self.sending_queue.get_nowait()  # Clear the queue
                except queue.Empty:
                    continue

            # Set error on all response containers
            for event_id, (
                response_container,
                response_event,
            ) in self.response_tracking.items():
                response_container["response"] = Result(
                    EventID=event_id,
                    Sent=False,
                    Error="Error: Disconnected from server",
                )
                response_event.set()  # Signal that the response has been processed
            self.response_tracking.clear()

    def send_events_stream(self):
        def send_requests():
            while not self.shutdown_event.is_set():
                try:
                    msg = self.sending_queue.get(
                        timeout=1
                    )  # timeout to check for shutdown event periodically
                    yield msg
                except queue.Empty:
                    continue

        while not self.shutdown_event.is_set():
            try:
                with self.lock:
                    self.allow_new_messages = True
                responses = self.clientStub.SendEventsStream(send_requests())
                for response in responses:
                    if self.shutdown_event.is_set():
                        break
                    response_event_id = response.EventID
                    with self.lock:
                        if response_event_id in self.response_tracking:
                            response_container, response_event = self.response_tracking[
                                response_event_id
                            ]
                            response_container["response"] = response
                            response_event.set()
            except grpc.RpcError as e:
                self.logger.debug(decode_grpc_error(e))
                self.handle_disconnection()
                time.sleep(self.connection.reconnect_interval_seconds)
                continue
            except Exception as e:
                self.logger.debug(f"Error: {str(e)}")
                self.handle_disconnection()
                time.sleep(self.connection.reconnect_interval_seconds)
                continue
