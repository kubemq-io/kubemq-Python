import logging
import threading
import time
import grpc
import socket
from typing import List
from kubemq.transport import *
from kubemq.pubsub import *
from kubemq.common.exceptions import *
from kubemq.common.helpers import *
from kubemq.common.requests import *
from kubemq.common.subscribe_type import *
from kubemq.common.cancellation_token import *


class Client:
    """

    Class Client

    The Client class provides functionality for connecting to the KubeMQ server and interacting with it.

    Attributes:
    - `address`: A string representing the address of the KubeMQ server.
    - `client_id`: A string representing the client id.
    - `auth_token`: A string representing the authentication token.
    - `tls`: A boolean indicating whether to use TLS encryption.
    - `tls_cert_file`: A string representing the path to the TLS certificate file.
    - `tls_key_file`: A string representing the path to the TLS key file.
    - `tls_ca_file`: A string representing the path to the TLS CA file.
    - `max_send_size`: An integer representing the maximum size of a send message.
    - `max_receive_size`: An integer representing the maximum size of a receive message.
    - `disable_auto_reconnect`: A boolean indicating whether to disable auto reconnection.
    - `reconnect_interval_seconds`: An integer representing the interval in seconds between reconnection attempts.
    - `keep_alive`: A boolean indicating whether to enable keep alive.
    - `ping_interval_in_seconds`: An integer representing the interval in seconds between ping messages.
    - `ping_timeout_in_seconds`: An integer representing the timeout in seconds for the ping response.
    - `log_level`: An integer representing the log level.

    Methods:
    - `__init__`: Initializes the client with the given parameters.
    - `__enter__`: Enters the context manager.
    - `__exit__`: Exits the context manager.
    - `connect`: Connects to the KubeMQ server.
    - `close`: Closes the connection with the KubeMQ server.
    - `ping`: Pings the KubeMQ server.
    - `send_events_message`: Sends an event message to the KubeMQ server.
    - `send_events_store_message`: Sends an event store message to the KubeMQ server.
    - `create_events_channel`: Creates an events channel on the KubeMQ server.
    - `create_events_store_channel`: Creates an events store channel on the KubeMQ server.
    - `delete_events_channel`: Deletes an events channel from the KubeMQ server.
    - `delete_events_store_channel`: Deletes an events store channel from the KubeMQ server.
    - `list_events_channels`: Lists all events channels on the KubeMQ server.
    - `list_events_store_channels`: Lists all events store channels on the KubeMQ server.
    - `subscribe_to_events`: Subscribes to events on the KubeMQ server.
    - `subscribe_to_events_store`: Subscribes to events store on the KubeMQ server.

    """

    def __init__(
        self,
        address: str = "",
        client_id: str = socket.gethostname(),
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
        log_level: int = None,
    ) -> None:
        self.connection: Connection = Connection(
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
                ping_timeout_in_seconds=ping_timeout_in_seconds,
            ),
            log_level=log_level,
        )
        self.logger = logging.getLogger("KubeMQ")
        if log_level is not None:
            self.logger.setLevel(log_level)
        else:
            self.logger.setLevel(logging.CRITICAL + 1)
        try:
            self.transport: Transport = Transport(self.connection).initialize()
            self.shutdown_event: threading.Event = threading.Event()
            self.event_sender = None
        except ValueError as e:
            ex = ValidationError(e)
            self.logger.error(str(ex))
            raise ex
        except Exception as e:
            ex = GRPCError(e)
            self.logger.error(str(ex))
            raise ex

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def connect(self):
        try:
            self.logger.debug(f"Client connecting to {self.connection.address}")
            self.transport: Transport = Transport(self.connection).initialize()
            self.logger.debug(f"Client connected to {self.connection.address}")
        except Exception as e:
            ex = GRPCError(e)
            self.logger.error(str(ex))
            raise ex

    def close(self):
        try:
            self.shutdown_event.set()
            self.logger.debug(f"Client disconnecting from {self.connection.address}")
            self.transport.close()
        except Exception as e:
            ex = GRPCError(e)
            self.logger.error(str(ex))
            raise ex

    def ping(self) -> ServerInfo:
        """
        Ping method sends a ping request to the server and returns the server information.

        :param self: The current object.
        :return: The server information as a ServerInfo object.
        :raises GRPCError: If an exception occurs while pinging the server.
        """
        try:
            self.logger.debug(f"Client pinging {self.connection.address}")
            return self.transport.ping()
        except Exception as e:
            ex = GRPCError(e)
            self.logger.error(str(ex))
            raise ex

    def send_events_message(self, message: EventMessage):
        """
        Sends the given event message.

        Args:
            message (EventMessage): The event message to send.

        Returns:
            None

        Raises:
            None
        """

        if self.event_sender is None:
            self.event_sender = EventSender(
                self.transport, self.shutdown_event, self.logger, self.connection
            )
        self.event_sender.send(message.encode(self.connection.client_id))

    def send_events_store_message(self, message: EventStoreMessage) -> EventSendResult:
        """
        Sends an event store message using the provided parameters.

        Parameters:
        - message (EventStoreMessage): The event store message to send.

        Returns:
        - EventSendResult: The result of sending the message.

        Raises:
        - None

        Example usage:
            message = EventStoreMessage()
            result = send_events_store_message(message)
            print(result)
        """
        if self.event_sender is None:
            self.event_sender = EventSender(
                self.transport, self.shutdown_event, self.logger, self.connection
            )
        result = self.event_sender.send(message.encode(self.connection.client_id))
        if result is not None:
            return EventSendResult().decode(result)

    def create_events_channel(self, channel: str) -> [bool, None]:
        """
        Create a new events channel.

        Args:
            channel: The name of the channel to create.

        Returns:
            bool or None: True if the channel is successfully created, None otherwise.
        """
        return create_channel_request(
            self.transport, self.connection.client_id, channel, "events"
        )

    def create_events_store_channel(self, channel: str):
        """
        Creates a channel for storing events.

        Args:
            channel (str): The name of the channel to create.

        Returns:
            None

        """
        return create_channel_request(
            self.transport, self.connection.client_id, channel, "events_store"
        )

    def delete_events_channel(self, channel: str):
        """
        Deletes the specified events channel.

        Parameters:
        - channel (str): The name of the channel to be deleted. It should be a string.

        Returns:
        - None

        """
        return delete_channel_request(
            self.transport, self.connection.client_id, channel, "events"
        )

    def delete_events_store_channel(self, channel: str):
        """
        Deletes the specified channel from the events store.

        :param channel: The name of the channel to be deleted.
        """
        return delete_channel_request(
            self.transport, self.connection.client_id, channel, "events_store"
        )

    def list_events_channels(self, channel_search: str = "") -> List[PubSubChannel]:
        """Lists the events channels.

        Args:
            channel_search (str, optional): The search string used to filter the channel names. Defaults to "".

        Returns:
            List[PubSubChannel]: A list of PubSubChannel objects representing the events channels.

        """
        return list_pubsub_channels(
            self.transport, self.connection.client_id, "events", channel_search
        )

    def list_events_store_channels(
        self, channel_search: str = ""
    ) -> List[PubSubChannel]:
        """
        Returns a list of PubSubChannel objects representing the channels in the events_store.

        Args:
            channel_search (str): Optional. If provided, only the channels containing the
                specified search string will be included in the resulting list. Defaults to "".

        Returns:
            List[PubSubChannel]: A list of PubSubChannel objects representing the channels
                in the events_store.

        """
        return list_pubsub_channels(
            self.transport, self.connection.client_id, "events_store", channel_search
        )

    def subscribe_to_events(
        self, subscription: EventsSubscription, cancel: CancellationToken = None
    ):
        """
        Subscribes to events using the given subscription.

        :param subscription: The subscription object containing the event details.
        :type subscription: EventsSubscription
        :param cancel: Optional cancellation token to cancel the subscription.
        :type cancel: CancellationToken, optional

        :return: None
        :rtype: None
        """
        self._subscribe(subscription, cancel)

    def subscribe_to_events_store(
        self, subscription: EventsStoreSubscription, cancel: CancellationToken = None
    ):
        """
        Subscribes to events in the events store.

        Args:
            subscription (EventsStoreSubscription): The subscription object that defines the events to subscribe to.
            cancel (CancellationToken, optional): A cancellation token that can be used to cancel the subscription. Defaults to None.

        Returns:
            None: This method does not return anything.

        """
        self._subscribe(subscription, cancel)

    def _subscribe(
        self,
        subscription: [EventsSubscription, EventsStoreSubscription],
        cancel: [CancellationToken, None],
    ):
        if cancel is None:
            cancel = CancellationToken()
        cancel_token_event = cancel.event
        args = ()
        if isinstance(subscription, EventsStoreSubscription):
            args = (
                lambda: self.transport.kubemq_client().SubscribeToEvents(
                    subscription.encode(self.connection.client_id)
                ),
                lambda message: subscription.raise_on_receive_message(
                    EventStoreMessageReceived().decode(message)
                ),
                lambda error: subscription.raise_on_error(error),
                cancel_token_event,
            )
        if isinstance(subscription, EventsSubscription):
            args = (
                lambda: self.transport.kubemq_client().SubscribeToEvents(
                    subscription.encode(self.connection.client_id)
                ),
                lambda message: subscription.raise_on_receive_message(
                    EventMessageReceived().decode(message)
                ),
                lambda error: subscription.raise_on_error(error),
                cancel_token_event,
            )
        threading.Thread(target=self._subscribe_task, args=args, daemon=True).start()

    def _subscribe_task(
        self,
        stream_callable,
        decode_callable,
        error_callable,
        cancel_token: threading.Event,
    ):
        while not cancel_token.is_set() and not self.shutdown_event.is_set():
            try:
                response = stream_callable()
                for message in response:
                    if cancel_token.is_set():
                        break
                    decode_callable(message)
            except grpc.RpcError as e:
                error_callable(decode_grpc_error(e))
                time.sleep(self.connection.reconnect_interval_seconds)
                continue
            except Exception as e:
                error_callable(decode_grpc_error(e))
                time.sleep(self.connection.reconnect_interval_seconds)
                continue
