import logging
import threading
import asyncio
import time
import uuid
import grpc
import socket
from typing import List
from kubemq.transport import *
from kubemq.queues.queues_message import *
from kubemq.queues import *
from kubemq.common.exceptions import *
from kubemq.common.helpers import *
from kubemq.common.requests import *
from kubemq.common.cancellation_token import *
from kubemq.grpc import ReceiveQueueMessagesRequest


class Client:
    """


    Client class represents a client that connects to the KubeMQ server.

    Attributes:
        connection (Connection): The connection configuration.
        logger (logging.Logger): The logger for logging events.
        transport (Transport): The transport layer used for communication.
        shutdown_event (threading.Event): The event used for notifying the client to shutdown.
        upstream_sender (UpstreamSender): The sender used for sending upstream messages.
        downstream_receiver (DownstreamReceiver): The receiver used for receiving downstream messages.

    Methods:
        connect(): Connects the client to the KubeMQ server.
        close(): Closes the connection to the KubeMQ server.
        ping() -> ServerInfo: Pings the KubeMQ server.
        send_queues_message(message: QueueMessage) -> QueueSendResult: Sends a message to the KubeMQ server queues.
        create_queues_channel(channel: str) -> [bool, None]: Creates a queues channel.
        delete_queues_channel(channel: str) -> [bool, None]: Deletes a queues channel.
        list_queues_channels(channel_search) -> List[QueuesChannel]: Lists the queues channels.
        receive_queues_messages(channel: str = None, max_messages: int = 1, wait_timeout_in_seconds: int = 60, auto_ack: bool = False) -> QueuesPollResponse: Receives messages from a queues
    * channel.

    Example usage:
        client = Client(address='localhost:50000')
        client.connect()
        server_info = client.ping()
        print(f"Server version: {server_info.server_version}")
        client.close()

    Note: Make sure to set the connection attributes correctly before connecting to the server.

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
            self.upstream_sender = None
            self.downstream_receiver = None
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
            time.sleep(1)
            self.shutdown_event.set()
            if self.upstream_sender is not None:
                self.upstream_sender.close()
            if self.downstream_receiver is not None:
                self.downstream_receiver.close()
            self.logger.debug(f"Client disconnecting from {self.connection.address}")
            self.transport.close()
        except Exception as e:
            ex = GRPCError(e)
            self.logger.error(str(ex))
            raise ex

    def ping(self) -> ServerInfo:
        """
        Ping the server and get the server information.

        Returns:
            ServerInfo: The server information.

        Raises:
            GRPCError: If an error occurs during the ping.

        """
        try:
            self.logger.debug(f"Client pinging {self.connection.address}")
            return self.transport.ping()
        except Exception as e:
            ex = GRPCError(e)
            self.logger.error(str(ex))
            raise ex

    def send_queues_message(self, message: QueueMessage) -> QueueSendResult:
        """
        Sends a message to the queues.

        Args:
            self: The current object instance.
            message (QueueMessage): The message to be sent.

        Returns:
            QueueSendResult: The result of the message send operation.
        """
        # message.validate()
        if self.upstream_sender is None:
            self.upstream_sender = UpstreamSender(
                self.transport,  self.logger, self.connection
            )

        return self.upstream_sender.send(message)

    def create_queues_channel(self, channel: str) -> [bool, None]:
        """
        Create a queues channel for the given channel.

        Parameters:
        - channel (str): The name of the channel to create.

        Returns:
        - bool or None: Returns True if the channel is created successfully, otherwise returns None.

        """
        return create_channel_request(
            self.transport, self.connection.client_id, channel, "queues"
        )

    def delete_queues_channel(self, channel: str) -> [bool, None]:
        """
        Delete Queues Channel

        Deletes a specific channel from the queues in the current connection.

        Parameters:
        - channel (str): The name of the channel to delete.

        Returns:
        - bool or None: Returns True if the channel was successfully deleted. Returns None if there was an error during deletion.
        """
        return delete_channel_request(
            self.transport, self.connection.client_id, channel, "queues"
        )

    def list_queues_channels(self, channel_search) -> List[QueuesChannel]:
        """
        Retrieve a list of QueuesChannel objects based on the provided channel_search.

        Parameters:
        :param channel_search: The search term used to filter the list of QueuesChannels.

        Returns:
        - A list of QueuesChannel objects that match the search term.
        """
        return list_queues_channels(
            self.transport, self.connection.client_id, channel_search
        )

    def receive_queues_messages(
        self,
        channel: str = None,
        max_messages: int = 1,
        wait_timeout_in_seconds: int = 60,
        auto_ack: bool = False,
        visibility_seconds: int = 0,
    ) -> QueuesPollResponse:
        """

        Receive messages from a specific channel in the queues.

        Parameters:
        - channel (str): The name of the channel to receive messages from. Defaults to None.
        - max_messages (int): The maximum number of messages to receive. Defaults to 1.
        - wait_timeout_in_seconds (int): The maximum time in seconds to wait for new messages. Defaults to 60.
        - auto_ack (bool): Whether to automatically acknowledge the received messages. Defaults to False.

        Returns:
        - QueuesPollResponse: The response object containing the received messages.

        Raises:
        - ValueError: If the channel is None, max_messages is less than 1, or wait_timeout_in_seconds is less than 1.

        """
        if channel is None:
            raise ValueError("channel cannot be None.")
        if max_messages < 1:
            raise ValueError("poll_max_messages must be greater than 0.")
        if wait_timeout_in_seconds < 1:
            raise ValueError("poll_wait_timeout_in_seconds must be greater than 0.")
        if auto_ack and visibility_seconds > 0:
            raise ValueError(
                "auto_ack and visibility_seconds cannot be set together"
            )
        if self.downstream_receiver is None:
            self.downstream_receiver = DownstreamReceiver(
                self.transport,  self.logger, self.connection
            )

        request = QueuesDownstreamRequest()
        request.RequestID = str(uuid.uuid4())
        request.ClientID = self.connection.client_id
        request.Channel = channel
        request.MaxItems = max_messages
        request.WaitTimeout = wait_timeout_in_seconds * 1000
        request.AutoAck = auto_ack
        request.RequestTypeData = QueuesDownstreamRequestType.Get
        kubemq_response: QueuesDownstreamResponse = self.downstream_receiver.send(
            request
        )
        response = QueuesPollResponse().decode(
            response=kubemq_response,
            receiver_client_id=self.connection.client_id,
            response_handler=self.downstream_receiver.send_without_response,
            request_visibility_seconds=visibility_seconds,
        )

        return response

    def waiting(
        self, channel: str, max_messages: int, wait_timeout_in_seconds: int
    ) -> QueueMessagesWaiting:
        """
        Get waiting messages from a queue.

        Args:
            channel (str): The name of the queue channel.
            max_messages (int): The maximum number of messages to retrieve.
            wait_timeout_in_seconds (int): The maximum amount of time to wait for messages in seconds.

        Returns:
            QueueMessagesWaiting: An object containing the waiting messages.

        Raises:
            ValueError: If channel is None, max_messages is less than 1, or wait_timeout_in_seconds is less than 1.
        """
        self.logger.debug(f"Get waiting messages from queue: {channel}")
        if channel is None:
            raise ValueError("channel cannot be None.")
        if max_messages < 1:
            raise ValueError("max_messages must be greater than 0.")
        if wait_timeout_in_seconds < 1:
            raise ValueError("wait_timeout_in_seconds must be greater than 0.")

        request = ReceiveQueueMessagesRequest(
            RequestID=str(uuid.uuid4()),
            ClientID=self.connection.client_id,
            Channel=channel,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_timeout_in_seconds,
            IsPeak=True,
        )

        response = self.transport.kubemq_client().ReceiveQueueMessages(request)
        waiting_messages = QueueMessagesWaiting(
            is_error=response.IsError, error=response.Error
        )

        if not response.Messages:
            return waiting_messages

        self.logger.debug(f"Waiting messages count: {len(response.Messages)}")
        for message in response.Messages:
            waiting_messages.messages.append(
                QueueMessageWaitingPulled.decode(message, self.connection.client_id)
            )

        return waiting_messages

    def pull(
        self, channel: str, max_messages: int, wait_timeout_in_seconds: int
    ) -> QueueMessagesPulled:
        """
        Pulls messages from a queue.

        Args:
            channel (str): The name of the queue channel.
            max_messages (int): The maximum number of messages to pull.
            wait_timeout_in_seconds (int): The maximum amount of time to wait for messages in seconds.

        Returns:
            QueueMessagesPulled: An object containing the pulled messages.

        Raises:
            ValueError: If channel is None, max_messages is less than 1, or wait_timeout_in_seconds is less than 1.
        """
        self.logger.debug(f"Pulling messages from queue: {channel}")
        if channel is None:
            raise ValueError("channel cannot be None.")
        if max_messages < 1:
            raise ValueError("max_messages must be greater than 0.")
        if wait_timeout_in_seconds < 1:
            raise ValueError("wait_timeout_in_seconds must be greater than 0.")

        request = ReceiveQueueMessagesRequest(
            RequestID=str(uuid.uuid4()),
            ClientID=self.connection.client_id,
            Channel=channel,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_timeout_in_seconds,
            IsPeak=False,
        )

        response = self.transport.kubemq_client().ReceiveQueueMessages(request)
        pulled_messages = QueueMessagesPulled(
            is_error=response.IsError, error=response.Error
        )

        if not response.Messages:
            return pulled_messages

        self.logger.debug(f"Pulled messages count: {len(response.Messages)}")
        for message in response.Messages:
            pulled_messages.messages.append(
                QueueMessageWaitingPulled.decode(message, self.connection.client_id)
            )

        return pulled_messages
