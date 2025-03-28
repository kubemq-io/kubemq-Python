"""
Error Handling Strategy:
- Methods that interact directly with the KubeMQ server (ping, send_queues_message, etc.)
  will raise exceptions when errors occur.
- Helper methods (create_queues_channel, delete_queues_channel, etc.) will return None
  when errors occur, as they are wrappers around lower-level functions.
- All exceptions are logged before being raised or returned.
"""

import asyncio
import logging
import threading
import time
import uuid
import socket
from typing import List, Optional

from kubemq.transport.transport import Transport
from kubemq.transport.connection import Connection
from kubemq.transport.tls_config import TlsConfig
from kubemq.transport.keep_alive import KeepAliveConfig
from kubemq.transport.server_info import ServerInfo

from kubemq.queues.queues_message import QueueMessage
from kubemq.queues.queues_send_result import QueueSendResult
from kubemq.queues.queues_poll_response import QueuesPollResponse
from kubemq.queues.upstream_sender import UpstreamSender
from kubemq.queues.downstream_receiver import DownstreamReceiver
from kubemq.queues.queues_messages_waiting_pulled import (
    QueueMessagesWaiting,
    QueueMessagesPulled,
    QueueMessageWaitingPulled,
)

from kubemq.common.exceptions import ValidationError, GRPCError
from kubemq.common import create_channel_request
from kubemq.common.requests import delete_channel_request, list_queues_channels
from kubemq.common.channel_stats import QueuesChannel
from kubemq.grpc import (
    ReceiveQueueMessagesRequest,
    QueuesDownstreamRequest,
    QueuesDownstreamResponse,
    QueuesDownstreamRequestType,
)


class Client:
    """
    Client class represents a client that connects to the KubeMQ server.

    This class provides methods for sending and receiving messages, creating and
    deleting channels, and managing the connection to the KubeMQ server.

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
        create_queues_channel(channel: str) -> Optional[bool]: Creates a queues channel.
        delete_queues_channel(channel: str) -> Optional[bool]: Deletes a queues channel.
        list_queues_channels(channel_search: str) -> List[QueuesChannel]: Lists the queues channels.
        receive_queues_messages(channel: str = None, max_messages: int = 1,
                               wait_timeout_in_seconds: int = 60, auto_ack: bool = False) -> QueuesPollResponse:
            Receives messages from a queues channel.

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
        # New parameters for sender/receiver configuration
        queue_size: int = 0,
        queue_timeout: float = 0.1,
        request_sleep_interval: float = 0.1,
        send_timeout: float = 2.0,
        connection_monitor_interval: float = 1.0,
    ) -> None:
        """Initialize a new Client.

        Args:
            address: The address of the KubeMQ server
            client_id: The client ID to use
            auth_token: The authentication token to use
            tls: Whether to use TLS
            tls_cert_file: The path to the TLS certificate file
            tls_key_file: The path to the TLS key file
            tls_ca_file: The path to the TLS CA file
            max_send_size: The maximum size of messages to send
            max_receive_size: The maximum size of messages to receive
            disable_auto_reconnect: Whether to disable auto-reconnect
            reconnect_interval_seconds: The interval between reconnection attempts
            keep_alive: Whether to use keep-alive
            ping_interval_in_seconds: The interval between ping messages
            ping_timeout_in_seconds: The timeout for ping messages
            log_level: The log level to use
            queue_size: Maximum size of the request queue (0 for unlimited)
            queue_timeout: Timeout in seconds for queue polling
            request_sleep_interval: Sleep interval in seconds between requests
            send_timeout: Timeout in seconds for waiting for a send response
            connection_monitor_interval: Interval in seconds for monitoring connection status
        """
        self.connection = Connection(
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

        self.logger.debug("Initializing client with connection parameters:")
        self.logger.debug(f" - Address: {address}")
        self.logger.debug(f" - Client ID: {client_id}")
        self.logger.debug(f" - Disable Auto Reconnect: {disable_auto_reconnect}")
        self.logger.debug(
            f" - Reconnect Interval: {reconnect_interval_seconds} seconds"
        )

        # Store configuration for sender/receiver
        self.queue_size = queue_size
        self.queue_timeout = queue_timeout
        self.request_sleep_interval = request_sleep_interval
        self.send_timeout = send_timeout
        self.connection_monitor_interval = connection_monitor_interval

        try:
            self.transport = Transport(self.connection).initialize()
            self.shutdown_event = threading.Event()
            self.upstream_sender = None
            self.downstream_receiver = None

            # Monitor connection status for logging
            connection_monitor = threading.Thread(
                target=self._monitor_connection, daemon=True
            )
            connection_monitor.start()
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

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        self.close()

    def connect(self) -> None:
        """Connect to the KubeMQ server if not already connected."""
        try:
            self.logger.debug(f"Client connecting to {self.connection.address}")
            if not self.transport.is_connected():
                self.transport = Transport(self.connection).initialize()
            self.logger.debug(f"Client connected to {self.connection.address}")
        except Exception as e:
            ex = GRPCError(e)
            self.logger.error(str(ex))
            raise ex

    def close(self) -> None:
        """Close the connection to the KubeMQ server and release resources."""
        try:
            # time.sleep(1) - Removed as it's not clear why this is needed
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

    async def close_async(self) -> None:
        """
        Asynchronous version of close().

        Closes the connection to the KubeMQ server and releases resources asynchronously.
        """
        await asyncio.to_thread(self.close)

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

    async def ping_async(self) -> ServerInfo:
        """
        Asynchronous version of ping().

        Returns:
            ServerInfo: The server information.

        Raises:
            GRPCError: If an error occurs during the ping.
        """
        return await asyncio.to_thread(self.ping)

    def send_queues_message(self, message: QueueMessage) -> QueueSendResult:
        """
        Sends a message to the queues.

        Args:
            message (QueueMessage): The message to be sent.

        Returns:
            QueueSendResult: The result of the message send operation.
        """
        # message.validate()
        if self.upstream_sender is None:
            self.upstream_sender = UpstreamSender(
                self.transport,
                self.logger,
                self.connection,
                queue_size=self.queue_size,
                queue_timeout=self.queue_timeout,
                request_sleep_interval=self.request_sleep_interval,
                send_timeout=self.send_timeout,
            )

        return self.upstream_sender.send(message)

    async def send_queues_message_async(self, message: QueueMessage) -> QueueSendResult:
        """
        Asynchronous version of send_queues_message().

        Args:
            message (QueueMessage): The message to be sent.

        Returns:
            QueueSendResult: The result of the message send operation.
        """
        return await asyncio.to_thread(self.send_queues_message, message)

    def create_queues_channel(self, channel: str) -> Optional[bool]:
        """
        Create a queues channel for the given channel.

        Args:
            channel (str): The name of the channel to create.

        Returns:
            Optional[bool]: Returns True if the channel is created successfully,
                           None if there was an error during creation.
        """
        return create_channel_request(
            self.transport, self.connection.client_id, channel, "queues"
        )

    async def create_queues_channel_async(self, channel: str) -> Optional[bool]:
        """
        Asynchronous version of create_queues_channel().

        Args:
            channel (str): The name of the channel to create.

        Returns:
            Optional[bool]: Returns True if the channel is created successfully,
                           None if there was an error during creation.
        """
        return await asyncio.to_thread(self.create_queues_channel, channel)

    def delete_queues_channel(self, channel: str) -> Optional[bool]:
        """
        Delete Queues Channel

        Deletes a specific channel from the queues in the current connection.

        Args:
            channel (str): The name of the channel to delete.

        Returns:
            Optional[bool]: Returns True if the channel was successfully deleted,
                           None if there was an error during deletion.
        """
        return delete_channel_request(
            self.transport, self.connection.client_id, channel, "queues"
        )

    async def delete_queues_channel_async(self, channel: str) -> Optional[bool]:
        """
        Asynchronous version of delete_queues_channel().

        Args:
            channel (str): The name of the channel to delete.

        Returns:
            Optional[bool]: Returns True if the channel was successfully deleted,
                           None if there was an error during deletion.
        """
        return await asyncio.to_thread(self.delete_queues_channel, channel)

    def list_queues_channels(self, channel_search: str) -> List[QueuesChannel]:
        """
        Retrieve a list of QueuesChannel objects based on the provided channel_search.

        Args:
            channel_search (str): The search term used to filter the list of QueuesChannels.

        Returns:
            List[QueuesChannel]: A list of QueuesChannel objects that match the search term.
        """
        return list_queues_channels(
            self.transport, self.connection.client_id, channel_search
        )

    async def list_queues_channels_async(
        self, channel_search: str
    ) -> List[QueuesChannel]:
        """
        Asynchronous version of list_queues_channels().

        Args:
            channel_search (str): The search term used to filter the list of QueuesChannels.

        Returns:
            List[QueuesChannel]: A list of QueuesChannel objects that match the search term.
        """
        return await asyncio.to_thread(self.list_queues_channels, channel_search)

    def receive_queues_messages(
        self,
        channel: str = None,
        max_messages: int = 1,
        wait_timeout_in_seconds: int = 60,
        auto_ack: bool = False,
        visibility_seconds: int = 0,
    ) -> QueuesPollResponse:
        """
        Receive messages from a queues channel.

        Args:
            channel (str): The name of the channel to receive messages from.
            max_messages (int): The maximum number of messages to receive.
            wait_timeout_in_seconds (int): The timeout in seconds to wait for messages.
            auto_ack (bool): Whether to automatically acknowledge messages.
            visibility_seconds (int): The visibility timeout in seconds for received messages.

        Returns:
            QueuesPollResponse: The response containing the received messages.
        """
        if self.downstream_receiver is None:
            self.downstream_receiver = DownstreamReceiver(
                self.transport,
                self.logger,
                self.connection,
                queue_size=self.queue_size,
                queue_timeout=self.queue_timeout,
                request_sleep_interval=self.request_sleep_interval,
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

    async def receive_queues_messages_async(
        self,
        channel: str = None,
        max_messages: int = 1,
        wait_timeout_in_seconds: int = 60,
        auto_ack: bool = False,
        visibility_seconds: int = 0,
    ) -> QueuesPollResponse:
        """
        Asynchronous version of receive_queues_messages().

        Args:
            channel (str): The name of the channel to receive messages from.
            max_messages (int): The maximum number of messages to receive.
            wait_timeout_in_seconds (int): The timeout in seconds to wait for messages.
            auto_ack (bool): Whether to automatically acknowledge messages.
            visibility_seconds (int): The visibility timeout in seconds for received messages.

        Returns:
            QueuesPollResponse: The response containing the received messages.
        """
        return await asyncio.to_thread(
            self.receive_queues_messages,
            channel,
            max_messages,
            wait_timeout_in_seconds,
            auto_ack,
            visibility_seconds,
        )

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

    async def waiting_async(
        self, channel: str, max_messages: int, wait_timeout_in_seconds: int
    ) -> QueueMessagesWaiting:
        """
        Asynchronous version of waiting().

        Args:
            channel (str): The name of the queue channel.
            max_messages (int): The maximum number of messages to retrieve.
            wait_timeout_in_seconds (int): The maximum amount of time to wait for messages in seconds.

        Returns:
            QueueMessagesWaiting: An object containing the waiting messages.

        Raises:
            ValueError: If channel is None, max_messages is less than 1, or wait_timeout_in_seconds is less than 1.
        """
        return await asyncio.to_thread(
            self.waiting, channel, max_messages, wait_timeout_in_seconds
        )

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

    async def pull_async(
        self, channel: str, max_messages: int, wait_timeout_in_seconds: int
    ) -> QueueMessagesPulled:
        """
        Asynchronous version of pull().

        Args:
            channel (str): The name of the queue channel.
            max_messages (int): The maximum number of messages to pull.
            wait_timeout_in_seconds (int): The maximum amount of time to wait for messages in seconds.

        Returns:
            QueueMessagesPulled: An object containing the pulled messages.

        Raises:
            ValueError: If channel is None, max_messages is less than 1, or wait_timeout_in_seconds is less than 1.
        """
        return await asyncio.to_thread(
            self.pull, channel, max_messages, wait_timeout_in_seconds
        )

    def _monitor_connection(self) -> None:
        """Monitor the connection status and log changes."""
        last_status = True
        while not self.shutdown_event.is_set():
            current_status = self.transport.is_connected()
            if current_status != last_status:
                if current_status:
                    self.logger.info(
                        f"Connection to {self.connection.address} restored"
                    )
                else:
                    self.logger.warning(f"Connection to {self.connection.address} lost")
                last_status = current_status
            time.sleep(self.connection_monitor_interval)
