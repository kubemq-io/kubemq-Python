import logging
import threading
import time
import grpc
import socket
from typing import List
from kubemq.transport import *
from kubemq.cq import *
from kubemq.common.exceptions import *
from kubemq.common.helpers import *
from kubemq.common.requests import *
from kubemq.common.subscribe_type import *
from kubemq.common.cancellation_token import *


class Client:
    """
    Client class represents a client for connecting to the KubeMQ server.

    Attributes:
        connection (Connection): The connection configuration for the client.
        logger (Logger): The logger used for logging messages.
        transport (Transport): The transport object for establishing the connection.
        shutdown_event (threading.Event): An event used for triggering client shutdown.

    Methods:
        __init__(address: str = "",
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
                 log_level: int = None) -> None:
            Initializes a new instance of the Client class.

            Args:
                address (str): The address of the KubeMQ server.
                client_id (str): The client identifier.
                auth_token (str): The authentication token.
                tls (bool): Indicates if the connection should use TLS.
                tls_cert_file (str): The path to the TLS certificate file.
                tls_key_file (str): The path to the TLS private key file.
                tls_ca_file (str): The path to the TLS CA certificate file.
                max_send_size (int): The maximum size of a single send request.
                max_receive_size (int): The maximum size of a single receive request.
                disable_auto_reconnect (bool): Indicates if auto-reconnecting on connection failure is disabled.
                reconnect_interval_seconds (int): The interval between reconnection attempts in seconds.
                keep_alive (bool): Indicates if the connection should maintain keep-alive messages.
                ping_interval_in_seconds (int): The interval between ping messages in seconds.
                ping_timeout_in_seconds (int): The timeout for a ping message in seconds.
                log_level (int): The log level for the logger.

        __enter__(self) -> Client:
            Returns the current instance of the Client class.

        __exit__(self, exc_type, exc_value, traceback) -> None:
            Closes the connection when exiting the context.

            Args:
                exc_type: The exception type.
                exc_value: The exception value.
                traceback: The traceback.

        connect(self) -> None:
            Connects to the KubeMQ server.

        close(self) -> None:
            Closes the connection to the KubeMQ server.

        ping(self) -> ServerInfo:
            Pings the KubeMQ server.

            Returns:
                ServerInfo: The server information.

        send_command_request(self, message: CommandMessage) -> CommandResponseMessage:
            Sends a command request to the KubeMQ server.

            Args:
                message (CommandMessage): The command message to send.

            Returns:
                CommandResponseMessage: The command response message.

        send_query_request(self, message: QueryMessage) -> QueryResponseMessage:
            Sends a query request to the KubeMQ server.

            Args:
                message (QueryMessage): The query message to send.

            Returns:
                QueryResponseMessage: The query response message.

        send_response_message(self, message: [CommandResponseMessage, QueryResponseMessage]) -> None:
            Sends a response message to the KubeMQ server.

            Args:
                message (CommandResponseMessage or QueryResponseMessage): The response message to send.

        create_commands_channel(self, channel: str) -> bool or None:
            Creates a commands channel on the KubeMQ server.

            Args:
                channel (str): The name of the channel.

            Returns:
                bool or None: True if the channel was created successfully, None if an error occurred.

        create_queries_channel(self, channel: str) -> None:
            Creates a queries channel on the KubeMQ server.

            Args:
                channel (str): The name of the channel.

        delete_commands_channel(self, channel: str) -> None:
            Deletes a commands channel from the KubeMQ server.

            Args:
                channel (str): The name of the channel.

        delete_queries_channel(self, channel: str) -> None:
            Deletes a queries channel from the KubeMQ server.

            Args:
                channel (str): The name of the channel.

        list_commands_channels(self, channel_search: str = "") -> List[CQChannel]:
            Lists the available commands channels on the KubeMQ server.

            Args:
                channel_search (str): The search pattern for filtering channel names.

            Returns:
                List[CQChannel]: A list of CQChannel objects representing the commands channels.

        list_queries_channels(self, channel_search: str = "") -> List[CQChannel]:
            Lists the available queries channels on the KubeMQ server.

            Args:
                channel_search (str): The search pattern for filtering channel names.

            Returns:
                List[CQChannel]: A list of CQChannel objects representing the queries channels.

        subscribe_to_commands(self, subscription: CommandsSubscription, cancel: CancellationToken = None) -> None:
            Subscribes to commands on the KubeMQ server.

            Args:
                subscription (CommandsSubscription): The commands subscription.
                cancel (CancellationToken): The cancellation token for canceling the subscription.

        subscribe_to_queries(self, subscription: QueriesSubscription, cancel: CancellationToken = None) -> None:
            Subscribes to queries on the KubeMQ server.

            Args:
                subscription (QueriesSubscription): The queries subscription.
                cancel (CancellationToken): The cancellation token for canceling the subscription.
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
            self.logger.debug(f"Client disconnecting from {self.connection.address}")
            self.transport.close()
            self.logger.debug(f"Client disconnected from {self.connection.address}")
            self.shutdown_event.set()
        except Exception as e:
            ex = GRPCError(e)
            self.logger.error(str(ex))
            raise ex

    def ping(self) -> ServerInfo:
        """
        Pings the server to check the connection status.

        Returns:
            ServerInfo: Information about the server.

        Raises:
            GRPCError: If an exception occurs while pinging the server.
        """
        try:
            self.logger.debug(f"Client pinging {self.connection.address}")
            return self.transport.ping()
        except Exception as e:
            ex = GRPCError(e)
            self.logger.error(str(ex))
            raise ex

    def send_command_request(self, message: CommandMessage) -> CommandResponseMessage:
        """
        Send a command request to the Kubemq server.

        Parameters:
            message (CommandMessage): The command message to send.

        Returns:
            CommandResponseMessage: The response message received from the server.
        """
        response = self.transport.kubemq_client().SendRequest(
            message.encode(self.connection.client_id)
        )
        return CommandResponseMessage().decode(response)

    def send_query_request(self, message: QueryMessage) -> QueryResponseMessage:
        """

        Send a query request to the Kubemq server.

        Parameters:
        - message: An instance of the QueryMessage class representing the query message to be sent.

        Returns:
        - An instance of the QueryResponseMessage class representing the response received from the Kubemq server.

        Note:
        - The parameter message needs to be validated before sending the request.
        - The request is sent using the Kubemq client's SendRequest method with the encoded message and the client ID.
        - The response received from the Kubemq server is decoded into an instance of the QueryResponseMessage class.

        """
        response = self.transport.kubemq_client().SendRequest(
            message.encode(self.connection.client_id)
        )
        return QueryResponseMessage().decode(response)

    def send_response_message(
        self, message: [CommandResponseMessage, QueryResponseMessage]
    ) -> None:
        """

        Sends a response message using the Kubemq client.

        Parameters:
        - message: A CommandResponseMessage or QueryResponseMessage object. The message to be sent as a response.

        Return Type:
        None

        """
        self.transport.kubemq_client().SendResponse(
            message.encode(self.connection.client_id)
        )

    def create_commands_channel(self, channel: str) -> [bool, None]:
        """Create a commands channel for the given channel.

        Args:
            channel (str): The name of the channel to create the commands channel for.

        Returns:
            [bool, None]: Returns either True if the commands channel was created successfully, or None if there was an error.
        """
        return create_channel_request(
            self.transport, self.connection.client_id, channel, "commands"
        )

    def create_queries_channel(self, channel: str):
        """
        Create a queries channel for the given channel.

        Parameters:
            channel (str): The name of the channel to create a queries channel for.

        Returns:
            None
        """
        return create_channel_request(
            self.transport, self.connection.client_id, channel, "queries"
        )

    def delete_commands_channel(self, channel: str):
        """
        Delete the "commands" channel for the specified client.

        :param channel: The name of the channel to delete.
        :type channel: str
        """
        return delete_channel_request(
            self.transport, self.connection.client_id, channel, "commands"
        )

    def delete_queries_channel(self, channel: str):
        """
        Delete a queries channel.

        :param channel: The name of the channel to delete.
        """
        return delete_channel_request(
            self.transport, self.connection.client_id, channel, "queries"
        )

    def list_commands_channels(self, channel_search: str = "") -> List[CQChannel]:
        """
        Lists the CQChannels that have the "commands" capability and match the provided search string.

        Args:
            channel_search (str): Optional. A string used to filter the channels by name.

        Returns:
            List[CQChannel]: A list of CQChannel objects that have the "commands" capability and match the search criteria.
        """
        return list_cq_channels(
            self.transport, self.connection.client_id, "commands", channel_search
        )

    def list_queries_channels(self, channel_search: str = "") -> List[CQChannel]:
        """
        Retrieves a list of CQChannels representing the available query channels.

        Args:
            channel_search (str, optional): A string used to filter the query channels. Only channels matching
                                            the search string will be returned. Defaults to "".

        Returns:
            List[CQChannel]: A list of CQChannel objects representing the available query channels.

        """
        return list_cq_channels(
            self.transport, self.connection.client_id, "queries", channel_search
        )

    def subscribe_to_commands(
        self, subscription: CommandsSubscription, cancel: CancellationToken = None
    ):
        """
        Subscribes to commands using the specified subscription and cancellation token (optional).

        :param subscription: The CommandsSubscription object that contains the commands to subscribe to.
        :param cancel: The CancellationToken object used to cancel the subscription (optional).
        """
        self._subscribe(subscription, cancel)

    def subscribe_to_queries(
        self, subscription: QueriesSubscription, cancel: CancellationToken = None
    ):
        """
        Subscribes to the given query subscription.

        :param subscription: The query subscription to subscribe to.
        :type subscription: QueriesSubscription
        :param cancel: The cancellation token to cancel the subscription (optional).
        :type cancel: CancellationToken, defaults to None
        """
        self._subscribe(subscription, cancel)

    def _subscribe(
        self,
        subscription: [CommandsSubscription, QueriesSubscription],
        cancel: [CancellationToken, None],
    ):
        if cancel is None:
            cancel = CancellationToken()
        cancel_token_event = cancel.event
        args = ()
        if isinstance(subscription, CommandsSubscription):
            args = (
                lambda: self.transport.kubemq_client().SubscribeToRequests(
                    subscription.decode(self.connection.client_id)
                ),
                lambda message: subscription.raise_on_receive_message(
                    CommandMessageReceived().decode(message)
                ),
                lambda error: subscription.raise_on_error(error),
                cancel_token_event,
            )
        if isinstance(subscription, QueriesSubscription):
            args = (
                lambda: self.transport.kubemq_client().SubscribeToRequests(
                    subscription.encode(self.connection.client_id)
                ),
                lambda message: subscription.raise_on_receive_message(
                    QueryMessageReceived().decode(message)
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
