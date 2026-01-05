"""Queues client for KubeMQ."""

from __future__ import annotations

import threading
import time
import uuid
from pathlib import Path

from kubemq.common import create_channel_request
from kubemq.common.channel_stats import QueuesChannel
from kubemq.common.requests import delete_channel_request, list_queues_channels
from kubemq.core import BaseClient, ClientConfig
from kubemq.core.compat import deprecated_async_method, run_in_thread
from kubemq.core.config import KeepAliveConfig, TLSConfig
from kubemq.grpc import (
    QueuesDownstreamRequest,
    QueuesDownstreamRequestType,
    QueuesDownstreamResponse,
    ReceiveQueueMessagesRequest,
)
from kubemq.queues.downstream_receiver import DownstreamReceiver
from kubemq.queues.queues_message import QueueMessage
from kubemq.queues.queues_messages_waiting_pulled import (
    QueueMessagesPulled,
    QueueMessagesWaiting,
    QueueMessageWaitingPulled,
)
from kubemq.queues.queues_poll_response import QueuesPollResponse
from kubemq.queues.queues_send_result import QueueSendResult
from kubemq.queues.upstream_sender import UpstreamSender
from kubemq.transport.server_info import ServerInfo


class Client(BaseClient):
    """Queues client for sending and receiving queue messages.

    Note:
        The `*_async()` methods on this class use thread-based async wrappers.
        For native async support with better performance, use `AsyncClient`
        (or `AsyncQueuesClient`) from `kubemq.queues.async_client` instead::

            from kubemq.queues import AsyncClient

            async with AsyncClient(address="localhost:50000") as client:
                result = await client.send_queue_message(QueueMessage(
                    channel="my-queue",
                    body=b"Hello, World!"
                ))

    Example:
        # Using context manager (recommended)
        with Client(address="localhost:50000") as client:
            result = client.send_queues_message(QueueMessage(
                channel="my-queue",
                body=b"Hello, World!"
            ))

        # Using configuration object
        config = ClientConfig(address="localhost:50000", client_id="my-app")
        client = Client(config=config)
        try:
            result = client.send_queues_message(...)
        finally:
            client.close()
    """

    def __init__(
        self,
        address: str = "",
        client_id: str | None = None,
        auth_token: str | None = None,
        config: ClientConfig | None = None,
        # Legacy parameters for backward compatibility
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
        log_level: int | None = None,
        # Queues-specific parameters
        send_timeout: float = 2.0,
        connection_monitor_interval: float = 1.0,
    ) -> None:
        """Initialize the Queues client.

        Args:
            address: KubeMQ server address (host:port)
            client_id: Client identifier (defaults to hostname)
            auth_token: Authentication token
            config: Pre-built ClientConfig object (overrides other params)
            tls: Whether to use TLS (legacy)
            tls_cert_file: Path to TLS certificate (legacy)
            tls_key_file: Path to TLS key (legacy)
            tls_ca_file: Path to TLS CA certificate (legacy)
            max_send_size: Maximum send message size (legacy)
            max_receive_size: Maximum receive message size (legacy)
            disable_auto_reconnect: Disable auto-reconnect (legacy)
            reconnect_interval_seconds: Reconnect interval (legacy)
            keep_alive: Enable keep-alive (legacy)
            ping_interval_in_seconds: Keep-alive ping interval (legacy)
            ping_timeout_in_seconds: Keep-alive ping timeout (legacy)
            log_level: Logging level (legacy)
            send_timeout: Timeout for send operations in seconds
            connection_monitor_interval: Interval for connection monitoring in seconds
        """
        # Build config from legacy parameters if not provided
        if config is None:
            tls_config = (
                TLSConfig(
                    enabled=tls,
                    cert_file=Path(tls_cert_file) if tls_cert_file else None,
                    key_file=Path(tls_key_file) if tls_key_file else None,
                    ca_file=Path(tls_ca_file) if tls_ca_file else None,
                )
                if tls
                else TLSConfig()
            )

            keep_alive_config = (
                KeepAliveConfig(
                    enabled=keep_alive,
                    ping_interval_in_seconds=ping_interval_in_seconds or 30,
                    ping_timeout_in_seconds=ping_timeout_in_seconds or 10,
                )
                if keep_alive
                else KeepAliveConfig()
            )

            config = ClientConfig(
                address=address,
                client_id=client_id,
                auth_token=auth_token,
                tls=tls_config,
                keep_alive=keep_alive_config,
                max_send_size=max_send_size or ClientConfig.DEFAULT_MAX_MESSAGE_SIZE,
                max_receive_size=max_receive_size or ClientConfig.DEFAULT_MAX_MESSAGE_SIZE,
                auto_reconnect=not disable_auto_reconnect,
                reconnect_interval_seconds=reconnect_interval_seconds or 1,
                log_level=log_level,
            )

        super().__init__(config=config)

        # Queues-specific initialization
        self.send_timeout = send_timeout
        self.connection_monitor_interval = connection_monitor_interval
        self._upstream_sender: UpstreamSender | None = None
        self._downstream_receiver: DownstreamReceiver | None = None
        self._upstream_sender_lock = threading.Lock()
        self._downstream_receiver_lock = threading.Lock()

        # Legacy attribute for backward compatibility
        self.connection = self._config.to_legacy_connection()

        # Start connection monitor
        connection_monitor = threading.Thread(target=self._monitor_connection, daemon=True)
        connection_monitor.start()

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close_async()

    async def close_async(self) -> None:
        """Asynchronous version of close().

        Closes the connection to the KubeMQ server and releases resources asynchronously.
        """
        self._shutdown_event.set()
        if self._upstream_sender is not None:
            self._upstream_sender.close()
        if self._downstream_receiver is not None:
            self._downstream_receiver.close()
        self._logger.debug(f"Client disconnecting from {self._config.address}")
        if self._transport is not None:
            await self._transport.close_async()

    @deprecated_async_method("AsyncQueuesClient.ping")
    async def ping_async(self) -> ServerInfo:
        """Asynchronous version of ping().

        Deprecated:
            Use `AsyncQueuesClient.ping()` for native async support.

        Returns:
            ServerInfo: The server information.

        Raises:
            KubeMQConnectionError: If an error occurs during the ping.
        """
        return await run_in_thread(self.ping)

    def _cleanup_resources(self) -> None:
        """Clean up Queues-specific resources."""
        if self._upstream_sender is not None:
            self._upstream_sender.close()
            self._upstream_sender = None
        if self._downstream_receiver is not None:
            self._downstream_receiver.close()
            self._downstream_receiver = None

    def _monitor_connection(self) -> None:
        """Monitor the connection status and log changes."""
        last_status = True
        while not self._shutdown_event.is_set():
            current_status = self._transport.is_connected() if self._transport else False
            if current_status != last_status:
                if current_status:
                    self._logger.info(f"Connection to {self._config.address} restored")
                else:
                    self._logger.warning(f"Connection to {self._config.address} lost")
                last_status = current_status
            time.sleep(self.connection_monitor_interval)

    def _get_upstream_sender(self) -> UpstreamSender:
        """Thread-safe lazy initialization of upstream sender."""
        self._ensure_connected()
        assert self._transport is not None
        with self._upstream_sender_lock:
            if self._upstream_sender is None:
                self._upstream_sender = UpstreamSender(
                    self._transport,
                    self._logger,
                    self.connection,
                    send_timeout=self.send_timeout,
                )
            return self._upstream_sender

    def _get_downstream_receiver(self) -> DownstreamReceiver:
        """Thread-safe lazy initialization of downstream receiver."""
        self._ensure_connected()
        assert self._transport is not None
        with self._downstream_receiver_lock:
            if self._downstream_receiver is None:
                self._downstream_receiver = DownstreamReceiver(
                    self._transport,
                    self._logger,
                    self.connection,
                )
            return self._downstream_receiver

    def send_queues_message(self, message: QueueMessage) -> QueueSendResult:
        """Send a message to the queues.

        Args:
            message: The message to send

        Returns:
            QueueSendResult with the result of the send operation
        """
        sender = self._get_upstream_sender()
        pb_message = message.encode_message(self._config.client_id or "")
        result = sender.send(pb_message)
        if result is None:
            return QueueSendResult(is_error=True, error="Send failed - no response")
        return result

    @deprecated_async_method("AsyncQueuesClient.send_queue_message")
    async def send_queues_message_async(self, message: QueueMessage) -> QueueSendResult:
        """Send a message to the queues asynchronously.

        Deprecated:
            Use `AsyncQueuesClient.send_queue_message()` for native async support.

        Args:
            message: The message to send

        Returns:
            QueueSendResult with the result of the send operation
        """
        return await run_in_thread(self.send_queues_message, message)

    def create_queues_channel(self, channel: str) -> bool | None:
        """Create a queues channel.

        Args:
            channel: The name of the channel to create

        Returns:
            True if successful, None if there was an error
        """
        return create_channel_request(self._transport, self._config.client_id, channel, "queues")

    async def create_queues_channel_async(self, channel: str) -> bool | None:
        """Create a queues channel asynchronously.

        Args:
            channel: The name of the channel to create

        Returns:
            True if successful, None if there was an error
        """
        return await run_in_thread(self.create_queues_channel, channel)

    def delete_queues_channel(self, channel: str) -> bool | None:
        """Delete a queues channel.

        Args:
            channel: The name of the channel to delete

        Returns:
            True if successful, None if there was an error
        """
        return delete_channel_request(self._transport, self._config.client_id, channel, "queues")

    async def delete_queues_channel_async(self, channel: str) -> bool | None:
        """Delete a queues channel asynchronously.

        Args:
            channel: The name of the channel to delete

        Returns:
            True if successful, None if there was an error
        """
        return await run_in_thread(self.delete_queues_channel, channel)

    def list_queues_channels(self, channel_search: str = "") -> list[QueuesChannel]:
        """List queues channels.

        Args:
            channel_search: Optional filter string

        Returns:
            List of QueuesChannel objects
        """
        return list_queues_channels(self._transport, self._config.client_id, channel_search)

    async def list_queues_channels_async(self, channel_search: str = "") -> list[QueuesChannel]:
        """List queues channels asynchronously.

        Args:
            channel_search: Optional filter string

        Returns:
            List of QueuesChannel objects
        """
        return await run_in_thread(self.list_queues_channels, channel_search)

    def receive_queues_messages(
        self,
        channel: str | None = None,
        max_messages: int = 1,
        wait_timeout_in_seconds: int = 60,
        auto_ack: bool = False,
        visibility_seconds: int = 0,
    ) -> QueuesPollResponse:
        """Receive messages from a queues channel.

        Args:
            channel: The name of the channel to receive messages from
            max_messages: Maximum number of messages to receive
            wait_timeout_in_seconds: Timeout in seconds to wait for messages
            auto_ack: Whether to automatically acknowledge messages
            visibility_seconds: Visibility timeout in seconds for received messages

        Returns:
            QueuesPollResponse containing the received messages
        """
        receiver = self._get_downstream_receiver()
        client_id = self._config.client_id or ""

        request = QueuesDownstreamRequest()
        request.RequestID = str(uuid.uuid4())
        request.ClientID = client_id
        request.Channel = channel or ""
        request.MaxItems = max_messages
        request.WaitTimeout = wait_timeout_in_seconds * 1000
        request.AutoAck = auto_ack
        request.RequestTypeData = QueuesDownstreamRequestType.Get
        kubemq_response = receiver.send(request)
        if kubemq_response is None:
            return QueuesPollResponse()
        response = QueuesPollResponse().decode(
            response=kubemq_response,
            receiver_client_id=client_id,
            response_handler=receiver.send_without_response,  # type: ignore[arg-type]
            request_visibility_seconds=visibility_seconds,
        )

        return response

    async def receive_queues_messages_async(
        self,
        channel: str | None = None,
        max_messages: int = 1,
        wait_timeout_in_seconds: int = 60,
        auto_ack: bool = False,
        visibility_seconds: int = 0,
    ) -> QueuesPollResponse:
        """Receive messages from a queues channel asynchronously.

        Args:
            channel: The name of the channel to receive messages from
            max_messages: Maximum number of messages to receive
            wait_timeout_in_seconds: Timeout in seconds to wait for messages
            auto_ack: Whether to automatically acknowledge messages
            visibility_seconds: Visibility timeout in seconds for received messages

        Returns:
            QueuesPollResponse containing the received messages
        """
        return await run_in_thread(
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
        """Get waiting messages from a queue (peek without removing).

        Args:
            channel: The name of the queue channel
            max_messages: Maximum number of messages to retrieve
            wait_timeout_in_seconds: Maximum time to wait for messages in seconds

        Returns:
            QueueMessagesWaiting containing the waiting messages

        Raises:
            ValueError: If parameters are invalid
        """
        self._logger.debug(f"Get waiting messages from queue: {channel}")
        if channel is None:
            raise ValueError("channel cannot be None.")
        if max_messages < 1:
            raise ValueError("max_messages must be greater than 0.")
        if wait_timeout_in_seconds < 1:
            raise ValueError("wait_timeout_in_seconds must be greater than 0.")

        self._ensure_connected()
        assert self._transport is not None
        client_id = self._config.client_id or ""

        request = ReceiveQueueMessagesRequest(
            RequestID=str(uuid.uuid4()),
            ClientID=client_id,
            Channel=channel,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_timeout_in_seconds,
            IsPeak=True,
        )

        response = self._transport.kubemq_client().ReceiveQueueMessages(request)
        waiting_messages = QueueMessagesWaiting(is_error=response.IsError, error=response.Error)

        if not response.Messages:
            return waiting_messages

        self._logger.debug(f"Waiting messages count: {len(response.Messages)}")
        for message in response.Messages:
            waiting_messages.messages.append(
                QueueMessageWaitingPulled.decode(message, client_id)
            )

        return waiting_messages

    async def waiting_async(
        self, channel: str, max_messages: int, wait_timeout_in_seconds: int
    ) -> QueueMessagesWaiting:
        """Get waiting messages from a queue asynchronously (peek without removing).

        Args:
            channel: The name of the queue channel
            max_messages: Maximum number of messages to retrieve
            wait_timeout_in_seconds: Maximum time to wait for messages in seconds

        Returns:
            QueueMessagesWaiting containing the waiting messages

        Raises:
            ValueError: If parameters are invalid
        """
        return await run_in_thread(self.waiting, channel, max_messages, wait_timeout_in_seconds)

    def pull(
        self, channel: str, max_messages: int, wait_timeout_in_seconds: int
    ) -> QueueMessagesPulled:
        """Pull messages from a queue (retrieve and remove).

        Args:
            channel: The name of the queue channel
            max_messages: Maximum number of messages to pull
            wait_timeout_in_seconds: Maximum time to wait for messages in seconds

        Returns:
            QueueMessagesPulled containing the pulled messages

        Raises:
            ValueError: If parameters are invalid
        """
        self._logger.debug(f"Pulling messages from queue: {channel}")
        if channel is None:
            raise ValueError("channel cannot be None.")
        if max_messages < 1:
            raise ValueError("max_messages must be greater than 0.")
        if wait_timeout_in_seconds < 1:
            raise ValueError("wait_timeout_in_seconds must be greater than 0.")

        self._ensure_connected()
        assert self._transport is not None
        client_id = self._config.client_id or ""

        request = ReceiveQueueMessagesRequest(
            RequestID=str(uuid.uuid4()),
            ClientID=client_id,
            Channel=channel,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_timeout_in_seconds,
            IsPeak=False,
        )

        response = self._transport.kubemq_client().ReceiveQueueMessages(request)
        pulled_messages = QueueMessagesPulled(is_error=response.IsError, error=response.Error)

        if not response.Messages:
            return pulled_messages

        self._logger.debug(f"Pulled messages count: {len(response.Messages)}")
        for message in response.Messages:
            pulled_messages.messages.append(
                QueueMessageWaitingPulled.decode(message, client_id)
            )

        return pulled_messages

    async def pull_async(
        self, channel: str, max_messages: int, wait_timeout_in_seconds: int
    ) -> QueueMessagesPulled:
        """Pull messages from a queue asynchronously (retrieve and remove).

        Args:
            channel: The name of the queue channel
            max_messages: Maximum number of messages to pull
            wait_timeout_in_seconds: Maximum time to wait for messages in seconds

        Returns:
            QueueMessagesPulled containing the pulled messages

        Raises:
            ValueError: If parameters are invalid
        """
        return await run_in_thread(self.pull, channel, max_messages, wait_timeout_in_seconds)
