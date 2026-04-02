"""Queues client for KubeMQ."""

from __future__ import annotations

import threading
import time
import uuid
from pathlib import Path


from kubemq._internal.deprecation import deprecated, deprecated_async
from kubemq._internal.telemetry import (
    KubeMQTagsCarrier,
    error_code_to_error_type,
)
from kubemq.common import create_channel_request
from kubemq.common.channel_stats import QueuesChannel
from kubemq.common.requests import delete_channel_request, list_queues_channels
from kubemq.core import BaseClient, ClientConfig
from kubemq.core.compat import run_in_thread
from kubemq.core.config import KeepAliveConfig, TLSConfig
from kubemq.core.exceptions import KubeMQValidationError
from kubemq.grpc import (
    QueuesDownstreamRequest,
    QueuesDownstreamRequestType,
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
from kubemq.queues.queues_send_result import QueueBatchSendResult, QueueSendResult
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

    Thread Safety:
        This class is thread-safe. Share across threads. One instance
        per application recommended.
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


        # Start connection monitor
        connection_monitor = threading.Thread(target=self._monitor_connection, daemon=True)
        connection_monitor.start()

    async def __aenter__(self) -> Client:
        """Async context manager entry."""
        return self

    async def __aexit__(
        self, exc_type: type | None, exc_val: BaseException | None, exc_tb: object | None
    ) -> None:
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

    @deprecated_async(replacement="AsyncQueuesClient.ping()", since="4.0.0", removal="5.0.0")
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
                    self._config,
                    send_timeout=self.send_timeout,
                    max_queue_size=self._config.max_send_queue_size,
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
                    self._config,
                    max_queue_size=self._config.max_send_queue_size,
                )
            return self._downstream_receiver

    def _send_queue_message_impl(self, message: QueueMessage) -> QueueSendResult:
        """Internal implementation for sending a queue message."""
        self._validate_message_size(message.body)
        start = time.perf_counter()
        error_type_val = None
        with self._instrumentor.start_span("send", message.channel) as span:
            try:
                sender = self._get_upstream_sender()
                pb_message = message.encode_message(self._config.client_id or "")
                tags_dict = dict(pb_message.Tags)
                KubeMQTagsCarrier(tags_dict).inject()
                pb_message.Tags.update(tags_dict)
                if span.is_recording():
                    from kubemq._internal.semconv import (
                        MESSAGING_MESSAGE_BODY_SIZE,
                        MESSAGING_MESSAGE_ID,
                    )

                    span.set_attribute(MESSAGING_MESSAGE_ID, message.id)
                    span.set_attribute(MESSAGING_MESSAGE_BODY_SIZE, len(message.body))
                result = sender.send(pb_message)
                self._instrumentor._metrics.record_sent_message("send", message.channel)
                if result is None:
                    return QueueSendResult(is_error=True, error="Send failed - no response")
                return result
            except (ValueError, TypeError) as e:
                error_type_val = "validation"
                self._instrumentor.record_error(span, e, error_type_val)
                raise KubeMQValidationError(str(e), is_retryable=False) from e
            except Exception as e:
                error_type_val = error_code_to_error_type(getattr(e, "code", None))
                self._instrumentor.record_error(span, e, error_type_val)
                raise
            finally:
                duration = time.perf_counter() - start
                self._instrumentor._metrics.record_operation_duration(
                    duration, "send", message.channel, error_type_val
                )

    def send_queue_message_simple(self, message: QueueMessage) -> QueueSendResult:
        """Send a single queue message via unary SendQueueMessage RPC.

        Uses the unary ``SendQueueMessage`` RPC for single-shot delivery.

        Args:
            message: The message to send.

        Returns:
            QueueSendResult: Contains ``is_error`` (bool), ``error``
            (error description), ``message_id`` (server-assigned ID),
            ``sent_at`` (timestamp), and ``expired_at`` (expiration time).

        Raises:
            KubeMQValidationError: If the message fails validation (e.g.,
                empty channel, body exceeds ``max_send_size``).
            KubeMQConnectionError: If the server is unreachable or the
                connection is lost.
            KubeMQAuthenticationError: If the auth token is invalid or
                expired, or the client lacks permission for the channel.
            KubeMQTimeoutError: If the operation exceeds the server deadline.
            KubeMQClientClosedError: If the client has already been closed.

        See Also:
            :meth:`send_queue_message`: Streaming variant with higher
                throughput.
            :meth:`receive_queue_messages`: Consume messages from a queue.
        """
        self._validate_message_size(message.body)
        self._ensure_connected()
        assert self._transport is not None
        start = time.perf_counter()
        error_type_val = None
        with self._instrumentor.start_span("send", message.channel) as span:
            try:
                pb_message = message.encode_message(self._config.client_id or "")
                tags_dict = dict(pb_message.Tags)
                KubeMQTagsCarrier(tags_dict).inject()
                pb_message.Tags.update(tags_dict)
                if span.is_recording():
                    from kubemq._internal.semconv import (
                        MESSAGING_MESSAGE_BODY_SIZE,
                        MESSAGING_MESSAGE_ID,
                    )

                    span.set_attribute(MESSAGING_MESSAGE_ID, message.id)
                    span.set_attribute(MESSAGING_MESSAGE_BODY_SIZE, len(message.body))
                result = self._transport.kubemq_client().SendQueueMessage(pb_message)
                self._instrumentor._metrics.record_sent_message("send", message.channel)
                return QueueSendResult.decode(result)
            except (ValueError, TypeError) as e:
                error_type_val = "validation"
                self._instrumentor.record_error(span, e, error_type_val)
                raise KubeMQValidationError(str(e), is_retryable=False) from e
            except Exception as e:
                error_type_val = error_code_to_error_type(getattr(e, "code", None))
                self._instrumentor.record_error(span, e, error_type_val)
                raise
            finally:
                duration = time.perf_counter() - start
                self._instrumentor._metrics.record_operation_duration(
                    duration, "send", message.channel, error_type_val
                )

    def send_queue_message(self, message: QueueMessage) -> QueueSendResult:
        """Send a message to a queue.

        Uses the bidirectional streaming upstream sender for higher
        throughput than the unary :meth:`send_queue_message_simple`.

        Args:
            message: The message to send.

        Returns:
            QueueSendResult: Contains ``is_error`` (bool), ``error``
            (error description), ``message_id`` (server-assigned ID),
            ``sent_at`` (timestamp), and ``expired_at`` (expiration time).

        Raises:
            KubeMQValidationError: If the message fails validation (e.g.,
                empty channel, body exceeds ``max_send_size``).
            KubeMQConnectionError: If the server is unreachable or the
                connection is lost.
            KubeMQAuthenticationError: If the auth token is invalid or
                expired, or the client lacks permission for the channel.
            KubeMQTimeoutError: If the operation exceeds the server deadline.
            KubeMQClientClosedError: If the client has already been closed.

        See Also:
            :class:`~kubemq.queues.queues_message.QueueMessage`:
                Message type for queue operations.
            :meth:`receive_queue_messages`: Consume messages from a queue.
            :meth:`send_queue_messages_batch`: Send multiple messages
                atomically.

        Example:
            >>> from kubemq.queues import Client
            >>> from kubemq.queues.queues_message import QueueMessage
            >>> with Client(address="localhost:50000") as client:
            ...     result = client.send_queue_message(QueueMessage(
            ...         channel="queues.tasks",
            ...         body=b'{"task": "send_email", "to": "user@example.com"}',
            ...         metadata="email-task",
            ...     ))
            ...     print(f"Sent: {not result.is_error}, ID: {result.message_id}")
        """
        return self._send_queue_message_impl(message)

    @deprecated(replacement="send_queue_message()", since="4.0.0", removal="5.0.0")
    def send_queues_message(self, message: QueueMessage) -> QueueSendResult:
        """Send a message to the queues.

        Deprecated:
            Use ``send_queue_message()`` instead. Will be removed in v5.0.

        Args:
            message: The message to send

        Returns:
            QueueSendResult: Contains ``is_error``, ``error``,
            ``message_id``, ``sent_at``, and ``expired_at`` fields.

        Raises:
            KubeMQValidationError: If the message fails validation.
            KubeMQConnectionError: If the server is unreachable.
            KubeMQAuthenticationError: If authentication or authorization
                fails.
            KubeMQClientClosedError: If the client has already been closed.
        """
        return self._send_queue_message_impl(message)

    @deprecated_async(
        replacement="AsyncQueuesClient.send_queue_message()", since="4.0.0", removal="5.0.0"
    )
    async def send_queues_message_async(self, message: QueueMessage) -> QueueSendResult:
        """Send a message to the queues asynchronously.

        Deprecated:
            Use ``AsyncQueuesClient.send_queue_message()`` for native async support.

        Args:
            message: The message to send

        Returns:
            QueueSendResult: Contains ``is_error``, ``error``,
            ``message_id``, ``sent_at``, and ``expired_at`` fields.

        Raises:
            KubeMQValidationError: If the message fails validation.
            KubeMQConnectionError: If the server is unreachable.
            KubeMQAuthenticationError: If authentication or authorization
                fails.
            KubeMQClientClosedError: If the client has already been closed.
        """
        return await run_in_thread(self._send_queue_message_impl, message)

    def send_queue_messages_batch(self, messages: list[QueueMessage]) -> QueueBatchSendResult:
        """Send multiple queue messages as a server-side batch.

        Uses the gRPC ``SendQueueMessagesBatch`` RPC for atomic batch tracking
        with ``BatchID`` correlation and aggregate ``HaveErrors`` flag.

        Args:
            messages: List of messages to send. Each message must have a
                valid ``channel`` and ``body``.

        Returns:
            QueueBatchSendResult: Contains ``batch_id`` (correlation ID for
            the batch), ``have_errors`` (bool indicating if any message
            failed), and ``results`` (list of per-message
            :class:`QueueSendResult` objects).

        Raises:
            KubeMQValidationError: If any message fails validation (e.g.,
                empty channel, body exceeds ``max_send_size``).
            KubeMQConnectionError: If the server is unreachable or the
                connection is lost.
            KubeMQAuthenticationError: If the auth token is invalid or
                expired, or the client lacks permission for the channel.
            KubeMQTimeoutError: If the operation exceeds the server deadline.
            KubeMQClientClosedError: If the client has already been closed.

        See Also:
            :meth:`send_queue_message`: Send a single queue message.
        """
        self._ensure_connected()
        assert self._transport is not None

        from kubemq.grpc import QueueMessagesBatchRequest

        batch_id = str(uuid.uuid4())
        client_id = self._config.client_id or ""

        batch_request = QueueMessagesBatchRequest()
        batch_request.BatchID = batch_id
        for msg in messages:
            self._validate_message_size(msg.body)
            pb_msg = msg.encode_message(client_id)
            tags_dict = dict(pb_msg.Tags)
            KubeMQTagsCarrier(tags_dict).inject()
            pb_msg.Tags.update(tags_dict)
            batch_request.Messages.append(pb_msg)

        batch_response = self._transport.kubemq_client().SendQueueMessagesBatch(batch_request)

        results: list[QueueSendResult] = []
        for pb_result in batch_response.Results:
            results.append(QueueSendResult.decode(pb_result))

        return QueueBatchSendResult(
            batch_id=batch_response.BatchID,
            results=results,
            have_errors=batch_response.HaveErrors,
        )

    def create_queues_channel(self, channel: str) -> bool | None:
        """Create a queues channel.

        Args:
            channel: The name of the channel to create.

        Returns:
            bool: ``True`` if the channel was created successfully.

        Raises:
            KubeMQConnectionError: If the server is unreachable.
            KubeMQAuthenticationError: If the auth token is invalid or the
                client lacks permission to create channels.
            KubeMQTimeoutError: If the operation exceeds the server deadline.
            KubeMQError: If the server rejects the request (e.g., channel
                already exists or invalid name).
        """
        return create_channel_request(self._transport, self._config.client_id, channel, "queues")

    async def create_queues_channel_async(self, channel: str) -> bool | None:
        """Create a queues channel asynchronously.

        Args:
            channel: The name of the channel to create.

        Returns:
            bool: ``True`` if the channel was created successfully.

        Raises:
            KubeMQConnectionError: If the server is unreachable.
            KubeMQAuthenticationError: If the auth token is invalid or the
                client lacks permission to create channels.
            KubeMQTimeoutError: If the operation exceeds the server deadline.
            KubeMQError: If the server rejects the request.
        """
        return await run_in_thread(self.create_queues_channel, channel)

    def delete_queues_channel(self, channel: str) -> bool | None:
        """Delete a queues channel.

        Args:
            channel: The name of the channel to delete.

        Returns:
            bool: ``True`` if the channel was deleted successfully.

        Raises:
            KubeMQConnectionError: If the server is unreachable.
            KubeMQAuthenticationError: If the auth token is invalid or the
                client lacks permission to delete channels.
            KubeMQChannelError: If the channel does not exist.
            KubeMQTimeoutError: If the operation exceeds the server deadline.
        """
        return delete_channel_request(self._transport, self._config.client_id, channel, "queues")

    async def delete_queues_channel_async(self, channel: str) -> bool | None:
        """Delete a queues channel asynchronously.

        Args:
            channel: The name of the channel to delete.

        Returns:
            bool: ``True`` if the channel was deleted successfully.

        Raises:
            KubeMQConnectionError: If the server is unreachable.
            KubeMQAuthenticationError: If the auth token is invalid or the
                client lacks permission to delete channels.
            KubeMQChannelError: If the channel does not exist.
            KubeMQTimeoutError: If the operation exceeds the server deadline.
        """
        return await run_in_thread(self.delete_queues_channel, channel)

    def list_queues_channels(self, channel_search: str = "") -> list[QueuesChannel]:
        """List queues channels.

        Args:
            channel_search: Optional wildcard filter (e.g., ``"queues.*"``).
                An empty string returns all channels.

        Returns:
            list[QueuesChannel]: Each entry contains the channel ``name``,
            ``type``, ``is_active`` status, and message statistics
            (pending, expired, delivered counts).

        Raises:
            KubeMQConnectionError: If the server is unreachable.
            KubeMQAuthenticationError: If the auth token is invalid or the
                client lacks permission.
            KubeMQTimeoutError: If the operation exceeds the server deadline.
        """
        return list_queues_channels(self._transport, self._config.client_id, channel_search)

    async def list_queues_channels_async(self, channel_search: str = "") -> list[QueuesChannel]:
        """List queues channels asynchronously.

        Args:
            channel_search: Optional wildcard filter (e.g., ``"queues.*"``).
                An empty string returns all channels.

        Returns:
            list[QueuesChannel]: Each entry contains the channel ``name``,
            ``type``, ``is_active`` status, and message statistics.

        Raises:
            KubeMQConnectionError: If the server is unreachable.
            KubeMQAuthenticationError: If the auth token is invalid or the
                client lacks permission.
            KubeMQTimeoutError: If the operation exceeds the server deadline.
        """
        return await run_in_thread(self.list_queues_channels, channel_search)

    def _receive_queue_messages_impl(
        self,
        channel: str | None = None,
        max_messages: int = 1,
        wait_timeout_in_seconds: int = 60,
        auto_ack: bool = False,
        metadata: dict[str, str] | None = None,
    ) -> QueuesPollResponse:
        """Internal implementation for receiving queue messages."""
        if not self._config.client_id:
            raise ValueError("ClientID required for downstream operations")
        if max_messages < 1 or max_messages > 1024:
            raise ValueError("max_messages must be between 1 and 1024")
        if wait_timeout_in_seconds < 0 or wait_timeout_in_seconds > 3600:
            raise ValueError("wait_timeout_in_seconds must be between 0 and 3600")
        ch = channel or ""
        start = time.perf_counter()
        error_type_val = None
        with self._instrumentor.start_span("receive", ch) as span:
            try:
                receiver = self._get_downstream_receiver()
                client_id = self._config.client_id or ""

                request = QueuesDownstreamRequest()
                request.RequestID = str(uuid.uuid4())
                request.ClientID = client_id
                request.Channel = ch
                request.MaxItems = max_messages
                request.WaitTimeout = wait_timeout_in_seconds * 1000
                request.AutoAck = auto_ack
                request.RequestTypeData = QueuesDownstreamRequestType.Get
                if metadata:
                    for k, v in metadata.items():
                        request.Metadata[k] = v
                kubemq_response = receiver.send(request)
                if kubemq_response is None:
                    return QueuesPollResponse()
                response = QueuesPollResponse().decode(
                    response=kubemq_response,
                    receiver_client_id=client_id,
                    response_handler=receiver.send_without_response,  # type: ignore[arg-type]
                )
                if response.messages:
                    for _ in response.messages:
                        self._instrumentor._metrics.record_consumed_message("receive", ch)
                return response
            except Exception as e:
                error_type_val = error_code_to_error_type(getattr(e, "code", None))
                self._instrumentor.record_error(span, e, error_type_val)
                raise
            finally:
                duration = time.perf_counter() - start
                self._instrumentor._metrics.record_operation_duration(
                    duration, "receive", ch, error_type_val
                )

    def receive_queue_messages(
        self,
        channel: str | None = None,
        max_messages: int = 1,
        wait_timeout_in_seconds: int = 60,
        auto_ack: bool = False,
        metadata: dict[str, str] | None = None,
    ) -> QueuesPollResponse:
        """Receive messages from a queue channel.

        Long-polls the server for up to ``wait_timeout_in_seconds``. If
        ``auto_ack`` is ``False``, each message must be explicitly
        acknowledged, rejected, or re-queued.

        Args:
            channel: The name of the channel to receive messages from.
            max_messages: Maximum number of messages to receive (1–1024).
            wait_timeout_in_seconds: Timeout in seconds to wait for messages
                (0–3600).
            auto_ack: Whether to automatically acknowledge messages on
                receipt.
            metadata: Optional key-value metadata to attach to the
                downstream request.

        Returns:
            QueuesPollResponse: Contains ``messages`` (list of received
            queue messages, each supporting ``.ack()``, ``.nack()``,
            and ``.requeue()``), ``is_error`` (bool), ``error`` (error
            description), and ``messages_received`` / ``messages_expired``
            counts.

        Raises:
            ValueError: If ``max_messages`` is not between 1 and 1024 or
                ``wait_timeout_in_seconds`` is not between 0 and 3600.
            KubeMQConnectionError: If the server is unreachable or the
                connection is lost.
            KubeMQAuthenticationError: If the auth token is invalid or
                expired, or the client lacks permission for the channel.
            KubeMQTimeoutError: If the operation exceeds the server deadline.
            KubeMQClientClosedError: If the client has already been closed.

        See Also:
            :meth:`send_queue_message`: Send messages to a queue.
            :meth:`waiting`: Peek at waiting messages without consuming.
            :meth:`pull`: Pull and remove messages from a queue.

        Example:
            >>> from kubemq.queues import Client
            >>> with Client(address="localhost:50000") as client:
            ...     response = client.receive_queue_messages(
            ...         channel="queues.tasks",
            ...         max_messages=10,
            ...         wait_timeout_in_seconds=5,
            ...         auto_ack=False,
            ...     )
            ...     for msg in response.messages:
            ...         print(f"Got: {msg.body}")
            ...         msg.ack()
        """
        return self._receive_queue_messages_impl(
            channel, max_messages, wait_timeout_in_seconds, auto_ack, metadata
        )

    @deprecated(replacement="receive_queue_messages()", since="4.0.0", removal="5.0.0")
    def receive_queues_messages(
        self,
        channel: str | None = None,
        max_messages: int = 1,
        wait_timeout_in_seconds: int = 60,
        auto_ack: bool = False,
    ) -> QueuesPollResponse:
        """Receive messages from a queues channel.

        Deprecated:
            Use ``receive_queue_messages()`` instead. Will be removed in v5.0.

        Args:
            channel: The name of the channel to receive messages from
            max_messages: Maximum number of messages to receive (1–1024)
            wait_timeout_in_seconds: Timeout in seconds to wait for messages
            auto_ack: Whether to automatically acknowledge messages

        Returns:
            QueuesPollResponse: Contains ``messages``, ``is_error``,
            ``error``, and message count fields.

        Raises:
            ValueError: If ``max_messages`` or ``wait_timeout_in_seconds``
                are out of range.
            KubeMQConnectionError: If the server is unreachable.
            KubeMQAuthenticationError: If authentication or authorization
                fails.
            KubeMQClientClosedError: If the client has already been closed.
        """
        return self._receive_queue_messages_impl(
            channel, max_messages, wait_timeout_in_seconds, auto_ack
        )

    async def receive_queues_messages_async(
        self,
        channel: str | None = None,
        max_messages: int = 1,
        wait_timeout_in_seconds: int = 60,
        auto_ack: bool = False,
    ) -> QueuesPollResponse:
        """Receive messages from a queues channel asynchronously.

        Deprecated:
            Use ``AsyncQueuesClient.receive_queue_messages()`` for native async support.

        Args:
            channel: The name of the channel to receive messages from
            max_messages: Maximum number of messages to receive (1–1024)
            wait_timeout_in_seconds: Timeout in seconds to wait for messages
            auto_ack: Whether to automatically acknowledge messages

        Returns:
            QueuesPollResponse: Contains ``messages``, ``is_error``,
            ``error``, and message count fields.

        Raises:
            ValueError: If ``max_messages`` or ``wait_timeout_in_seconds``
                are out of range.
            KubeMQConnectionError: If the server is unreachable.
            KubeMQAuthenticationError: If authentication or authorization
                fails.
            KubeMQClientClosedError: If the client has already been closed.
        """
        return await run_in_thread(
            self._receive_queue_messages_impl,
            channel,
            max_messages,
            wait_timeout_in_seconds,
            auto_ack,
        )

    def peek_queue_messages(
        self, channel: str, max_messages: int, wait_timeout_in_seconds: int
    ) -> QueueMessagesWaiting:
        """Get waiting messages from a queue (peek without removing).

        Retrieves messages that are currently waiting in the queue without
        consuming them. Messages remain available for other consumers.

        Args:
            channel: The name of the queue channel.
            max_messages: Maximum number of messages to retrieve (1–1024).
            wait_timeout_in_seconds: Maximum time to wait for messages in
                seconds (1–3600).

        Returns:
            QueueMessagesWaiting: Contains ``messages`` (list of
            :class:`QueueMessageWaitingPulled` objects), ``is_error``
            (bool), ``error`` (error description), ``messages_received``
            and ``messages_expired`` counts, and ``is_peak`` flag.

        Raises:
            ValueError: If ``channel`` is ``None``, ``max_messages`` is not
                between 1 and 1024, or ``wait_timeout_in_seconds`` is not
                between 1 and 3600.
            KubeMQConnectionError: If the server is unreachable or the
                connection is lost.
            KubeMQAuthenticationError: If the auth token is invalid or the
                client lacks permission for the channel.
            KubeMQTimeoutError: If the operation exceeds the server deadline.
            KubeMQClientClosedError: If the client has already been closed.

        See Also:
            :meth:`pull`: Pull and remove messages from a queue.
            :meth:`receive_queue_messages`: Consume messages with
                acknowledgment support.
        """
        self._logger.debug(f"Get waiting messages from queue: {channel}")
        if channel is None:
            raise ValueError("channel cannot be None.")
        if max_messages < 1 or max_messages > 1024:
            raise ValueError("max_messages must be between 1 and 1024.")
        if wait_timeout_in_seconds < 1 or wait_timeout_in_seconds > 3600:
            raise ValueError("wait_timeout_in_seconds must be between 1 and 3600.")

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
        waiting_messages = QueueMessagesWaiting(
            is_error=response.IsError,
            error=response.Error,
            messages_received=getattr(response, "MessagesReceived", 0),
            messages_expired=getattr(response, "MessagesExpired", 0),
            is_peak=getattr(response, "IsPeak", True),
        )

        if not response.Messages:
            return waiting_messages

        self._logger.debug(f"Waiting messages count: {len(response.Messages)}")
        for message in response.Messages:
            waiting_messages.messages.append(QueueMessageWaitingPulled.decode(message, client_id))

        return waiting_messages

    async def peek_queue_messages_async(
        self, channel: str, max_messages: int, wait_timeout_in_seconds: int
    ) -> QueueMessagesWaiting:
        """Get waiting messages from a queue asynchronously (peek without removing).

        Args:
            channel: The name of the queue channel.
            max_messages: Maximum number of messages to retrieve (1–1024).
            wait_timeout_in_seconds: Maximum time to wait for messages in
                seconds (1–3600).

        Returns:
            QueueMessagesWaiting: Contains ``messages``, ``is_error``,
            ``error``, and message count fields.

        Raises:
            ValueError: If ``channel`` is ``None``, ``max_messages`` or
                ``wait_timeout_in_seconds`` are out of range.
            KubeMQConnectionError: If the server is unreachable.
            KubeMQAuthenticationError: If authentication or authorization
                fails.
            KubeMQTimeoutError: If the operation exceeds the server deadline.
            KubeMQClientClosedError: If the client has already been closed.
        """
        return await run_in_thread(self.peek_queue_messages, channel, max_messages, wait_timeout_in_seconds)

    def ack_all_queue_messages(self, channel: str, wait_time_seconds: int = 60) -> int:
        """Acknowledge all messages in a queue.

        Args:
            channel: Queue channel to ack all messages.
            wait_time_seconds: How long the server should wait for messages
                to ack (in seconds).

        Returns:
            int: The number of messages that were acknowledged.

        Raises:
            KubeMQMessageError: If the server returns an error response
                (e.g., no messages to acknowledge).
            KubeMQConnectionError: If the server is unreachable or the
                connection is lost.
            KubeMQAuthenticationError: If the auth token is invalid or the
                client lacks permission for the channel.
            KubeMQTimeoutError: If the operation exceeds the server deadline.
            KubeMQClientClosedError: If the client has already been closed.
        """
        self._ensure_connected()
        assert self._transport is not None
        from kubemq.core.exceptions import KubeMQMessageError
        from kubemq.grpc import AckAllQueueMessagesRequest

        request = AckAllQueueMessagesRequest()
        request.RequestID = str(uuid.uuid4())
        request.ClientID = self._config.client_id or ""
        request.Channel = channel
        request.WaitTimeSeconds = wait_time_seconds

        response = self._transport.kubemq_client().AckAllQueueMessages(request)

        if response.IsError:
            raise KubeMQMessageError(
                response.Error,
                operation="AckAllQueueMessages",
                channel=channel,
            )

        return int(response.AffectedMessages)

    def pull(
        self, channel: str, max_messages: int, wait_timeout_in_seconds: int
    ) -> QueueMessagesPulled:
        """Pull messages from a queue (retrieve and remove).

        Unlike :meth:`waiting`, this method consumes messages — they are
        removed from the queue and will not be delivered to other consumers.

        Args:
            channel: The name of the queue channel.
            max_messages: Maximum number of messages to pull (1–1024).
            wait_timeout_in_seconds: Maximum time to wait for messages in
                seconds (1–3600).

        Returns:
            QueueMessagesPulled: Contains ``messages`` (list of
            :class:`QueueMessageWaitingPulled` objects), ``is_error``
            (bool), ``error`` (error description), ``messages_received``
            and ``messages_expired`` counts.

        Raises:
            ValueError: If ``channel`` is ``None``, ``max_messages`` is not
                between 1 and 1024, or ``wait_timeout_in_seconds`` is not
                between 1 and 3600.
            KubeMQConnectionError: If the server is unreachable or the
                connection is lost.
            KubeMQAuthenticationError: If the auth token is invalid or the
                client lacks permission for the channel.
            KubeMQTimeoutError: If the operation exceeds the server deadline.
            KubeMQClientClosedError: If the client has already been closed.

        See Also:
            :meth:`waiting`: Peek at messages without removing them.
            :meth:`receive_queue_messages`: Consume messages with
                acknowledgment support.
        """
        self._logger.debug(f"Pulling messages from queue: {channel}")
        if channel is None:
            raise ValueError("channel cannot be None.")
        if max_messages < 1 or max_messages > 1024:
            raise ValueError("max_messages must be between 1 and 1024.")
        if wait_timeout_in_seconds < 1 or wait_timeout_in_seconds > 3600:
            raise ValueError("wait_timeout_in_seconds must be between 1 and 3600.")

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
        pulled_messages = QueueMessagesPulled(
            is_error=response.IsError,
            error=response.Error,
            messages_received=getattr(response, "MessagesReceived", 0),
            messages_expired=getattr(response, "MessagesExpired", 0),
            is_peak=getattr(response, "IsPeak", False),
        )

        if not response.Messages:
            return pulled_messages

        self._logger.debug(f"Pulled messages count: {len(response.Messages)}")
        for message in response.Messages:
            pulled_messages.messages.append(QueueMessageWaitingPulled.decode(message, client_id))

        return pulled_messages

    async def pull_async(
        self, channel: str, max_messages: int, wait_timeout_in_seconds: int
    ) -> QueueMessagesPulled:
        """Pull messages from a queue asynchronously (retrieve and remove).

        Args:
            channel: The name of the queue channel.
            max_messages: Maximum number of messages to pull (1–1024).
            wait_timeout_in_seconds: Maximum time to wait for messages in
                seconds (1–3600).

        Returns:
            QueueMessagesPulled: Contains ``messages``, ``is_error``,
            ``error``, and message count fields.

        Raises:
            ValueError: If ``channel`` is ``None``, ``max_messages`` or
                ``wait_timeout_in_seconds`` are out of range.
            KubeMQConnectionError: If the server is unreachable.
            KubeMQAuthenticationError: If authentication or authorization
                fails.
            KubeMQTimeoutError: If the operation exceeds the server deadline.
            KubeMQClientClosedError: If the client has already been closed.
        """
        return await run_in_thread(self.pull, channel, max_messages, wait_timeout_in_seconds)
