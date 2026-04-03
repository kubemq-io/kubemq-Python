from __future__ import annotations

import threading
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from kubemq.grpc import (
    QueueMessage as pbQueueMessage,
    QueuesDownstreamRequest,
    QueuesDownstreamRequestType,
    QueuesDownstreamResponse,
)


@dataclass
class QueueMessageReceived:
    """Represents a message received from a KubeMQ queue.

    This class encapsulates a message received from a KubeMQ queue and provides
    methods for acknowledging, negatively acknowledging, and re-queuing the message.

    Thread Safety:
        - All operations that modify the message state are thread-safe
        - The class uses locks to ensure thread safety

    Attributes:
        id: The unique identifier of the message.
        channel: The channel the message was received on.
        metadata: Additional metadata associated with the message.
        body: The message payload.
        from_client_id: The client ID of the sender.
        tags: Additional tags associated with the message.
        timestamp: When the message was sent.
        sequence: The sequence number of the message.
        receive_count: How many times the message has been received.
        is_re_routed: Whether the message was re-routed.
        re_route_from_queue: The original queue if re-routed.
        delayed_to: When the message should be delivered.
        expired_at: When the message expires.
        transaction_id: The ID of the transaction.
        receiver_client_id: The client ID of the receiver.
        is_transaction_completed: Whether the transaction is completed.
        is_auto_acked: Whether the message is automatically acknowledged.

    See Also:
        QueueMessage: The message model for sending to a queue.
        QueuesClient.receive_queue_messages: Receive messages from a queue.
    """

    id: str = ""
    channel: str = ""
    metadata: str = ""
    body: bytes = b""
    from_client_id: str = ""
    tags: dict[str, str] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=lambda: datetime.fromtimestamp(0))
    sequence: int = 0
    receive_count: int = 0
    is_re_routed: bool = False
    re_route_from_queue: str = ""
    expired_at: datetime = field(default_factory=lambda: datetime.fromtimestamp(0))
    delayed_to: datetime = field(default_factory=lambda: datetime.fromtimestamp(0))
    transaction_id: str = ""
    is_transaction_completed: bool = False
    receiver_client_id: str = ""
    response_handler: Callable[[QueuesDownstreamRequest], QueuesDownstreamResponse] | None = None
    async_response_handler: Callable[[QueuesDownstreamRequest], Any] | None = None
    is_auto_acked: bool = False
    md5_of_body: str = ""
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    _message_completed: bool = field(default=False, init=False, repr=False)

    # Public API methods

    def ack(self) -> None:
        """Acknowledge the message, indicating successful processing.

        This method sends an acknowledgment to the server, indicating that
        the message has been successfully processed and can be removed from the queue.

        Raises:
            ValueError: If the message is auto-acknowledged, the transaction is already
                       completed, or the response handler is not set.
        """
        self._do_operation(QueuesDownstreamRequestType.AckRange)

    def nack(self) -> None:
        """Negatively acknowledge the message, indicating unsuccessful processing.

        This method sends a negative acknowledgment to the server, indicating
        that the message could not be processed and should be handled according
        to the server's configuration.

        Raises:
            ValueError: If the message is auto-acknowledged, the transaction is already
                       completed, or the response handler is not set.
        """
        self._do_operation(QueuesDownstreamRequestType.NAckRange)

    def re_queue(self, channel: str) -> None:
        """Re-queue the message to another channel.

        This method sends a request to re-queue the message to another channel.

        Args:
            channel: The channel to re-queue the message to.

        Raises:
            ValueError: If the message is auto-acknowledged, the transaction is already
                       completed, or the response handler is not set.
        """
        self._do_operation(QueuesDownstreamRequestType.ReQueueRange, channel)

    async def async_ack(self) -> None:
        """Acknowledge the message asynchronously (for native async clients)."""
        await self._do_async_operation(QueuesDownstreamRequestType.AckRange)

    async def async_nack(self) -> None:
        """Negatively acknowledge the message asynchronously (for native async clients)."""
        await self._do_async_operation(QueuesDownstreamRequestType.NAckRange)

    async def async_re_queue(self, channel: str) -> None:
        """Re-queue the message to another channel asynchronously (for native async clients)."""
        await self._do_async_operation(QueuesDownstreamRequestType.ReQueueRange, channel)

    # Properties

    @property
    def is_completed(self) -> bool:
        """Thread-safe check if the message transaction is completed.

        Returns:
            bool: True if the message transaction is completed, False otherwise.
        """
        with self._lock:
            return self._message_completed

    def get_message_age(self) -> float:
        """Get the age of the message in seconds.

        Returns:
            float: The age of the message in seconds.
        """
        return (datetime.now() - self.timestamp).total_seconds()

    def is_expired(self) -> bool:
        """Check if the message has expired.

        Returns:
            bool: True if the message has expired, False otherwise.
        """
        return self.expired_at > datetime.fromtimestamp(0) and datetime.now() > self.expired_at

    # Class methods

    @classmethod
    def decode(
        cls,
        message: pbQueueMessage,
        transaction_id: str,
        transaction_is_completed: bool = False,
        receiver_client_id: str = "",
        response_handler: Callable[[QueuesDownstreamRequest], QueuesDownstreamResponse]
        | None = None,
        is_auto_acked: bool = False,
        async_response_handler: Callable[[QueuesDownstreamRequest], Any] | None = None,
    ) -> "QueueMessageReceived":
        """Decode a protobuf message into a QueueMessageReceived instance.

        Args:
            message: The protobuf message to decode.
            transaction_id: The ID of the transaction.
            transaction_is_completed: Whether the transaction is completed.
            receiver_client_id: The client ID of the receiver.
            response_handler: The handler for sending responses (sync clients).
            is_auto_acked: Whether the message is automatically acknowledged.
            async_response_handler: Async handler for sending responses (native async clients).

        Returns:
            QueueMessageReceived: The decoded message.
        """
        return cls(
            id=message.MessageID,
            channel=message.Channel,
            metadata=message.Metadata,
            body=message.Body,
            from_client_id=message.ClientID,
            tags={tag: message.Tags[tag] for tag in message.Tags},
            timestamp=(
                datetime.fromtimestamp(message.Attributes.Timestamp / 1e9)
                if message.Attributes
                else datetime.fromtimestamp(0)
            ),
            sequence=message.Attributes.Sequence if message.Attributes else 0,
            receive_count=message.Attributes.ReceiveCount if message.Attributes else 0,
            is_re_routed=message.Attributes.ReRouted if message.Attributes else False,
            re_route_from_queue=(
                message.Attributes.ReRoutedFromQueue if message.Attributes else ""
            ),
            expired_at=(
                datetime.fromtimestamp(message.Attributes.ExpirationAt / 1e9)
                if message.Attributes
                else datetime.fromtimestamp(0)
            ),
            delayed_to=(
                datetime.fromtimestamp(message.Attributes.DelayedTo / 1e9)
                if message.Attributes
                else datetime.fromtimestamp(0)
            ),
            transaction_id=transaction_id,
            is_transaction_completed=transaction_is_completed,
            receiver_client_id=receiver_client_id,
            response_handler=response_handler,
            async_response_handler=async_response_handler,
            is_auto_acked=is_auto_acked,
            md5_of_body=(
                message.Attributes.MD5OfBody
                if message.Attributes and hasattr(message.Attributes, "MD5OfBody")
                else ""
            ),
        )

    # Internal helper methods

    def _do_operation(
        self, request_type: QueuesDownstreamRequestType, re_queue_channel: str = ""
    ) -> None:
        """Perform an operation on the message.

        Args:
            request_type: The type of operation to perform.
            re_queue_channel: The channel to re-queue the message to (if applicable).

        Raises:
            ValueError: If the message is auto-acknowledged, the transaction is already
                       completed, the message transaction is already completed, or
                       the response handler is not set.

        Thread Safety:
            This method is thread-safe.
        """
        with self._lock:
            if self.is_auto_acked:
                raise ValueError(
                    "transaction was set with auto ack, message operations are not allowed"
                )
            if self.is_transaction_completed:
                raise ValueError("transaction is already completed")
            if self._message_completed:
                raise ValueError("message transaction is already completed")
            if not self.response_handler:
                raise ValueError("response_handler is not set")

            request = QueuesDownstreamRequest()
            request.RequestID = str(uuid.uuid4())
            request.ClientID = self.receiver_client_id
            request.Channel = self.channel
            request.RequestTypeData = request_type
            request.RefTransactionId = self.transaction_id
            request.SequenceRange.append(self.sequence)
            request.ReQueueChannel = re_queue_channel
            self._message_completed = True
            self.response_handler(request)

    async def _do_async_operation(
        self, request_type: QueuesDownstreamRequestType, re_queue_channel: str = ""
    ) -> None:
        """Perform an async operation on the message.

        Used by native async clients that provide an async_response_handler.
        """
        if self.is_auto_acked:
            raise ValueError(
                "transaction was set with auto ack, message operations are not allowed"
            )
        if self.is_transaction_completed:
            raise ValueError("transaction is already completed")
        if self._message_completed:
            raise ValueError("message transaction is already completed")
        if not self.async_response_handler:
            raise ValueError("async_response_handler is not set")

        request = QueuesDownstreamRequest()
        request.RequestID = str(uuid.uuid4())
        request.ClientID = self.receiver_client_id
        request.Channel = self.channel
        request.RequestTypeData = request_type
        request.RefTransactionId = self.transaction_id
        request.SequenceRange.append(self.sequence)
        request.ReQueueChannel = re_queue_channel
        self._message_completed = True
        await self.async_response_handler(request)

    def _mark_transaction_completed(self) -> None:
        """Mark the transaction as completed.

        Thread Safety:
            This method is thread-safe.
        """
        with self._lock:
            self._message_completed = True
            self.is_transaction_completed = True

    def __str__(self) -> str:
        """Get a string representation of the message.

        Returns:
            str: A string representation of the message.
        """
        return (
            f"QueueMessageReceived: id={self.id}, channel={self.channel}, metadata={self.metadata}, "
            f"from_client_id={self.from_client_id}, tags={self.tags}, timestamp={self.timestamp}, "
            f"sequence={self.sequence}, receive_count={self.receive_count}, is_re_routed={self.is_re_routed}, "
            f"re_route_from_queue={self.re_route_from_queue}, expired_at={self.expired_at}, "
            f"delayed_to={self.delayed_to}, transaction_id={self.transaction_id}, "
            f"is_transaction_completed={self.is_transaction_completed}, receiver_client_id={self.receiver_client_id}, "
            f"is_auto_acked={self.is_auto_acked}"
        )
