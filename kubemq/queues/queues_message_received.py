from pydantic import BaseModel, Field
from typing import Dict, Callable, Optional
from datetime import datetime
import logging
import time
import uuid
import threading

from kubemq.grpc import QueueMessage as pbQueueMessage
from kubemq.grpc import (
    QueuesDownstreamRequest,
    QueuesDownstreamRequestType,
    QueuesDownstreamResponse,
)


class QueueMessageReceived(BaseModel):
    """
    Represents a message received from a KubeMQ queue.
    
    This class encapsulates a message received from a KubeMQ queue and provides
    methods for acknowledging, rejecting, and re-queuing the message. It also
    manages message visibility timeouts.
    
    Thread Safety:
        - All operations that modify the message state are thread-safe
        - The class uses locks to ensure thread safety
        
    Visibility Timeout:
        - If a visibility timeout is specified, the message will be automatically
          rejected after the timeout expires
        - The visibility timeout can be extended using the extend_visibility_timer method
    
    Attributes:
        id: The unique identifier of the message
        channel: The channel the message was received on
        metadata: Additional metadata associated with the message
        body: The message payload
        from_client_id: The client ID of the sender
        tags: Additional tags associated with the message
        timestamp: When the message was sent
        sequence: The sequence number of the message
        receive_count: How many times the message has been received
        is_re_routed: Whether the message was re-routed
        re_route_from_queue: The original queue if re-routed
        expired_at: When the message expires
        delayed_to: When the message should be delivered
        transaction_id: The ID of the transaction
        is_transaction_completed: Whether the transaction is completed
        receiver_client_id: The client ID of the receiver
        visibility_seconds: How long the message is visible
        is_auto_acked: Whether the message is automatically acknowledged
    """
    
    class Config:
        arbitrary_types_allowed = True
    
    id: str = Field(default="")
    channel: str = Field(default="")
    metadata: str = Field(default="")
    body: bytes = Field(default=b"")
    from_client_id: str = Field(default="")
    tags: Dict[str, str] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=lambda: datetime.fromtimestamp(0))
    sequence: int = Field(default=0)
    receive_count: int = Field(default=0)
    is_re_routed: bool = Field(default=False)
    re_route_from_queue: str = Field(default="")
    expired_at: datetime = Field(default_factory=lambda: datetime.fromtimestamp(0))
    delayed_to: datetime = Field(default_factory=lambda: datetime.fromtimestamp(0))
    transaction_id: str = Field(default="")
    is_transaction_completed: bool = Field(default=False)
    receiver_client_id: str = Field(default="")
    response_handler: Optional[
        Callable[[QueuesDownstreamRequest], QueuesDownstreamResponse]
    ] = Field(default=None)
    visibility_seconds: int = Field(default=0)
    is_auto_acked: bool = Field(default=False)
    _visibility_timer: Optional[threading.Timer] = None
    _message_completed: bool = False
    _timer_expired: bool = False

    def __init__(self, **data):
        super().__init__(**data)
        self._lock = threading.Lock()

    # Public API methods
    
    def ack(self):
        """
        Acknowledge the message, indicating successful processing.
        
        This method sends an acknowledgment to the server, indicating that
        the message has been successfully processed and can be removed from the queue.
        
        Raises:
            ValueError: If the message is auto-acknowledged, the transaction is already
                       completed, or the response handler is not set.
        """
        self._do_operation(QueuesDownstreamRequestType.AckRange)

    def reject(self):
        """
        Reject the message, indicating unsuccessful processing.
        
        This method sends a rejection to the server, indicating that
        the message could not be processed and should be handled according
        to the server's configuration.
        
        Raises:
            ValueError: If the message is auto-acknowledged, the transaction is already
                       completed, or the response handler is not set.
        """
        self._do_operation(QueuesDownstreamRequestType.NAckRange)

    def re_queue(self, channel: str):
        """
        Re-queue the message to another channel.
        
        This method sends a request to re-queue the message to another channel.
        
        Args:
            channel: The channel to re-queue the message to.
            
        Raises:
            ValueError: If the message is auto-acknowledged, the transaction is already
                       completed, or the response handler is not set.
        """
        self._do_operation(QueuesDownstreamRequestType.ReQueueRange, channel)
        
    def extend_visibility_timer(self, additional_seconds: int):
        """
        Extend the visibility timeout of the message.
        
        Args:
            additional_seconds: The number of seconds to extend the visibility timeout by.
            
        Raises:
            ValueError: If additional_seconds is not positive, the message visibility
                       was not set, the visibility has expired, or the message
                       transaction is already completed.
                       
        Thread Safety:
            This method is thread-safe.
        """
        if additional_seconds <= 0:
            raise ValueError("additional_seconds must be greater than 0")
        
        with self._lock:
            if not self._visibility_timer:
                raise ValueError("message visibility was not set for this transaction")
            if self._timer_expired:
                raise ValueError("message visibility expired, cannot perform operation")
            if self._message_completed:
                raise ValueError("message transaction is already completed")
                
            # Calculate elapsed time since timer started
            elapsed_time = time.time() - getattr(self._visibility_timer, "_start_time", time.time())
            # Calculate remaining time
            remaining_time = max(0, self._visibility_timer.interval - elapsed_time)
            new_duration = remaining_time + additional_seconds
            
            self._visibility_timer.cancel()
            self._visibility_timer = threading.Timer(
                new_duration, self._on_visibility_expired
            )
            self._visibility_timer._start_time = time.time()
            self._visibility_timer.start()

    # Properties
    
    @property
    def is_completed(self) -> bool:
        """
        Thread-safe check if the message transaction is completed.
        
        Returns:
            bool: True if the message transaction is completed, False otherwise.
        """
        with self._lock:
            return self._message_completed

    @property
    def is_visible(self) -> bool:
        """
        Thread-safe check if the message is still visible.
        
        Returns:
            bool: True if the message is still visible, False otherwise.
        """
        with self._lock:
            return not self._timer_expired and not self._message_completed
            
    def get_remaining_visibility_seconds(self) -> float:
        """
        Get the remaining visibility time in seconds.
        
        Returns:
            float: The remaining visibility time in seconds, or 0 if the message
                   is not visible or no visibility timeout was set.
                   
        Thread Safety:
            This method is thread-safe.
        """
        with self._lock:
            if not self._visibility_timer or self._timer_expired or self._message_completed:
                return 0
            
            elapsed_time = time.time() - getattr(self._visibility_timer, "_start_time", time.time())
            remaining_time = max(0, self._visibility_timer.interval - elapsed_time)
            return remaining_time
            
    def get_message_age(self) -> float:
        """
        Get the age of the message in seconds.
        
        Returns:
            float: The age of the message in seconds.
        """
        return (datetime.now() - self.timestamp).total_seconds()

    def is_expired(self) -> bool:
        """
        Check if the message has expired.
        
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
        transaction_is_completed: bool,
        receiver_client_id: str,
        response_handler: Callable[[QueuesDownstreamRequest], QueuesDownstreamResponse],
        visibility_seconds: int,
        is_auto_acked: bool,
    ) -> "QueueMessageReceived":
        """
        Decode a protobuf message into a QueueMessageReceived instance.
        
        Args:
            message: The protobuf message to decode.
            transaction_id: The ID of the transaction.
            transaction_is_completed: Whether the transaction is completed.
            receiver_client_id: The client ID of the receiver.
            response_handler: The handler for sending responses.
            visibility_seconds: How long the message is visible.
            is_auto_acked: Whether the message is automatically acknowledged.
            
        Returns:
            QueueMessageReceived: The decoded message.
        """
        instance = cls(
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
                datetime.fromtimestamp(message.Attributes.ExpirationAt / 1e6)
                if message.Attributes
                else datetime.fromtimestamp(0)
            ),
            delayed_to=(
                datetime.fromtimestamp(message.Attributes.DelayedTo / 1e6)
                if message.Attributes
                else datetime.fromtimestamp(0)
            ),
            transaction_id=transaction_id,
            is_transaction_completed=transaction_is_completed,
            receiver_client_id=receiver_client_id,
            response_handler=response_handler,
            visibility_seconds=visibility_seconds,
            is_auto_acked=is_auto_acked,
        )

        if instance.visibility_seconds > 0:
            instance._start_visibility_timer()

        return instance

    # Internal helper methods
    
    def _do_operation(
        self, request_type: QueuesDownstreamRequestType, re_queue_channel: str = ""
    ):
        """
        Perform an operation on the message.
        
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
            self._cancel_visibility_timer()
            self.response_handler(request)

    def _mark_transaction_completed(self):
        """
        Mark the transaction as completed.
        
        Thread Safety:
            This method is thread-safe.
        """
        with self._lock:
            self._message_completed = True
            self.is_transaction_completed = True
            self._cancel_visibility_timer()
            
    # Visibility timer methods
    
    def _start_visibility_timer(self):
        """
        Start the visibility timer.
        
        Thread Safety:
            This method is thread-safe.
        """
        with self._lock:
            self._visibility_timer = threading.Timer(
                self.visibility_seconds, self._on_visibility_expired
            )
            self._visibility_timer._start_time = time.time()
            self._visibility_timer.start()
            
    def _cancel_visibility_timer(self):
        """
        Cancel the visibility timer if it exists.
        
        Thread Safety:
            This method is thread-safe.
        """
        if self._visibility_timer and not self._timer_expired:
            self._visibility_timer.cancel()
            self._visibility_timer = None

    def _on_visibility_expired(self):
        """
        Handle visibility timer expiration.
        
        This method is called when the visibility timer expires.
        It marks the message as expired and rejects it.
        
        Thread Safety:
            This method is thread-safe.
        """
        with self._lock:
            self._timer_expired = True
            self._visibility_timer = None
        
        try:
            self.reject()
        except Exception as e:
            # Log the error instead of raising it
            logging.error(f"Error rejecting message after visibility timeout: {e}")


    def __str__(self) -> str:
        """
        Get a string representation of the message.
        
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
            f"visibility_seconds={self.visibility_seconds}, is_auto_acked={self.is_auto_acked}"
        )
