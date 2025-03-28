from pydantic import BaseModel, Field, field_validator
from typing import Dict, List, Optional, Self, ClassVar
from datetime import datetime
import time
from kubemq.grpc import QueueMessage as pbQueueMessage


class QueueMessageWaitingPulled(BaseModel):
    """
    Represents a message that is waiting in a queue or has been pulled from a queue.
    
    This class encapsulates all the properties of a message that has been received
    from a KubeMQ queue, either by peeking at the queue (waiting) or by pulling
    the message from the queue.
    
    Attributes:
        id: The unique identifier of the message.
        channel: The channel (queue name) where the message was received from.
        metadata: Metadata associated with the message.
        body: The binary payload of the message.
        from_client_id: The client ID of the sender.
        tags: Key-value pairs for additional message metadata.
        timestamp: When the message was sent.
        sequence: The sequence number of the message.
        receive_count: How many times the message has been received.
        is_re_routed: Whether the message was re-routed.
        re_route_from_queue: The original queue if re-routed.
        expired_at: When the message expires.
        delayed_to: When the message should be delivered.
        receiver_client_id: The client ID of the receiver.
        
    Examples:
        ```python
        # Decode a message from a protobuf message
        message = QueueMessageWaitingPulled.decode(pb_message, "client-1")
        
        # Check if the message is expired
        if message.is_expired():
            print("Message has expired")
            
        # Check if the message is delayed
        if message.is_delayed():
            print(f"Message will be available in {message.get_delay_seconds()} seconds")
        ```
    """
    
    # Pydantic configuration
    class Config:
        arbitrary_types_allowed = True
        frozen = True  # Make instances immutable
    
    # Class attributes
    EPOCH: ClassVar[datetime] = datetime.fromtimestamp(0)
    
    # Instance attributes
    id: str = Field(default="", description="The unique identifier of the message")
    channel: str = Field(default="", description="The channel (queue name) where the message was received from")
    metadata: str = Field(default="", description="Metadata associated with the message")
    body: bytes = Field(default=b"", description="The binary payload of the message")
    from_client_id: str = Field(default="", description="The client ID of the sender")
    tags: Dict[str, str] = Field(default_factory=dict, description="Key-value pairs for additional message metadata")
    timestamp: datetime = Field(default_factory=lambda: datetime.fromtimestamp(0), description="When the message was sent")
    sequence: int = Field(default=0, description="The sequence number of the message")
    receive_count: int = Field(default=0, description="How many times the message has been received")
    is_re_routed: bool = Field(default=False, description="Whether the message was re-routed")
    re_route_from_queue: str = Field(default="", description="The original queue if re-routed")
    expired_at: datetime = Field(default_factory=lambda: datetime.fromtimestamp(0), description="When the message expires")
    delayed_to: datetime = Field(default_factory=lambda: datetime.fromtimestamp(0), description="When the message should be delivered")
    receiver_client_id: str = Field(default="", description="The client ID of the receiver")
    
    # Validators
    @field_validator("channel")
    def channel_must_not_be_empty(cls, v: str) -> str:
        """
        Validate that the channel is not empty.
        
        Args:
            v: The channel value to validate
            
        Returns:
            The validated channel value
            
        Raises:
            ValueError: If the channel is empty
        """
        if not v:
            raise ValueError("Channel cannot be empty. Please provide a valid queue name.")
        return v

    # Utility methods
    def is_expired(self) -> bool:
        """
        Check if the message has expired.
        
        Returns:
            bool: True if the message has expired, False otherwise.
        """
        return self.expired_at > self.EPOCH and datetime.now() > self.expired_at
    
    def is_delayed(self) -> bool:
        """
        Check if the message is delayed.
        
        Returns:
            bool: True if the message is delayed, False otherwise.
        """
        return self.delayed_to > self.EPOCH and datetime.now() < self.delayed_to
    
    def get_delay_seconds(self) -> float:
        """
        Get the number of seconds until the message is no longer delayed.
        
        Returns:
            float: The number of seconds until the message is available, or 0 if not delayed.
        """
        if not self.is_delayed():
            return 0
        
        return max(0, (self.delayed_to - datetime.now()).total_seconds())
    
    def get_age_seconds(self) -> float:
        """
        Get the age of the message in seconds.
        
        Returns:
            float: The age of the message in seconds.
        """
        if self.timestamp == self.EPOCH:
            return 0
            
        return (datetime.now() - self.timestamp).total_seconds()
    
    def with_updates(self, **kwargs) -> Self:
        """
        Create a new message with updated values.
        
        Since instances are immutable, this method creates a new
        instance with the specified updates.
        
        Args:
            **kwargs: The fields to update and their new values
            
        Returns:
            A new instance with the updated values
        """
        data = self.model_dump()
        data.update(kwargs)
        return self.__class__(**data)
    
    # Encoding/decoding methods
    def encode(self) -> pbQueueMessage:
        """
        Encode the message to a protobuf QueueMessage.
        
        Returns:
            A protobuf QueueMessage containing the encoded message
        """
        pb_queue = pbQueueMessage()
        pb_queue.MessageID = self.id
        pb_queue.ClientID = self.from_client_id
        pb_queue.Channel = self.channel
        pb_queue.Metadata = self.metadata
        pb_queue.Body = self.body
        pb_queue.Tags.update(self.tags)
        
        # Set attributes if they have non-default values
        if (self.timestamp != self.EPOCH or self.sequence != 0 or 
            self.receive_count != 0 or self.is_re_routed or 
            self.re_route_from_queue or self.expired_at != self.EPOCH or 
            self.delayed_to != self.EPOCH):
            
            if self.timestamp != self.EPOCH:
                pb_queue.Attributes.Timestamp = int(self.timestamp.timestamp() * 1e9)
            
            pb_queue.Attributes.Sequence = self.sequence
            pb_queue.Attributes.ReceiveCount = self.receive_count
            pb_queue.Attributes.ReRouted = self.is_re_routed
            pb_queue.Attributes.ReRoutedFromQueue = self.re_route_from_queue
            
            if self.expired_at != self.EPOCH:
                pb_queue.Attributes.ExpirationAt = int(self.expired_at.timestamp() * 1e6)
            
            if self.delayed_to != self.EPOCH:
                pb_queue.Attributes.DelayedTo = int(self.delayed_to.timestamp() * 1e6)
        
        return pb_queue
    
    @classmethod
    def decode(
        cls,
        message: pbQueueMessage,
        receiver_client_id: str,
    ) -> Self:
        """
        Create a QueueMessageWaitingPulled from a protobuf QueueMessage.
        
        Args:
            message: The protobuf QueueMessage to decode
            receiver_client_id: The client ID of the receiver
            
        Returns:
            A new QueueMessageWaitingPulled instance
            
        Raises:
            ValueError: If the message is invalid
        """
        if not message:
            raise ValueError("Cannot decode None message")
            
        if not message.Channel:
            raise ValueError("Message must have a channel")
            
        try:
            tags = {key: message.Tags[key] for key in message.Tags}
            
            return cls(
                id=message.MessageID,
                channel=message.Channel,
                metadata=message.Metadata,
                body=message.Body,
                from_client_id=message.ClientID,
                tags=tags,
                timestamp=(
                    datetime.fromtimestamp(message.Attributes.Timestamp / 1e9)
                    if message.Attributes and message.Attributes.Timestamp
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
                    if message.Attributes and message.Attributes.ExpirationAt
                    else datetime.fromtimestamp(0)
                ),
                delayed_to=(
                    datetime.fromtimestamp(message.Attributes.DelayedTo / 1e6)
                    if message.Attributes and message.Attributes.DelayedTo
                    else datetime.fromtimestamp(0)
                ),
                receiver_client_id=receiver_client_id,
            )
        except Exception as e:
            raise ValueError(f"Failed to decode message: {str(e)}")

    def __str__(self) -> str:
        """
        Get a string representation of the message.
        
        Returns:
            A string representation of the message
        """
        try:
            body_preview = self.body[:20].decode('utf-8', errors='replace') if self.body else ""
            if len(self.body) > 20:
                body_preview += "..."
                
            return (
                f"QueueMessageWaitingPulled: id={self.id}, channel={self.channel}, "
                f"metadata={self.metadata}, body_preview='{body_preview}', "
                f"from_client_id={self.from_client_id}, timestamp={self.timestamp}, "
                f"sequence={self.sequence}, receive_count={self.receive_count}, "
                f"expired_at={self.expired_at}, delayed_to={self.delayed_to}"
            )
        except Exception as e:
            return f"QueueMessageWaitingPulled: id={self.id}, channel={self.channel}, [Error displaying message: {str(e)}]"
    
    def __repr__(self) -> str:
        """
        Get a detailed representation of the message.
        
        Returns:
            A detailed representation of the message
        """
        return (
            f"QueueMessageWaitingPulled(id={self.id!r}, channel={self.channel!r}, "
            f"metadata={self.metadata!r}, body={self.body!r}, "
            f"from_client_id={self.from_client_id!r}, tags={self.tags!r}, "
            f"timestamp={self.timestamp!r}, sequence={self.sequence}, "
            f"receive_count={self.receive_count}, is_re_routed={self.is_re_routed}, "
            f"re_route_from_queue={self.re_route_from_queue!r}, "
            f"expired_at={self.expired_at!r}, delayed_to={self.delayed_to!r}, "
            f"receiver_client_id={self.receiver_client_id!r})"
        )


class QueueMessagesBase(BaseModel):
    """
    Base class for collections of queue messages.
    
    This class serves as a base for both waiting and pulled message collections.
    It provides common functionality for working with collections of messages.
    
    Attributes:
        messages: The list of messages in the collection.
        is_error: Whether an error occurred when retrieving the messages.
        error: The error message if an error occurred.
    """
    
    class Config:
        frozen = True  # Make instances immutable
    
    messages: List[QueueMessageWaitingPulled] = Field(
        default_factory=list, 
        description="The list of messages in the collection"
    )
    is_error: bool = Field(
        default=False, 
        description="Whether an error occurred when retrieving the messages"
    )
    error: Optional[str] = Field(
        default=None, 
        description="The error message if an error occurred"
    )

    def get_messages(self) -> List[QueueMessageWaitingPulled]:
        """
        Get the list of messages in the collection.
        
        Returns:
            The list of messages in the collection.
        """
        return self.messages
    
    def count(self) -> int:
        """
        Get the number of messages in the collection.
        
        Returns:
            The number of messages in the collection.
        """
        return len(self.messages)
    
    def is_empty(self) -> bool:
        """
        Check if the collection is empty.
        
        Returns:
            True if the collection is empty, False otherwise.
        """
        return len(self.messages) == 0
    
    def with_updates(self, **kwargs) -> Self:
        """
        Create a new collection with updated values.
        
        Since instances are immutable, this method creates a new
        instance with the specified updates.
        
        Args:
            **kwargs: The fields to update and their new values
            
        Returns:
            A new instance with the updated values
        """
        data = self.model_dump()
        data.update(kwargs)
        return self.__class__(**data)

    def __str__(self) -> str:
        """
        Get a string representation of the collection.
        
        Returns:
            A string representation of the collection
        """
        return f"{self.__class__.__name__}: count={len(self.messages)}, is_error={self.is_error}, error={self.error}"
    
    def __repr__(self) -> str:
        """
        Get a detailed representation of the collection.
        
        Returns:
            A detailed representation of the collection
        """
        return f"{self.__class__.__name__}(messages={self.messages!r}, is_error={self.is_error}, error={self.error!r})"


class QueueMessagesWaiting(QueueMessagesBase):
    """
    Collection of messages waiting in a queue.
    
    This class represents messages that are waiting in a queue,
    typically retrieved using a peek operation.
    
    Examples:
        ```python
        # Create a collection of waiting messages
        waiting = QueueMessagesWaiting(
            messages=[message1, message2],
            is_error=False
        )
        
        # Check if the collection is empty
        if waiting.is_empty():
            print("No messages waiting")
        else:
            print(f"Found {waiting.count()} messages waiting")
        ```
    """
    pass


class QueueMessagesPulled(QueueMessagesBase):
    """
    Collection of messages pulled from a queue.
    
    This class represents messages that have been pulled from a queue,
    typically retrieved using a pull operation.
    
    Examples:
        ```python
        # Create a collection of pulled messages
        pulled = QueueMessagesPulled(
            messages=[message1, message2],
            is_error=False
        )
        
        # Process the pulled messages
        for message in pulled.get_messages():
            process_message(message)
        ```
    """
    pass
