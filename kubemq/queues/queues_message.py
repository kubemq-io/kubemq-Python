from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Dict, Optional, ClassVar, Self
import uuid
from datetime import datetime
from kubemq.grpc import QueueMessage as pbQueueMessage
from kubemq.grpc import QueuesUpstreamRequest as pbQueuesUpstreamRequest


class QueueMessage(BaseModel):
    """
    A class representing a message in a KubeMQ queue.
    
    This class encapsulates all the properties of a message that can be sent to a KubeMQ queue.
    It provides methods for validation, encoding to protobuf format, and creating messages
    from protobuf format.
    
    Attributes:
        id: The unique identifier for the message. If not provided, a UUID will be generated.
        channel: The channel (queue name) where the message will be sent. Required.
        metadata: Optional metadata associated with the message.
        body: The binary payload of the message.
        tags: Key-value pairs for additional message metadata.
        delay_in_seconds: Time in seconds to delay the message before it becomes available.
        expiration_in_seconds: Time in seconds after which the message expires.
        attempts_before_dead_letter_queue: Maximum number of receive attempts before moving to DLQ.
        dead_letter_queue: The queue where messages are moved after max receive attempts.
    
    Examples:
        ```python
        # Create a simple message
        message = QueueMessage(
            channel="my-queue",
            body=b"Hello, World!"
        )
        
        # Create a message with metadata and tags
        message = QueueMessage(
            channel="my-queue",
            metadata="Message metadata",
            body=b"Hello, World!",
            tags={"key1": "value1", "key2": "value2"}
        )
        
        # Create a message with delay and expiration
        message = QueueMessage(
            channel="my-queue",
            body=b"Hello, World!",
            delay_in_seconds=60,  # Delay for 1 minute
            expiration_in_seconds=3600  # Expire after 1 hour
        )
        
        # Create a message with dead letter queue configuration
        message = QueueMessage(
            channel="my-queue",
            body=b"Hello, World!",
            attempts_before_dead_letter_queue=3,
            dead_letter_queue="my-dlq"
        )
        ```
    """
    
    # Pydantic configuration
    class Config:
        arbitrary_types_allowed = True
        frozen = True  # Make instances immutable

    # Class attributes
    MAX_DELAY_SECONDS: ClassVar[int] = 43200  # 12 hours
    MAX_EXPIRATION_SECONDS: ClassVar[int] = 43200  # 12 hours
    
    # Instance attributes
    id: Optional[str] = Field(
        default=None, description="The unique identifier for the message"
    )
    channel: str = Field(..., description="The channel (queue name) where the message will be sent")
    metadata: Optional[str] = Field(
        default=None, description="The metadata associated with the message"
    )
    body: bytes = Field(default=b"", description="The binary payload of the message")
    tags: Dict[str, str] = Field(
        default_factory=dict, description="Key-value pairs for additional message metadata"
    )
    delay_in_seconds: int = Field(
        default=0,
        ge=0,
        description="Time in seconds to delay the message before it becomes available",
    )
    expiration_in_seconds: int = Field(
        default=0, 
        ge=0, 
        description="Time in seconds after which the message expires"
    )
    attempts_before_dead_letter_queue: int = Field(
        default=0,
        ge=0,
        description="Maximum number of receive attempts before moving to DLQ",
    )
    dead_letter_queue: str = Field(
        default="",
        description="The queue where messages are moved after max receive attempts",
    )

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
    
    @field_validator("delay_in_seconds")
    def validate_delay(cls, v: int) -> int:
        """
        Validate that the delay is within acceptable limits.
        
        Args:
            v: The delay value to validate
            
        Returns:
            The validated delay value
            
        Raises:
            ValueError: If the delay exceeds the maximum allowed value
        """
        if v > cls.MAX_DELAY_SECONDS:
            raise ValueError(f"Delay cannot exceed {cls.MAX_DELAY_SECONDS} seconds (12 hours)")
        return v
    
    @field_validator("expiration_in_seconds")
    def validate_expiration(cls, v: int) -> int:
        """
        Validate that the expiration is within acceptable limits.
        
        Args:
            v: The expiration value to validate
            
        Returns:
            The validated expiration value
            
        Raises:
            ValueError: If the expiration exceeds the maximum allowed value
        """
        if v > cls.MAX_EXPIRATION_SECONDS:
            raise ValueError(f"Expiration cannot exceed {cls.MAX_EXPIRATION_SECONDS} seconds (12 hours)")
        return v

    @model_validator(mode="after")
    def check_message_content(self) -> Self:
        """
        Validate that the message has at least one of metadata, body, or tags.
        
        Returns:
            The validated message
            
        Raises:
            ValueError: If the message has no content
        """
        if not self.metadata and not self.body and not self.tags:
            raise ValueError(
                "Message must have at least one of the following: metadata, body, or tags. "
                "Empty messages are not allowed."
            )
        
        # Validate dead letter queue configuration
        if self.attempts_before_dead_letter_queue > 0 and not self.dead_letter_queue:
            raise ValueError(
                "When specifying attempts_before_dead_letter_queue, "
                "you must also provide a dead_letter_queue."
            )
            
        return self

    # Encoding methods
    def encode(self, client_id: str) -> pbQueuesUpstreamRequest:
        """
        Encode the message to a QueuesUpstreamRequest protobuf object.
        
        This method is used when sending a message to the KubeMQ server.
        
        Args:
            client_id: The client ID to use for the message
            
        Returns:
            A QueuesUpstreamRequest protobuf object containing the encoded message
        """
        pb_queue_stream = pbQueuesUpstreamRequest()
        pb_queue_stream.RequestID = str(uuid.uuid4())
        pb_message = self.encode_message(client_id)
        pb_queue_stream.Messages.append(pb_message)
        return pb_queue_stream

    def encode_message(self, client_id: str) -> pbQueueMessage:
        """
        Encode the message to a QueueMessage protobuf object.
        
        Args:
            client_id: The client ID to use for the message
            
        Returns:
            A QueueMessage protobuf object containing the encoded message
        """
        pb_queue = pbQueueMessage()
        pb_queue.MessageID = self.id or str(uuid.uuid4())
        pb_queue.ClientID = client_id
        pb_queue.Channel = self.channel
        pb_queue.Metadata = self.metadata or ""
        pb_queue.Body = self.body
        pb_queue.Tags.update(self.tags)
        pb_queue.Policy.DelaySeconds = self.delay_in_seconds
        pb_queue.Policy.ExpirationSeconds = self.expiration_in_seconds
        pb_queue.Policy.MaxReceiveCount = self.attempts_before_dead_letter_queue
        pb_queue.Policy.MaxReceiveQueue = self.dead_letter_queue
        return pb_queue
    
    # Decoding methods
    @classmethod
    def decode(cls, pb_message: pbQueueMessage) -> Self:
        """
        Create a QueueMessage from a protobuf QueueMessage.
        
        Args:
            pb_message: The protobuf QueueMessage to decode
            
        Returns:
            A new QueueMessage instance
        """
        tags = {key: pb_message.Tags[key] for key in pb_message.Tags}
        
        return cls(
            id=pb_message.MessageID,
            channel=pb_message.Channel,
            metadata=pb_message.Metadata if pb_message.Metadata else None,
            body=pb_message.Body,
            tags=tags,
            delay_in_seconds=pb_message.Policy.DelaySeconds if pb_message.Policy else 0,
            expiration_in_seconds=pb_message.Policy.ExpirationSeconds if pb_message.Policy else 0,
            attempts_before_dead_letter_queue=pb_message.Policy.MaxReceiveCount if pb_message.Policy else 0,
            dead_letter_queue=pb_message.Policy.MaxReceiveQueue if pb_message.Policy else "",
        )
    
    # Utility methods
    def with_updates(self, **kwargs) -> Self:
        """
        Create a new QueueMessage with updated values.
        
        Since QueueMessage instances are immutable, this method creates a new
        instance with the specified updates.
        
        Args:
            **kwargs: The fields to update and their new values
            
        Returns:
            A new QueueMessage instance with the updated values
        """
        data = self.model_dump()
        data.update(kwargs)
        return self.__class__(**data)
    
    def __str__(self) -> str:
        """
        Get a string representation of the message.
        
        Returns:
            A string representation of the message
        """
        body_preview = self.body[:20].decode('utf-8', errors='replace') if self.body else ""
        if len(self.body) > 20:
            body_preview += "..."
            
        return (
            f"QueueMessage(id={self.id}, channel={self.channel}, "
            f"metadata={self.metadata}, body_preview='{body_preview}', "
            f"tags={self.tags}, delay={self.delay_in_seconds}s, "
            f"expiration={self.expiration_in_seconds}s)"
        )
    
    def __repr__(self) -> str:
        """
        Get a detailed representation of the message.
        
        Returns:
            A detailed representation of the message
        """
        return (
            f"QueueMessage(id={self.id!r}, channel={self.channel!r}, "
            f"metadata={self.metadata!r}, body={self.body!r}, tags={self.tags!r}, "
            f"delay_in_seconds={self.delay_in_seconds}, "
            f"expiration_in_seconds={self.expiration_in_seconds}, "
            f"attempts_before_dead_letter_queue={self.attempts_before_dead_letter_queue}, "
            f"dead_letter_queue={self.dead_letter_queue!r})"
        )
