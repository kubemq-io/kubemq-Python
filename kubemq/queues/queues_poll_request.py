from pydantic import BaseModel, Field, field_validator
from typing import Optional, Self
import uuid
from kubemq.grpc import QueuesDownstreamRequest, QueuesDownstreamRequestType


class QueuesPollRequest(BaseModel):
    """
    Class representing a request to poll messages from a queue.
    
    This class encapsulates the parameters needed to poll messages from a KubeMQ queue.
    It provides methods for validation and encoding to a protobuf request.
    
    Attributes:
        channel: The channel (queue name) to poll messages from.
        poll_max_messages: The maximum number of messages to poll in a single request.
        poll_wait_timeout_in_seconds: The maximum time to wait for messages in seconds.
        auto_ack_messages: Whether to automatically acknowledge received messages.
        visibility_seconds: The visibility time in seconds for the messages.
        
    Examples:
        ```python
        # Create a basic poll request
        request = QueuesPollRequest(
            channel="my-queue",
            poll_max_messages=10,
            poll_wait_timeout_in_seconds=30
        )
        
        # Create a poll request with auto acknowledgment
        request = QueuesPollRequest(
            channel="my-queue",
            poll_max_messages=5,
            poll_wait_timeout_in_seconds=10,
            auto_ack_messages=True
        )
        
        # Create a poll request with visibility timeout
        request = QueuesPollRequest(
            channel="my-queue",
            poll_max_messages=1,
            poll_wait_timeout_in_seconds=5,
            visibility_seconds=60  # Message will be invisible to other consumers for 60 seconds
        )
        
        # Encode the request to protobuf
        pb_request = request.encode("client-1")
        ```
    """
    
    # Pydantic configuration
    class Config:
        arbitrary_types_allowed = True
        frozen = True  # Make instances immutable

    # Instance attributes
    channel: Optional[str] = Field(
        default=None, description="The channel (queue name) to poll messages from"
    )
    poll_max_messages: int = Field(
        default=1,
        ge=1,
        description="The maximum number of messages to poll in a single request",
    )
    poll_wait_timeout_in_seconds: int = Field(
        default=60, 
        ge=1, 
        description="The maximum time to wait for messages in seconds"
    )
    auto_ack_messages: bool = Field(
        default=False,
        description="Whether to automatically acknowledge received messages",
    )
    visibility_seconds: int = Field(
        default=0,
        ge=0,
        description="The visibility time in seconds for the messages",
    )

    # Validators
    @field_validator("channel")
    def channel_must_not_be_empty(cls, v: Optional[str]) -> str:
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
            raise ValueError("Queue poll request must have a channel. Please provide a valid queue name.")
        return v
    
    @field_validator("visibility_seconds")
    def validate_visibility_seconds(cls, v: int) -> int:
        """
        Validate that the visibility seconds is within acceptable limits.
        
        Args:
            v: The visibility seconds value to validate
            
        Returns:
            The validated visibility seconds value
            
        Raises:
            ValueError: If the visibility seconds is negative
        """
        if v < 0:
            raise ValueError("Visibility seconds cannot be negative")
        return v

    # Utility methods
    def with_updates(self, **kwargs) -> Self:
        """
        Create a new poll request with updated values.
        
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
    
    # Encoding methods
    def encode(self, client_id: str = "") -> QueuesDownstreamRequest:
        """
        Encode the poll request to a QueuesDownstreamRequest protobuf object.
        
        This method is used when sending a poll request to the KubeMQ server.
        
        Args:
            client_id: The client ID to use for the request
            
        Returns:
            A QueuesDownstreamRequest protobuf object containing the encoded request
        """
        request = QueuesDownstreamRequest()
        request.RequestID = str(uuid.uuid4())
        request.ClientID = client_id
        request.Channel = self.channel
        request.MaxItems = self.poll_max_messages
        request.WaitTimeout = self.poll_wait_timeout_in_seconds * 1000
        request.AutoAck = self.auto_ack_messages
        request.RequestTypeData = QueuesDownstreamRequestType.Get
        return request

    # String representations
    def __str__(self) -> str:
        """
        Get a string representation of the poll request.
        
        Returns:
            A string representation of the poll request
        """
        return (
            f"QueuesPollRequest: channel={self.channel}, "
            f"poll_max_messages={self.poll_max_messages}, "
            f"poll_wait_timeout_in_seconds={self.poll_wait_timeout_in_seconds}, "
            f"auto_ack_messages={self.auto_ack_messages}, "
            f"visibility_seconds={self.visibility_seconds}"
        )
    
    def __repr__(self) -> str:
        """
        Get a detailed representation of the poll request.
        
        Returns:
            A detailed representation of the poll request
        """
        return (
            f"QueuesPollRequest(channel={self.channel!r}, "
            f"poll_max_messages={self.poll_max_messages}, "
            f"poll_wait_timeout_in_seconds={self.poll_wait_timeout_in_seconds}, "
            f"auto_ack_messages={self.auto_ack_messages}, "
            f"visibility_seconds={self.visibility_seconds})"
        )
