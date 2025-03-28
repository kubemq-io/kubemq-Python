from pydantic import BaseModel, Field
from typing import Optional, Self, ClassVar
from datetime import datetime
from kubemq.grpc import SendQueueMessageResult


class QueueSendResult(BaseModel):
    """
    Represents the result of sending a message to a queue.
    
    This class encapsulates the result of sending a message to a KubeMQ queue.
    It provides information about the message's ID, timestamps, and any errors
    that occurred during the send operation.
    
    Attributes:
        id: The unique identifier of the message.
        sent_at: The timestamp when the message was sent.
        expired_at: The timestamp when the message will expire.
        delayed_to: The timestamp when the message will be delivered.
        is_error: Indicates if there was an error while sending the message.
        error: The error message if `is_error` is True.
        
    Examples:
        ```python
        # Send a message and check the result
        result = client.send_queues_message(message)
        
        if result.is_error:
            print(f"Error sending message: {result.error}")
        else:
            print(f"Message sent successfully with ID: {result.id}")
            
        # Check if the message is delayed
        if result.is_delayed():
            print(f"Message will be delivered at: {result.delayed_to}")
            
        # Check if the message has an expiration
        if result.has_expiration():
            print(f"Message will expire at: {result.expired_at}")
        ```
    """
    
    # Pydantic configuration
    class Config:
        arbitrary_types_allowed = True
        frozen = True  # Make instances immutable
    
    # Class attributes
    EPOCH: ClassVar[datetime] = datetime.fromtimestamp(0)

    # Instance attributes
    id: Optional[str] = Field(
        default=None, description="The unique identifier of the message"
    )
    sent_at: Optional[datetime] = Field(
        default=None, description="The timestamp when the message was sent"
    )
    expired_at: Optional[datetime] = Field(
        default=None, description="The timestamp when the message will expire"
    )
    delayed_to: Optional[datetime] = Field(
        default=None, description="The timestamp when the message will be delivered"
    )
    is_error: bool = Field(
        default=False,
        description="Indicates if there was an error while sending the message",
    )
    error: Optional[str] = Field(
        default=None, description="The error message if `is_error` is True"
    )
    
    # Utility methods
    def is_successful(self) -> bool:
        """
        Check if the message was sent successfully.
        
        Returns:
            bool: True if the message was sent successfully, False otherwise.
        """
        return not self.is_error and self.id is not None
    
    def is_delayed(self) -> bool:
        """
        Check if the message is delayed.
        
        Returns:
            bool: True if the message is delayed, False otherwise.
        """
        return self.delayed_to is not None and self.delayed_to > datetime.now()
    
    def has_expiration(self) -> bool:
        """
        Check if the message has an expiration time.
        
        Returns:
            bool: True if the message has an expiration time, False otherwise.
        """
        return self.expired_at is not None
    
    def get_delay_seconds(self) -> float:
        """
        Get the number of seconds until the message is delivered.
        
        Returns:
            float: The number of seconds until the message is delivered, or 0 if not delayed.
        """
        if not self.is_delayed():
            return 0
        
        return max(0, (self.delayed_to - datetime.now()).total_seconds())
    
    def with_updates(self, **kwargs) -> Self:
        """
        Create a new result with updated values.
        
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

    # Decoding methods
    @classmethod
    def decode(cls, result: SendQueueMessageResult) -> Self:
        """
        Create a QueueSendResult from a protobuf SendQueueMessageResult.
        
        Args:
            result: The protobuf result to decode
            
        Returns:
            A new QueueSendResult instance
            
        Raises:
            ValueError: If the result is invalid
        """
        if not result:
            raise ValueError("Cannot decode None result")
            
        try:
            return cls(
                id=result.MessageID if result.MessageID else None,
                sent_at=(
                    datetime.fromtimestamp(result.SentAt / 1e9)
                    if result.SentAt > 0
                    else None
                ),
                expired_at=(
                    datetime.fromtimestamp(result.ExpirationAt / 1e9)
                    if result.ExpirationAt > 0
                    else None
                ),
                delayed_to=(
                    datetime.fromtimestamp(result.DelayedTo / 1e9)
                    if result.DelayedTo > 0
                    else None
                ),
                is_error=result.IsError if result.IsError else False,
                error=result.Error if result.Error else None,
            )
        except Exception as e:
            raise ValueError(f"Failed to decode result: {str(e)}")

    # String representations
    def __str__(self) -> str:
        """
        Get a string representation of the send result.
        
        Returns:
            A string representation of the send result
        """
        try:
            status = "ERROR" if self.is_error else "SUCCESS"
            return (
                f"QueueSendResult: status={status}, id={self.id}, "
                f"sent_at={self.sent_at}, "
                f"expired_at={self.expired_at}, "
                f"delayed_to={self.delayed_to}, "
                f"error={self.error if self.is_error else 'None'}"
            )
        except Exception as e:
            return f"QueueSendResult: [Error displaying result: {str(e)}]"
    
    def __repr__(self) -> str:
        """
        Get a detailed representation of the send result.
        
        Returns:
            A detailed representation of the send result
        """
        return (
            f"QueueSendResult(id={self.id!r}, "
            f"sent_at={self.sent_at!r}, "
            f"expired_at={self.expired_at!r}, "
            f"delayed_to={self.delayed_to!r}, "
            f"is_error={self.is_error}, "
            f"error={self.error!r})"
        )
