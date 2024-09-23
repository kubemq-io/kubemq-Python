from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime
from kubemq.grpc import SendQueueMessageResult


class QueueSendResult(BaseModel):
    """
    QueueSendResult represents the result of sending a message to a queue.

    Attributes:
        id (str): The unique identifier of the message.
        sent_at (datetime): The timestamp when the message was sent.
        expired_at (datetime): The timestamp when the message will expire.
        delayed_to (datetime): The timestamp when the message will be delivered.
        is_error (bool): Indicates if there was an error while sending the message.
        error (str): The error message if `is_error` is True.
    """

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

    @classmethod
    def decode(cls, result: SendQueueMessageResult) -> "QueueSendResult":
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

    class Config:
        arbitrary_types_allowed = True
    def __str__(self):
        return (
            f"QueueSendResult: id={self.id}, sent_at={self.sent_at}, expired_at={self.expired_at}, "
            f"delayed_to={self.delayed_to}, is_error={self.is_error}, error={self.error}"
        )