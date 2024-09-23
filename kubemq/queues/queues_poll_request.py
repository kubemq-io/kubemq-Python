from pydantic import BaseModel, Field, field_validator
from typing import Optional
import uuid
from kubemq.grpc import QueuesDownstreamRequest, QueuesDownstreamRequestType


class QueuesPollRequest(BaseModel):
    """
    Class representing a request to poll messages from a queue.

    Attributes:
        channel (str): The channel to subscribe to.
        poll_max_messages (int): The maximum number of messages to poll in a single request.
        poll_wait_timeout_in_seconds (int): The maximum time to wait for messages in seconds.
        auto_ack_messages (bool): Whether to automatically acknowledge received messages.
    """

    channel: Optional[str] = Field(
        default=None, description="The channel to subscribe to"
    )
    poll_max_messages: int = Field(
        default=1,
        ge=1,
        description="The maximum number of messages to poll in a single request",
    )
    poll_wait_timeout_in_seconds: int = Field(
        default=60, ge=1, description="The maximum time to wait for messages in seconds"
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

    @field_validator("channel")
    def channel_must_not_be_empty(cls, v: Optional[str]) -> str:
        if not v:
            raise ValueError("queue subscription must have a channel.")
        return v


    def encode(self, client_id: str = "") -> QueuesDownstreamRequest:
        request = QueuesDownstreamRequest()
        request.RequestID = str(uuid.uuid4())
        request.ClientID = client_id
        request.Channel = self.channel
        request.MaxItems = self.poll_max_messages
        request.WaitTimeout = self.poll_wait_timeout_in_seconds * 1000
        request.AutoAck = self.auto_ack_messages
        request.RequestTypeData = QueuesDownstreamRequestType.Get
        return request

    def __str__(self) -> str:
        return (
            f"QueuesSubscription: channel={self.channel}, poll_max_messages={self.poll_max_messages}, "
            f"poll_wait_timeout_in_seconds={self.poll_wait_timeout_in_seconds}, auto_ack_messages={self.auto_ack_messages}"
        )

    class Config:
        arbitrary_types_allowed = True
