from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Dict, Optional
import uuid
from kubemq.grpc import QueueMessage as pbQueueMessage
from kubemq.grpc import QueuesUpstreamRequest as pbQueuesUpstreamRequest


class QueueMessage(BaseModel):
    """A class representing a message in a queue."""

    id: Optional[str] = Field(
        default=None, description="The unique identifier for the message"
    )
    channel: str = Field(..., description="The channel of the message")
    metadata: Optional[str] = Field(
        default=None, description="The metadata associated with the message"
    )
    body: bytes = Field(default=b"", description="The body of the message")
    tags: Dict[str, str] = Field(
        default_factory=dict, description="The tags associated with the message"
    )
    delay_in_seconds: int = Field(
        default=0,
        ge=0,
        description="The delay in seconds before the message becomes available in the queue",
    )
    expiration_in_seconds: int = Field(
        default=0, ge=0, description="The expiration time in seconds for the message"
    )
    attempts_before_dead_letter_queue: int = Field(
        default=0,
        ge=0,
        description="The number of receive attempts allowed for the message before it is moved to the dead letter queue",
    )
    dead_letter_queue: str = Field(
        default="",
        description="The dead letter queue where the message will be moved after reaching the maximum receive attempts",
    )

    @field_validator("channel")
    def channel_must_not_be_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("Queue message must have a channel.")
        return v

    @model_validator(mode="after")
    def check_message_content(self) -> "QueueMessage":
        if not self.metadata and not self.body and not self.tags:
            raise ValueError(
                "Queue message must have at least one of the following: metadata, body, or tags."
            )
        return self

    def encode(self, client_id: str) -> pbQueuesUpstreamRequest:
        pb_queue_stream = pbQueuesUpstreamRequest()
        pb_queue_stream.RequestID = str(uuid.uuid4())
        pb_message = self.encode_message(client_id)
        pb_queue_stream.Messages.append(pb_message)
        return pb_queue_stream

    def encode_message(self, client_id: str) -> pbQueueMessage:
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

    class Config:
        arbitrary_types_allowed = True
