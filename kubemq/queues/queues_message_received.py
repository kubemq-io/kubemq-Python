from pydantic import BaseModel, Field
from typing import Dict, Callable, Optional
from datetime import datetime
from kubemq.grpc import QueueMessage as pbQueueMessage
from kubemq.grpc import (
    QueuesDownstreamRequest,
    QueuesDownstreamRequestType,
    QueuesDownstreamResponse,
)
import uuid


class QueueMessageReceived(BaseModel):
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

    def ack(self):
        if self.is_transaction_completed:
            raise ValueError("transaction is already completed")
        request = QueuesDownstreamRequest()
        request.RequestID = str(uuid.uuid4())
        request.ClientID = self.receiver_client_id
        request.Channel = self.channel
        request.RequestTypeData = QueuesDownstreamRequestType.AckRange
        request.RefTransactionId = self.transaction_id
        request.SequenceRange.append(self.sequence)
        if self.response_handler:
            self.response_handler(request)
        else:
            raise ValueError("response_handler is not set")

    def reject(self):
        if self.is_transaction_completed:
            raise ValueError("transaction is already completed")
        request = QueuesDownstreamRequest()
        request.RequestID = str(uuid.uuid4())
        request.ClientID = self.receiver_client_id
        request.Channel = self.channel
        request.RequestTypeData = QueuesDownstreamRequestType.NAckRange
        request.RefTransactionId = self.transaction_id
        request.SequenceRange.append(self.sequence)
        if self.response_handler:
            self.response_handler(request)
        else:
            raise ValueError("response_handler is not set")

    def re_queue(self, channel: str):
        if not channel:
            raise ValueError("re-queue channel cannot be empty")
        request = QueuesDownstreamRequest()
        request.RequestID = str(uuid.uuid4())
        request.ClientID = self.receiver_client_id
        request.Channel = self.channel
        request.RequestTypeData = QueuesDownstreamRequestType.ReQueueRange
        request.RefTransactionId = self.transaction_id
        request.SequenceRange.append(self.sequence)
        request.ReQueueChannel = channel
        if self.response_handler:
            self.response_handler(request)
        else:
            raise ValueError("response_handler is not set")

    @classmethod
    def decode(
        cls,
        message: pbQueueMessage,
        transaction_id: str,
        transaction_is_completed: bool,
        receiver_client_id: str,
        response_handler: Callable[[QueuesDownstreamRequest], QueuesDownstreamResponse],
    ) -> "QueueMessageReceived":
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
        )

    class Config:
        arbitrary_types_allowed = True
