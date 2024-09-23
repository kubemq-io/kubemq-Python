from pydantic import BaseModel, Field
from typing import Dict, List, Optional
from datetime import datetime
from kubemq.grpc import QueueMessage as pbQueueMessage


class QueueMessageWaitingPulled(BaseModel):
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
    receiver_client_id: str = Field(default="")

    @classmethod
    def decode(
        cls,
        message: pbQueueMessage,
        receiver_client_id: str,
    ) -> "QueueMessageWaitingPulled":
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
            receiver_client_id=receiver_client_id,
        )

    def __str__(self):
        return (
            f"QueueMessageWaitingPulled: id={self.id}, channel={self.channel}, "
            f"metadata={self.metadata}, body={self.body.decode()}, "
            f"from_client_id={self.from_client_id}, timestamp={self.timestamp}, "
            f"sequence={self.sequence}, receive_count={self.receive_count}, "
            f"is_re_routed={self.is_re_routed}, re_route_from_queue={self.re_route_from_queue}, "
            f"expired_at={self.expired_at}, delayed_to={self.delayed_to}, tags={self.tags}"
        )

    class Config:
        arbitrary_types_allowed = True


class QueueMessagesBase(BaseModel):
    messages: List[QueueMessageWaitingPulled] = Field(default_factory=list)
    is_error: bool = Field(default=False)
    error: Optional[str] = Field(default=None)

    def get_messages(self) -> List[QueueMessageWaitingPulled]:
        return self.messages

    def __str__(self) -> str:
        return (
            f"QueueMessages: messages={self.messages}, is_error={self.is_error}, error={self.error}"
        )


class QueueMessagesWaiting(QueueMessagesBase):
    pass


class QueueMessagesPulled(QueueMessagesBase):
    pass
