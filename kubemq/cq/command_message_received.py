from pydantic import BaseModel, Field
from typing import Dict
from datetime import datetime
from kubemq.grpc import Request as pbRequest


class CommandMessageReceived(BaseModel):
    id: str = Field(default="")
    from_client_id: str = Field(default="")
    timestamp: datetime = Field(default_factory=datetime.now)
    channel: str = Field(default="")
    metadata: str = Field(default="")
    body: bytes = Field(default=b"")
    reply_channel: str = Field(default="")
    tags: Dict[str, str] = Field(default_factory=dict)

    model_config = {"arbitrary_types_allowed": True}

    @classmethod
    def decode(cls, command_receive: pbRequest) -> "CommandMessageReceived":
        return cls(
            id=command_receive.RequestID,
            from_client_id=command_receive.ClientID,
            timestamp=datetime.now(),
            channel=command_receive.Channel,
            metadata=command_receive.Metadata,
            body=command_receive.Body,
            reply_channel=command_receive.ReplyChannel,
            tags=dict(command_receive.Tags),
        )

    def __repr__(self) -> str:
        return (
            f"CommandMessageReceived: id={self.id}, channel={self.channel}, "
            f"metadata={self.metadata}, body={self.body}, "
            f"from_client_id={self.from_client_id}, timestamp={self.timestamp}, "
            f"reply_channel={self.reply_channel}, tags={self.tags}"
        )
