from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator

from kubemq.cq.command_message_received import CommandMessageReceived
from kubemq.grpc import Response as pbResponse


class CommandResponseMessage(BaseModel):
    command_received: Optional[CommandMessageReceived] = None
    client_id: str = Field(default="")
    request_id: str = Field(default="")
    is_executed: bool = Field(default=False)
    timestamp: datetime = Field(default_factory=datetime.now)
    error: str = Field(default="")
    metadata: Optional[str] = None
    body: bytes = Field(default=b"")
    tags: dict[str, str] = Field(default_factory=dict)

    model_config = {"arbitrary_types_allowed": True}

    @field_validator("command_received")
    def validate_command_received(
        cls, v: Optional[CommandMessageReceived]
    ) -> Optional[CommandMessageReceived]:
        if v is None:
            raise ValueError("Command response must have a command request.")
        if v.reply_channel == "":
            raise ValueError("Command response must have a reply channel.")
        return v

    @classmethod
    def decode(cls, pb_response: pbResponse) -> "CommandResponseMessage":
        return cls(
            client_id=pb_response.ClientID,
            request_id=pb_response.RequestID,
            is_executed=pb_response.Executed,
            error=pb_response.Error,
            timestamp=datetime.fromtimestamp(pb_response.Timestamp / 1e9),
            metadata=pb_response.Metadata,
            body=pb_response.Body,
            tags=dict(pb_response.Tags),
        )

    def encode(self, client_id: str) -> pbResponse:
        if not self.command_received:
            raise ValueError("Command received is required for encoding.")
        pb_response = pbResponse()
        pb_response.ClientID = client_id
        pb_response.RequestID = self.command_received.id
        pb_response.ReplyChannel = self.command_received.reply_channel
        pb_response.Executed = self.is_executed
        pb_response.Error = self.error
        pb_response.Timestamp = int(self.timestamp.timestamp() * 1e9)
        pb_response.Metadata = self.metadata or ""
        pb_response.Body = self.body or b""
        for key, value in self.tags.items():
            pb_response.Tags[key] = value
        return pb_response

    def __repr__(self) -> str:
        return (
            f"CommandResponseMessage: client_id={self.client_id}, "
            f"request_id={self.request_id}, is_executed={self.is_executed}, "
            f"error={self.error}, timestamp={self.timestamp}"
        )
