from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import Optional
from kubemq.cq.command_message_received import CommandMessageReceived
from kubemq.grpc import Response as pbResponse


class CommandResponseMessage(BaseModel):
    command_received: Optional[CommandMessageReceived] = None
    client_id: str = Field(default="")
    request_id: str = Field(default="")
    is_executed: bool = Field(default=False)
    timestamp: datetime = Field(default_factory=datetime.now)
    error: str = Field(default="")

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
        return pb_response

    def __repr__(self) -> str:
        return (
            f"CommandResponseMessage: client_id={self.client_id}, "
            f"request_id={self.request_id}, is_executed={self.is_executed}, "
            f"error={self.error}, timestamp={self.timestamp}"
        )
