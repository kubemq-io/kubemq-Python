from typing import Dict, Optional
from pydantic import BaseModel, Field, field_validator, model_validator
import uuid
from kubemq.grpc import Request as pbCommand


class CommandMessage(BaseModel):
    id: Optional[str] = Field(default_factory=lambda: str(uuid.uuid4()))
    channel: str
    metadata: Optional[str] = None
    body: bytes = Field(default=b"")
    tags: Dict[str, str] = Field(default_factory=dict)
    timeout_in_seconds: int = Field(gt=0)

    model_config = {"arbitrary_types_allowed": True}

    @field_validator("channel")
    def channel_must_exist(cls, v: str) -> str:
        if not v:
            raise ValueError("Command message must have a channel.")
        return v

    @model_validator(mode="after")
    def at_least_one_must_exist(self) -> "CommandMessage":
        if not self.metadata and not self.body and not self.tags:
            raise ValueError(
                "Command message must have at least one of the following: metadata, body, or tags."
            )
        return self

    def encode(self, client_id: str) -> pbCommand:
        pb_command = pbCommand()
        pb_command.RequestID = self.id
        pb_command.ClientID = client_id
        pb_command.Channel = self.channel
        pb_command.Metadata = self.metadata or ""
        pb_command.Body = self.body
        pb_command.Timeout = self.timeout_in_seconds * 1000
        pb_command.RequestTypeData = pbCommand.RequestType.Command
        for key, value in self.tags.items():
            pb_command.Tags[key] = value
        return pb_command

    def __repr__(self) -> str:
        return (
            f"CommandMessage: id={self.id}, channel={self.channel}, "
            f"metadata={self.metadata}, body={self.body}, tags={self.tags}, "
            f"timeout_in_seconds={self.timeout_in_seconds}"
        )
