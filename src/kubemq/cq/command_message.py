from __future__ import annotations

import uuid
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from kubemq.common.channel_validators import validate_channel_name
from kubemq.grpc import Request as pbCommand


class CommandMessage(BaseModel):
    """A command message for request-response patterns.

    Instances are immutable after construction. Use ``with_updates()``
    or ``model_copy(update={...})`` to create modified copies.

    Thread Safety:
        Instances are immutable (frozen) and safe to read from multiple
        threads. However, reusing the same instance for multiple send
        operations is not recommended — create a new instance per send
        to ensure unique message IDs.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    id: str | None = Field(default_factory=lambda: str(uuid.uuid4()))
    channel: str
    metadata: str | None = None
    body: bytes = Field(default=b"")
    tags: dict[str, str] = Field(default_factory=dict)
    timeout_in_seconds: int = Field(gt=0)

    @field_validator("channel")
    def channel_must_exist(cls, v: str) -> str:
        """Validate that the channel is not empty."""
        if not v:
            raise ValueError("Command message must have a channel.")
        validate_channel_name(v)
        return v

    @model_validator(mode="after")
    def at_least_one_must_exist(self) -> CommandMessage:
        """Validate that at least one content field is set."""
        if not self.metadata and not self.body and not self.tags:
            raise ValueError(
                "Command message must have at least one of the following: metadata, body, or tags."
            )
        return self

    def encode(self, client_id: str, *, span: bytes = b"") -> pbCommand:
        """Encode the command message to a protobuf Request."""
        pb_command = pbCommand()
        pb_command.RequestID = self.id or str(uuid.uuid4())
        pb_command.ClientID = client_id
        pb_command.Channel = self.channel
        pb_command.Metadata = self.metadata or ""
        pb_command.Body = self.body
        pb_command.Timeout = self.timeout_in_seconds * 1000
        pb_command.RequestTypeData = pbCommand.RequestType.Command
        for key, value in self.tags.items():
            pb_command.Tags[key] = value
        if span:
            pb_command.Span = span
        return pb_command

    def with_updates(self, **kwargs: Any) -> Self:
        """Create a new message with updated values.

        Since message instances are immutable, this creates a copy
        with the specified fields overridden.

        Returns:
            A new instance of the same type with updated fields.
        """
        return self.model_copy(update=kwargs)

    def __repr__(self) -> str:
        return (
            f"CommandMessage: id={self.id}, channel={self.channel}, "
            f"metadata={self.metadata}, body={self.body!r}, tags={self.tags}, "
            f"timeout_in_seconds={self.timeout_in_seconds}"
        )
