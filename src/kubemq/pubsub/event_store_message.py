from __future__ import annotations

from typing import Self
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from kubemq.common.channel_validators import validate_channel_name
from kubemq.grpc import Event as pbEvent


class EventStoreMessage(BaseModel):
    """A persistent event store message.

    Instances are immutable after construction. Use ``with_updates()``
    or ``model_copy(update={...})`` to create modified copies.

    Thread Safety:
        Instances are immutable (frozen) and safe to read from multiple
        threads. However, reusing the same instance for multiple send
        operations is not recommended — create a new instance per send
        to ensure unique message IDs.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    id: str = Field(default_factory=lambda: str(uuid4()))
    channel: str
    metadata: str | None = None
    body: bytes = Field(default=b"")
    tags: dict[str, str] = Field(default_factory=dict)

    @field_validator("channel")
    def channel_must_exist(cls, v: str) -> str:
        """Validate that the channel is not empty."""
        if not v:
            raise ValueError("Event Store message must have a channel.")
        validate_channel_name(v)
        return v

    @field_validator("metadata", "body", "tags")
    def at_least_one_must_exist(cls, v: object, info: ValidationInfo) -> object:
        """Validate at least one content field is set."""
        if (
            info.data.get("metadata") is None
            and info.data.get("body") == b""
            and not info.data.get("tags")
        ):
            raise ValueError(
                "Event Store message must have at least one of the following: metadata, body, or tags."
            )
        return v

    def encode(self, client_id: str) -> pbEvent:
        """Encode the event store message to a protobuf Event."""
        pb_event = pbEvent()
        pb_event.EventID = self.id or str(uuid4())
        pb_event.ClientID = client_id
        pb_event.Channel = self.channel
        pb_event.Metadata = self.metadata or ""
        pb_event.Body = self.body
        pb_event.Store = True
        pb_event.Tags.update(self.tags)
        return pb_event

    def with_updates(self, **kwargs: object) -> Self:
        """Create a new message with updated values.

        Since message instances are immutable, this creates a copy
        with the specified fields overridden.

        Returns:
            A new instance of the same type with updated fields.
        """
        return self.model_copy(update=kwargs)
