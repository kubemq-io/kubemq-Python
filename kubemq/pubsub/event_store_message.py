from uuid import uuid4
from typing import Dict, Optional
from pydantic import BaseModel, Field, field_validator
from kubemq.grpc import Event as pbEvent


class EventStoreMessage(BaseModel):
    id: Optional[str] = None
    channel: str
    metadata: Optional[str] = None
    body: bytes = Field(default=b"")
    tags: Dict[str, str] = Field(default_factory=dict)

    @field_validator("channel")
    def channel_must_exist(cls, v):
        if not v:
            raise ValueError("Event Store message must have a channel.")
        return v

    @field_validator("metadata", "body", "tags")
    def at_least_one_must_exist(cls, v, info):
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
        tags = self.tags.copy()
        tags["x-kubemq-client-id"] = client_id

        pb_event = pbEvent()
        pb_event.EventID = self.id or str(uuid4())
        pb_event.ClientID = client_id
        pb_event.Channel = self.channel
        pb_event.Metadata = self.metadata or ""
        pb_event.Body = self.body
        pb_event.Store = True
        pb_event.Tags.update(tags)
        return pb_event

    def model_post_init(self, __context) -> None:
        if self.id is None:
            self.id = str(uuid4())

    class Config:
        arbitrary_types_allowed = True

    def model_dump(self, **kwargs):
        dump = super().model_dump(**kwargs)
        dump["body"] = (
            self.body.hex()
        )  # Convert bytes to hex string for better readability
        return dump
