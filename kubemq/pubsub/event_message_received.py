from datetime import datetime
from typing import Dict
from pydantic import BaseModel, Field
from kubemq.grpc import EventReceive as pbEventReceive


class EventMessageReceived(BaseModel):
    id: str = ""
    from_client_id: str = ""
    timestamp: datetime = Field(default_factory=datetime.now)
    channel: str = ""
    metadata: str = ""
    body: bytes = b""
    tags: Dict[str, str] = Field(default_factory=dict)

    @classmethod
    def decode(cls, event_receive: pbEventReceive) -> "EventMessageReceived":
        from_client_id = (
            event_receive.Tags.get("x-kubemq-client-id", "")
            if event_receive.Tags
            else ""
        )
        tags = dict(event_receive.Tags) if event_receive.Tags else {}

        return cls(
            id=event_receive.EventID,
            from_client_id=from_client_id,
            channel=event_receive.Channel,
            metadata=event_receive.Metadata,
            body=event_receive.Body,
            tags=tags,
        )

    class Config:
        arbitrary_types_allowed = True

    def model_dump(self, **kwargs):
        dump = super().model_dump(**kwargs)
        dump["timestamp"] = self.timestamp.isoformat()
        return dump
