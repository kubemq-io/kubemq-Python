from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from kubemq.grpc import EventReceive as pbEventReceive


class EventMessageReceived(BaseModel):
    """Received event message from a subscription.

    Thread Safety:
        Instances are safe to read from multiple threads or asyncio
        tasks after receipt. Do not modify fields after receiving.
    """

    id: str = ""
    from_client_id: str = ""
    timestamp: datetime = Field(default_factory=datetime.now)
    channel: str = ""
    metadata: str = ""
    body: bytes = b""
    tags: dict[str, str] = Field(default_factory=dict)

    @classmethod
    def decode(cls, event_receive: pbEventReceive) -> "EventMessageReceived":
        """Decode a protobuf EventReceive into an EventMessageReceived."""
        from_client_id = (
            event_receive.Tags.get("x-kubemq-client-id", "") if event_receive.Tags else ""
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

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    def model_dump(self, **kwargs: Any) -> dict[str, Any]:
        """Serialize the model to a dictionary with formatted timestamps."""
        dump = super().model_dump(**kwargs)
        dump["timestamp"] = self.timestamp.isoformat()
        return dump
