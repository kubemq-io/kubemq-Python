from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from kubemq.grpc import EventReceive as pbEventReceive


@dataclass(frozen=True)
class EventReceived:
    """Received event message from a subscription.

    Attributes:
        id: Unique event identifier.
        channel: Channel name the event was published to.
        metadata: Optional metadata string.
        body: Raw message payload as bytes.
        from_client_id: Client ID of the publisher (from tags).
        timestamp: Receipt timestamp.
        tags: Key-value tags associated with the event.

    See Also:
        EventMessage: The outgoing message type for publishing.
        PubSubClient.subscribe_to_events: Subscribe to events on a channel.

    Thread Safety:
        Instances are safe to read from multiple threads or asyncio
        tasks after receipt. Do not modify fields after receiving.
    """

    id: str = ""
    from_client_id: str = ""
    timestamp: datetime = field(default_factory=datetime.now)
    channel: str = ""
    metadata: str = ""
    body: bytes = b""
    tags: dict[str, str] = field(default_factory=dict)

    @classmethod
    def decode(cls, event_receive: pbEventReceive) -> "EventReceived":
        """Decode a protobuf EventReceive into an EventReceived.

        Returns:
            A new EventReceived instance populated from the protobuf message.
        """
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

    def to_dict(self, **kwargs: Any) -> dict[str, Any]:
        """Serialize the model to a dictionary with formatted timestamps."""
        return {
            "id": self.id,
            "from_client_id": self.from_client_id,
            "timestamp": self.timestamp.isoformat(),
            "channel": self.channel,
            "metadata": self.metadata,
            "body": self.body,
            "tags": self.tags,
        }
