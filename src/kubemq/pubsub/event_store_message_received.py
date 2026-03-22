from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from kubemq.grpc import EventReceive as pbEventReceive


@dataclass(frozen=True)
class EventStoreReceived:
    """Received event store message from a subscription.

    Attributes:
        id: Unique event identifier.
        channel: Channel name the event was published to.
        metadata: Optional metadata string.
        body: Raw message payload as bytes.
        from_client_id: Client ID of the publisher (from tags).
        timestamp: Receipt timestamp.
        sequence: Ordering sequence number for the stored event.
        tags: Key-value tags associated with the event.

    See Also:
        EventStoreMessage: The outgoing message type for publishing.
        PubSubClient.subscribe_to_events_store: Subscribe to stored events.

    Thread Safety:
        Instances are safe to read from multiple threads or asyncio
        tasks after receipt. Do not modify fields after receiving.
    """

    id: str = ""
    from_client_id: str = ""
    timestamp: datetime = field(default_factory=lambda: datetime.fromtimestamp(0))
    channel: str = ""
    metadata: str = ""
    body: bytes = b""
    sequence: int = 0
    tags: dict[str, str] = field(default_factory=dict)

    @classmethod
    def decode(cls, event_receive: pbEventReceive) -> "EventStoreReceived":
        """Decode a protobuf EventReceive into an EventStoreReceived."""
        from_client_id = (
            event_receive.Tags.get("x-kubemq-client-id", "") if event_receive.Tags else ""
        )
        tags = dict(event_receive.Tags) if event_receive.Tags else {}

        return cls(
            id=event_receive.EventID,
            from_client_id=from_client_id,
            timestamp=datetime.fromtimestamp(event_receive.Timestamp / 1e9),
            channel=event_receive.Channel,
            metadata=event_receive.Metadata,
            body=event_receive.Body,
            sequence=event_receive.Sequence,
            tags=tags,
        )

    def to_dict(self, **kwargs: Any) -> dict[str, Any]:
        """Serialize the model to a dictionary with formatted fields."""
        return {
            "id": self.id,
            "from_client_id": self.from_client_id,
            "timestamp": self.timestamp.isoformat(),
            "channel": self.channel,
            "metadata": self.metadata,
            "body": self.body.hex(),
            "sequence": self.sequence,
            "tags": self.tags,
        }
