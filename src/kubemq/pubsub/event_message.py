from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

from kubemq.common.channel_validators import validate_channel_name
from kubemq.common.helpers import fast_id
from kubemq.grpc import Event as pbEvent


@dataclass(frozen=True)
class EventMessage:
    """An event message for fire-and-forget publishing.

    Instances are immutable after construction. Use ``with_updates()``
    to create modified copies.

    Raises:
        KubeMQValidationError: If channel is missing or empty, or if all of
            metadata, body, and tags are empty (at least one must be set).

    See Also:
        EventReceived: The received counterpart when subscribing.
        PubSubClient.subscribe_to_events: Publish events to a channel.

    Thread Safety:
        Instances are immutable (frozen) and safe to read from multiple
        threads. However, reusing the same instance for multiple send
        operations is not recommended — create a new instance per send
        to ensure unique message IDs.
    """

    channel: str
    id: str = field(default_factory=fast_id)
    metadata: str | None = None
    body: bytes = b""
    tags: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate event message fields."""
        if not self.channel:
            raise ValueError("Event message must have a channel.")
        validate_channel_name(self.channel)
        if not self.metadata and not self.body and not self.tags:
            raise ValueError(
                "Event message must have at least one of the following: metadata, body, or tags."
            )

    def encode(self, client_id: str) -> pbEvent:
        """Encode the event message to a protobuf Event.

        Returns:
            The protobuf Event ready for transmission.
        """
        pb_event = pbEvent()
        pb_event.EventID = self.id or fast_id()
        pb_event.ClientID = client_id
        pb_event.Channel = self.channel
        pb_event.Metadata = self.metadata or ""
        pb_event.Body = self.body
        pb_event.Store = False
        pb_event.Tags.update(self.tags)
        return pb_event

    def with_updates(self, **kwargs: object) -> Self:
        """Create a new message with updated values.

        Returns:
            A new EventMessage with the specified fields replaced.
        """
        return dataclasses.replace(self, **kwargs)
