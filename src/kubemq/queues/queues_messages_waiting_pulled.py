from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, ClassVar

from kubemq.grpc import QueueMessage as pbQueueMessage


@dataclass(frozen=True)
class QueueMessageWaitingPulled:
    """Represents a message that is waiting in a queue or has been pulled from a queue."""

    # Class attributes
    EPOCH: ClassVar[datetime] = datetime.fromtimestamp(0)

    # Instance attributes
    id: str = ""
    channel: str = ""
    metadata: str = ""
    body: bytes = b""
    from_client_id: str = ""
    tags: dict[str, str] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=lambda: datetime.fromtimestamp(0))
    sequence: int = 0
    receive_count: int = 0
    is_re_routed: bool = False
    re_route_from_queue: str = ""
    expired_at: datetime = field(default_factory=lambda: datetime.fromtimestamp(0))
    delayed_to: datetime = field(default_factory=lambda: datetime.fromtimestamp(0))
    receiver_client_id: str = ""

    def __post_init__(self) -> None:
        """Validate message fields."""
        if not self.channel:
            raise ValueError("Channel cannot be empty. Please provide a valid queue name.")

    # Utility methods
    def is_expired(self) -> bool:
        """Check if the message has expired."""
        return self.expired_at > self.EPOCH and datetime.now() > self.expired_at

    def is_delayed(self) -> bool:
        """Check if the message is delayed."""
        return self.delayed_to > self.EPOCH and datetime.now() < self.delayed_to

    def get_delay_seconds(self) -> float:
        """Get the number of seconds until the message is no longer delayed."""
        if not self.is_delayed():
            return 0
        return max(0, (self.delayed_to - datetime.now()).total_seconds())

    def get_age_seconds(self) -> float:
        """Get the age of the message in seconds."""
        if self.timestamp == self.EPOCH:
            return 0
        return (datetime.now() - self.timestamp).total_seconds()

    def with_updates(self, **kwargs: Any) -> QueueMessageWaitingPulled:
        """Create a new message with updated values."""
        return dataclasses.replace(self, **kwargs)

    # Encoding/decoding methods
    def encode(self) -> pbQueueMessage:
        """Encode the message to a protobuf QueueMessage."""
        pb_queue = pbQueueMessage()
        pb_queue.MessageID = self.id
        pb_queue.ClientID = self.from_client_id
        pb_queue.Channel = self.channel
        pb_queue.Metadata = self.metadata
        pb_queue.Body = self.body
        pb_queue.Tags.update(self.tags)

        if (
            self.timestamp != self.EPOCH
            or self.sequence != 0
            or self.receive_count != 0
            or self.is_re_routed
            or self.re_route_from_queue
            or self.expired_at != self.EPOCH
            or self.delayed_to != self.EPOCH
        ):
            if self.timestamp != self.EPOCH:
                pb_queue.Attributes.Timestamp = int(self.timestamp.timestamp() * 1e9)

            pb_queue.Attributes.Sequence = self.sequence
            pb_queue.Attributes.ReceiveCount = self.receive_count
            pb_queue.Attributes.ReRouted = self.is_re_routed
            pb_queue.Attributes.ReRoutedFromQueue = self.re_route_from_queue

            if self.expired_at != self.EPOCH:
                pb_queue.Attributes.ExpirationAt = int(self.expired_at.timestamp() * 1e9)

            if self.delayed_to != self.EPOCH:
                pb_queue.Attributes.DelayedTo = int(self.delayed_to.timestamp() * 1e9)

        return pb_queue

    @classmethod
    def decode(
        cls,
        message: pbQueueMessage,
        receiver_client_id: str,
    ) -> QueueMessageWaitingPulled:
        """Create a QueueMessageWaitingPulled from a protobuf QueueMessage."""
        if not message:
            raise ValueError("Cannot decode None message")

        if not message.Channel:
            raise ValueError("Message must have a channel")

        try:
            tags = {key: message.Tags[key] for key in message.Tags}

            return cls(
                id=message.MessageID,
                channel=message.Channel,
                metadata=message.Metadata,
                body=message.Body,
                from_client_id=message.ClientID,
                tags=tags,
                timestamp=(
                    datetime.fromtimestamp(message.Attributes.Timestamp / 1e9)
                    if message.Attributes and message.Attributes.Timestamp
                    else datetime.fromtimestamp(0)
                ),
                sequence=message.Attributes.Sequence if message.Attributes else 0,
                receive_count=message.Attributes.ReceiveCount if message.Attributes else 0,
                is_re_routed=message.Attributes.ReRouted if message.Attributes else False,
                re_route_from_queue=(
                    message.Attributes.ReRoutedFromQueue if message.Attributes else ""
                ),
                expired_at=(
                    datetime.fromtimestamp(message.Attributes.ExpirationAt / 1e9)
                    if message.Attributes and message.Attributes.ExpirationAt
                    else datetime.fromtimestamp(0)
                ),
                delayed_to=(
                    datetime.fromtimestamp(message.Attributes.DelayedTo / 1e9)
                    if message.Attributes and message.Attributes.DelayedTo
                    else datetime.fromtimestamp(0)
                ),
                receiver_client_id=receiver_client_id,
            )
        except Exception as e:
            raise ValueError(f"Failed to decode message: {str(e)}") from e

    def __str__(self) -> str:
        """Get a string representation of the message."""
        try:
            body_preview = self.body[:20].decode("utf-8", errors="replace") if self.body else ""
            if len(self.body) > 20:
                body_preview += "..."

            return (
                f"QueueMessageWaitingPulled: id={self.id}, channel={self.channel}, "
                f"metadata={self.metadata}, body_preview='{body_preview}', "
                f"from_client_id={self.from_client_id}, timestamp={self.timestamp}, "
                f"sequence={self.sequence}, receive_count={self.receive_count}, "
                f"expired_at={self.expired_at}, delayed_to={self.delayed_to}"
            )
        except Exception as e:
            return f"QueueMessageWaitingPulled: id={self.id}, channel={self.channel}, [Error displaying message: {str(e)}]"

    def __repr__(self) -> str:
        """Get a detailed representation of the message."""
        return (
            f"QueueMessageWaitingPulled(id={self.id!r}, channel={self.channel!r}, "
            f"metadata={self.metadata!r}, body={self.body!r}, "
            f"from_client_id={self.from_client_id!r}, tags={self.tags!r}, "
            f"timestamp={self.timestamp!r}, sequence={self.sequence}, "
            f"receive_count={self.receive_count}, is_re_routed={self.is_re_routed}, "
            f"re_route_from_queue={self.re_route_from_queue!r}, "
            f"expired_at={self.expired_at!r}, delayed_to={self.delayed_to!r}, "
            f"receiver_client_id={self.receiver_client_id!r})"
        )


@dataclass(frozen=True)
class QueueMessagesBase:
    """Base class for collections of queue messages."""

    messages: list[QueueMessageWaitingPulled] = field(default_factory=list)
    is_error: bool = False
    error: str | None = None
    messages_received: int = 0
    messages_expired: int = 0
    is_peak: bool = False

    def get_messages(self) -> list[QueueMessageWaitingPulled]:
        """Get the list of messages in the collection."""
        return self.messages

    def count(self) -> int:
        """Get the number of messages in the collection."""
        return len(self.messages)

    def is_empty(self) -> bool:
        """Check if the collection is empty."""
        return len(self.messages) == 0

    def with_updates(self, **kwargs: Any) -> QueueMessageWaitingPulled:
        """Create a new collection with updated values."""
        return dataclasses.replace(self, **kwargs)  # type: ignore[return-value]

    def __str__(self) -> str:
        """Get a string representation of the collection."""
        return f"{self.__class__.__name__}: count={len(self.messages)}, is_error={self.is_error}, error={self.error}"

    def __repr__(self) -> str:
        """Get a detailed representation of the collection."""
        return f"{self.__class__.__name__}(messages={self.messages!r}, is_error={self.is_error}, error={self.error!r})"


@dataclass(frozen=True)
class QueueMessagesWaiting(QueueMessagesBase):
    """Collection of messages waiting in a queue."""

    pass


@dataclass(frozen=True)
class QueueMessagesPulled(QueueMessagesBase):
    """Collection of messages pulled from a queue."""

    pass
