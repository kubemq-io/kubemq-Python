from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from typing import Any, ClassVar

from kubemq.common.channel_validators import validate_channel_name
from kubemq.common.helpers import fast_id
from kubemq.grpc import (
    QueueMessage as pbQueueMessage,
    QueuesUpstreamRequest as pbQueuesUpstreamRequest,
)


@dataclass(frozen=True)
class QueueMessage:
    """A class representing a message in a KubeMQ queue.

    This class encapsulates all the properties of a message that can be sent to a KubeMQ queue.
    It provides methods for validation, encoding to protobuf format, and creating messages
    from protobuf format.

    Attributes:
        id: The unique identifier for the message. If not provided, a UUID will be generated.
        channel: The channel (queue name) where the message will be sent. Required.
        metadata: Optional metadata associated with the message.
        body: The binary payload of the message.
        tags: Key-value pairs for additional message metadata.
        delay_in_seconds: Time in seconds to delay the message before it becomes available.
        expiration_in_seconds: Time in seconds after which the message expires.
        max_receive_count: Maximum number of receive attempts before moving to DLQ.

    Thread Safety:
        This class is **not** thread-safe. Create a new instance for
        each send operation. Do not share instances across threads or
        asyncio tasks.
        max_receive_queue: The queue where messages are moved after max receive attempts.

    Raises:
        KubeMQValidationError: When used with QueuesClient.send_queue_message, validation
            failures (empty channel, missing content, invalid delay/expiration, or
            max_receive_queue required but not set) are wrapped as KubeMQValidationError.

    See Also:
        QueueMessageReceived: The received counterpart when consuming from a queue.
        QueuesClient.send_queue_message: Send messages to a queue.
    """

    # Class attributes
    MAX_DELAY_SECONDS: ClassVar[int] = 43200  # 12 hours
    MAX_EXPIRATION_SECONDS: ClassVar[int] = 43200  # 12 hours

    # Required field first
    channel: str

    # Optional fields
    id: str | None = None
    metadata: str | None = None
    body: bytes = b""
    tags: dict[str, str] = field(default_factory=dict)
    delay_in_seconds: int = 0
    expiration_in_seconds: int = 0
    max_receive_count: int = 0
    max_receive_queue: str = ""

    def __post_init__(self) -> None:
        """Validate queue message fields."""
        if not self.channel:
            raise ValueError("Channel cannot be empty. Please provide a valid queue name.")
        validate_channel_name(self.channel)
        if self.delay_in_seconds < 0:
            raise ValueError("delay_in_seconds must be >= 0")
        if self.delay_in_seconds > self.MAX_DELAY_SECONDS:
            raise ValueError(f"Delay cannot exceed {self.MAX_DELAY_SECONDS} seconds (12 hours)")
        if self.expiration_in_seconds < 0:
            raise ValueError("expiration_in_seconds must be >= 0")
        if self.expiration_in_seconds > self.MAX_EXPIRATION_SECONDS:
            raise ValueError(
                f"Expiration cannot exceed {self.MAX_EXPIRATION_SECONDS} seconds (12 hours)"
            )
        if self.max_receive_count < 0:
            raise ValueError("max_receive_count must be >= 0")
        if not self.metadata and not self.body and not self.tags:
            raise ValueError(
                "Message must have at least one of the following: metadata, body, or tags. "
                "Empty messages are not allowed."
            )
        if self.max_receive_count > 0 and not self.max_receive_queue:
            raise ValueError(
                "When specifying max_receive_count, you must also provide a max_receive_queue."
            )

    # Encoding methods
    def encode(self, client_id: str) -> pbQueuesUpstreamRequest:
        """Encode the message to a QueuesUpstreamRequest protobuf object."""
        pb_queue_stream = pbQueuesUpstreamRequest()
        pb_queue_stream.RequestID = fast_id()
        pb_message = self.encode_message(client_id)
        pb_queue_stream.Messages.append(pb_message)
        return pb_queue_stream

    def encode_message(self, client_id: str) -> pbQueueMessage:
        """Encode the message to a QueueMessage protobuf object."""
        pb_queue = pbQueueMessage()
        pb_queue.MessageID = self.id or fast_id()
        pb_queue.ClientID = client_id
        pb_queue.Channel = self.channel
        pb_queue.Metadata = self.metadata or ""
        pb_queue.Body = self.body
        pb_queue.Tags.update(self.tags)
        pb_queue.Policy.DelaySeconds = self.delay_in_seconds
        pb_queue.Policy.ExpirationSeconds = self.expiration_in_seconds
        pb_queue.Policy.MaxReceiveCount = self.max_receive_count
        pb_queue.Policy.MaxReceiveQueue = self.max_receive_queue
        return pb_queue

    # Decoding methods
    @classmethod
    def decode(cls, pb_message: pbQueueMessage) -> QueueMessage:
        """Create a QueueMessage from a protobuf QueueMessage."""
        tags = {key: pb_message.Tags[key] for key in pb_message.Tags}

        return cls(
            id=pb_message.MessageID,
            channel=pb_message.Channel,
            metadata=pb_message.Metadata if pb_message.Metadata else None,
            body=pb_message.Body,
            tags=tags,
            delay_in_seconds=pb_message.Policy.DelaySeconds if pb_message.Policy else 0,
            expiration_in_seconds=pb_message.Policy.ExpirationSeconds if pb_message.Policy else 0,
            max_receive_count=pb_message.Policy.MaxReceiveCount if pb_message.Policy else 0,
            max_receive_queue=pb_message.Policy.MaxReceiveQueue if pb_message.Policy else "",
        )

    # Utility methods
    def with_updates(self, **kwargs: Any) -> QueueMessage:
        """Create a new QueueMessage with updated values."""
        return dataclasses.replace(self, **kwargs)

    def __str__(self) -> str:
        """Get a string representation of the message."""
        body_preview = self.body[:20].decode("utf-8", errors="replace") if self.body else ""
        if len(self.body) > 20:
            body_preview += "..."

        return (
            f"QueueMessage(id={self.id}, channel={self.channel}, "
            f"metadata={self.metadata}, body_preview='{body_preview}', "
            f"tags={self.tags}, delay={self.delay_in_seconds}s, "
            f"expiration={self.expiration_in_seconds}s)"
        )

    def __repr__(self) -> str:
        """Get a detailed representation of the message."""
        return (
            f"QueueMessage(id={self.id!r}, channel={self.channel!r}, "
            f"metadata={self.metadata!r}, body={self.body!r}, tags={self.tags!r}, "
            f"delay_in_seconds={self.delay_in_seconds}, "
            f"expiration_in_seconds={self.expiration_in_seconds}, "
            f"max_receive_count={self.max_receive_count}, "
            f"max_receive_queue={self.max_receive_queue!r})"
        )
