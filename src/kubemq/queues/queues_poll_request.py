from __future__ import annotations

import dataclasses
import uuid
from dataclasses import dataclass, field

from kubemq.common.channel_validators import validate_channel_name
from kubemq.grpc import QueuesDownstreamRequest, QueuesDownstreamRequestType


@dataclass(frozen=True)
class QueuesPollRequest:
    """Class representing a request to poll messages from a queue.

    Attributes:
        channel: The channel (queue name) to poll messages from.
        poll_max_messages: The maximum number of messages to poll in a single request.
        poll_wait_timeout_in_seconds: The maximum time to wait for messages in seconds.
        auto_ack_messages: Whether to automatically acknowledge received messages.
    """

    channel: str | None = None
    poll_max_messages: int = 1
    poll_wait_timeout_in_seconds: int = 60
    auto_ack_messages: bool = False
    metadata: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate poll request fields."""
        if not self.channel:
            raise ValueError(
                "Queue poll request must have a channel. Please provide a valid queue name."
            )
        validate_channel_name(self.channel)
        if not (1 <= self.poll_max_messages <= 1024):
            raise ValueError("poll_max_messages must be between 1 and 1024")
        if not (1 <= self.poll_wait_timeout_in_seconds <= 3600):
            raise ValueError("poll_wait_timeout_in_seconds must be between 1 and 3600")

    # Utility methods
    def with_updates(self, **kwargs: object) -> QueuesPollRequest:
        """Create a new poll request with updated values."""
        return dataclasses.replace(self, **kwargs)

    # Encoding methods
    def encode(self, client_id: str = "") -> QueuesDownstreamRequest:
        """Encode the poll request to a QueuesDownstreamRequest protobuf object."""
        request = QueuesDownstreamRequest()
        request.RequestID = str(uuid.uuid4())
        request.ClientID = client_id
        request.Channel = self.channel or ""
        request.MaxItems = self.poll_max_messages
        request.WaitTimeout = self.poll_wait_timeout_in_seconds * 1000
        request.AutoAck = self.auto_ack_messages
        request.RequestTypeData = QueuesDownstreamRequestType.Get
        if self.metadata:
            for k, v in self.metadata.items():
                request.Metadata[k] = v
        return request

    # String representations
    def __str__(self) -> str:
        """Get a string representation of the poll request."""
        return (
            f"QueuesPollRequest: channel={self.channel}, "
            f"poll_max_messages={self.poll_max_messages}, "
            f"poll_wait_timeout_in_seconds={self.poll_wait_timeout_in_seconds}, "
            f"auto_ack_messages={self.auto_ack_messages}"
        )

    def __repr__(self) -> str:
        """Get a detailed representation of the poll request."""
        return (
            f"QueuesPollRequest(channel={self.channel!r}, "
            f"poll_max_messages={self.poll_max_messages}, "
            f"poll_wait_timeout_in_seconds={self.poll_wait_timeout_in_seconds}, "
            f"auto_ack_messages={self.auto_ack_messages})"
        )
