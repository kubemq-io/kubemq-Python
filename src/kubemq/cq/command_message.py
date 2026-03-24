from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from typing import Any, Self

from kubemq.common.channel_validators import validate_channel_name
from kubemq.common.helpers import fast_id
from kubemq.grpc import Request as pbCommand


@dataclass(frozen=True)
class CommandMessage:
    """A command message for request-response patterns.

    Instances are immutable after construction. Use ``with_updates()``
    to create modified copies.

    Raises:
        KubeMQValidationError: If channel is missing or empty, or if all of
            metadata, body, and tags are empty (at least one must be set).

    See Also:
        CommandReceived: The received counterpart when subscribing.
        CQClient.send_command: Send commands to a channel.

    Thread Safety:
        Instances are immutable (frozen) and safe to read from multiple
        threads. However, reusing the same instance for multiple send
        operations is not recommended — create a new instance per send
        to ensure unique message IDs.
    """

    channel: str
    timeout_in_seconds: int
    id: str | None = field(default_factory=fast_id)
    metadata: str | None = None
    body: bytes = b""
    tags: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate command message fields."""
        if not self.channel:
            raise ValueError("Command message must have a channel.")
        validate_channel_name(self.channel)
        if self.timeout_in_seconds <= 0:
            raise ValueError("timeout_in_seconds must be greater than 0")
        if not self.metadata and not self.body and not self.tags:
            raise ValueError(
                "Command message must have at least one of the following: metadata, body, or tags."
            )

    def encode(self, client_id: str, *, span: bytes = b"") -> pbCommand:
        """Encode the command message to a protobuf Request.

        Returns:
            The protobuf Request ready for transmission.
        """
        pb_command = pbCommand()
        pb_command.RequestID = self.id or fast_id()
        pb_command.ClientID = client_id
        pb_command.Channel = self.channel
        pb_command.Metadata = self.metadata or ""
        pb_command.Body = self.body
        pb_command.Timeout = self.timeout_in_seconds * 1000
        pb_command.RequestTypeData = pbCommand.RequestType.Command
        for key, value in self.tags.items():
            pb_command.Tags[key] = value
        if span:
            pb_command.Span = span
        return pb_command

    def with_updates(self, **kwargs: Any) -> Self:
        """Create a new message with updated values.

        Returns:
            A new CommandMessage with the specified fields replaced.
        """
        return dataclasses.replace(self, **kwargs)

    def __repr__(self) -> str:
        return (
            f"CommandMessage: id={self.id}, channel={self.channel}, "
            f"metadata={self.metadata}, body={self.body!r}, tags={self.tags}, "
            f"timeout_in_seconds={self.timeout_in_seconds}"
        )
