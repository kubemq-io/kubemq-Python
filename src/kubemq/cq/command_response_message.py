from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from kubemq.cq.command_message_received import CommandReceived
from kubemq.grpc import Response as pbResponse


@dataclass
class CommandResponse:
    """Response message for a command request."""

    command_received: CommandReceived | None = None
    client_id: str = ""
    request_id: str = ""
    is_executed: bool = False
    timestamp: datetime = field(default_factory=datetime.now)
    error: str = ""
    metadata: str | None = None
    body: bytes = b""
    tags: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate command response when command_received is provided."""
        if self.command_received is not None and self.command_received.reply_channel == "":
            raise ValueError("Command response must have a reply channel.")

    @classmethod
    def decode(cls, pb_response: pbResponse) -> CommandResponse:
        """Decode a protobuf Response into a CommandResponse."""
        return cls(
            client_id=pb_response.ClientID,
            request_id=pb_response.RequestID,
            is_executed=pb_response.Executed,
            error=pb_response.Error,
            timestamp=datetime.fromtimestamp(pb_response.Timestamp / 1e9),
            metadata=pb_response.Metadata,
            body=pb_response.Body,
            tags=dict(pb_response.Tags),
        )

    def encode(self, client_id: str) -> pbResponse:
        """Encode the response message to a protobuf Response.

        Returns:
            The protobuf Response ready for transmission.
        """
        if not self.command_received:
            raise ValueError("Command received is required for encoding.")
        if not self.command_received.id or self.command_received.id.strip() == "":
            raise ValueError("RequestID cannot be empty")
        pb_response = pbResponse()
        pb_response.ClientID = client_id
        pb_response.RequestID = self.command_received.id
        pb_response.ReplyChannel = self.command_received.reply_channel
        pb_response.Executed = self.is_executed
        pb_response.Error = self.error
        pb_response.Timestamp = int(self.timestamp.timestamp() * 1e9)
        pb_response.Metadata = self.metadata or ""
        pb_response.Body = self.body or b""
        for key, value in self.tags.items():
            pb_response.Tags[key] = value
        return pb_response

    def __repr__(self) -> str:
        return (
            f"CommandResponse: client_id={self.client_id}, "
            f"request_id={self.request_id}, is_executed={self.is_executed}, "
            f"error={self.error}, timestamp={self.timestamp}"
        )
