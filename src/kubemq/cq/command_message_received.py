from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from kubemq.grpc import Request as pbRequest


@dataclass(frozen=True)
class CommandReceived:
    """Received command message from a subscription.

    Attributes:
        id: The unique ID of the command message.
        channel: The channel through which the command was received.
        metadata: Additional metadata associated with the command.
        body: The raw body of the command message.
        reply_channel: The channel to send the reply to.
        tags: Key-value tags associated with the command.
        from_client_id: The ID of the client that sent the command.
        timestamp: When the command was received.

    See Also:
        CommandMessage: The outgoing counterpart for sending commands.
        CQClient.subscribe_to_commands: Subscribe to receive commands.

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
    reply_channel: str = ""
    tags: dict[str, str] = field(default_factory=dict)

    @classmethod
    def decode(cls, command_receive: pbRequest) -> CommandReceived:
        """Decode a protobuf Request into a CommandReceived.

        Returns:
            A new CommandReceived instance populated from the protobuf message.
        """
        return cls(
            id=command_receive.RequestID,
            from_client_id=command_receive.ClientID,
            timestamp=datetime.now(),
            channel=command_receive.Channel,
            metadata=command_receive.Metadata,
            body=command_receive.Body,
            reply_channel=command_receive.ReplyChannel,
            tags=dict(command_receive.Tags),
        )

    def __repr__(self) -> str:
        return (
            f"CommandReceived: id={self.id}, channel={self.channel}, "
            f"metadata={self.metadata}, body={self.body!r}, "
            f"from_client_id={self.from_client_id}, timestamp={self.timestamp}, "
            f"reply_channel={self.reply_channel}, tags={self.tags}"
        )
