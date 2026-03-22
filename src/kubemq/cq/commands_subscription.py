import asyncio
from collections.abc import Callable
from dataclasses import dataclass

from kubemq.common.channel_validators import validate_channel_name
from kubemq.common.subscribe_type import SubscribeType
from kubemq.cq.command_message_received import CommandReceived
from kubemq.grpc import Subscribe


@dataclass
class CommandsSubscription:
    """Subscription configuration for commands."""

    channel: str
    on_receive_command_callback: Callable[[CommandReceived], None]
    group: str | None = None
    on_error_callback: Callable[[str], None] | None = None

    def __post_init__(self) -> None:
        """Validate subscription fields."""
        if not self.channel:
            raise ValueError("command subscription must have a channel.")
        validate_channel_name(self.channel)
        if not callable(self.on_receive_command_callback):
            raise ValueError(
                "command subscription must have a on_receive_command_callback function."
            )

    def raise_on_receive_message(self, received_command: CommandReceived) -> None:
        """Dispatch the received command to the callback."""
        self.on_receive_command_callback(received_command)

    async def raise_on_receive_message_async(self, received_command: CommandReceived) -> None:
        """Async-aware version that supports both sync and async callbacks."""
        if asyncio.iscoroutinefunction(self.on_receive_command_callback):
            await self.on_receive_command_callback(received_command)
        else:
            self.on_receive_command_callback(received_command)

    def raise_on_error(self, msg: str) -> None:
        """Dispatch the error message to the error callback."""
        if self.on_error_callback:
            self.on_error_callback(msg)

    async def raise_on_error_async(self, msg: str) -> None:
        """Async-aware version that supports both sync and async callbacks."""
        if self.on_error_callback:
            if asyncio.iscoroutinefunction(self.on_error_callback):
                await self.on_error_callback(msg)
            else:
                self.on_error_callback(msg)

    def encode(self, client_id: str = "") -> Subscribe:
        """Convert subscription to gRPC Subscribe message."""
        request = Subscribe()
        request.Channel = self.channel
        request.Group = self.group or ""
        request.ClientID = client_id
        request.SubscribeTypeData = SubscribeType.Commands.value  # type: ignore[assignment]
        return request

    def decode(self, client_id: str = "") -> Subscribe:
        """Deprecated: Use encode() instead."""
        import warnings

        warnings.warn(
            "decode() is deprecated, use encode() instead", DeprecationWarning, stacklevel=2
        )
        return self.encode(client_id)

    def __repr__(self) -> str:
        return f"CommandsSubscription: channel={self.channel}, group={self.group}"

    @classmethod
    def create(
        cls,
        channel: str,
        group: str | None = None,
        on_receive_command_callback: Callable[[CommandReceived], None] | None = None,
        on_error_callback: Callable[[str], None] | None = None,
    ) -> "CommandsSubscription":
        """Create a CommandsSubscription with validation."""
        if on_receive_command_callback is None:
            raise ValueError("on_receive_command_callback is required")
        return cls(
            channel=channel,
            group=group,
            on_receive_command_callback=on_receive_command_callback,
            on_error_callback=on_error_callback,
        )
