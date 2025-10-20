import asyncio
from pydantic import BaseModel, field_validator, ValidationError
from typing import Callable, Optional
from kubemq.grpc import Subscribe
from kubemq.common.subscribe_type import SubscribeType
from kubemq.cq.command_message_received import CommandMessageReceived


class CommandsSubscription(BaseModel):
    channel: str
    group: Optional[str] = None
    on_receive_command_callback: Callable[[CommandMessageReceived], None]
    on_error_callback: Optional[Callable[[str], None]] = None

    @field_validator("channel")
    def channel_must_exist(cls, v: str) -> str:
        if not v:
            raise ValueError("command subscription must have a channel.")
        return v

    @field_validator("on_receive_command_callback")
    def callback_must_exist(cls, v: Callable) -> Callable:
        if not callable(v):
            raise ValueError(
                "command subscription must have a on_receive_command_callback function."
            )
        return v

    def raise_on_receive_message(
        self, received_command: CommandMessageReceived
    ) -> None:
        self.on_receive_command_callback(received_command)

    async def raise_on_receive_message_async(
        self, received_command: CommandMessageReceived
    ) -> None:
        """Async-aware version that supports both sync and async callbacks."""
        if asyncio.iscoroutinefunction(self.on_receive_command_callback):
            await self.on_receive_command_callback(received_command)
        else:
            self.on_receive_command_callback(received_command)

    def raise_on_error(self, msg: str) -> None:
        if self.on_error_callback:
            self.on_error_callback(msg)

    async def raise_on_error_async(self, msg: str) -> None:
        """Async-aware version that supports both sync and async callbacks."""
        if self.on_error_callback:
            if asyncio.iscoroutinefunction(self.on_error_callback):
                await self.on_error_callback(msg)
            else:
                self.on_error_callback(msg)

    def decode(self, client_id: str = "") -> Subscribe:
        request = Subscribe()
        request.Channel = self.channel
        request.Group = self.group or ""
        request.ClientID = client_id
        request.SubscribeTypeData = SubscribeType.Commands.value
        return request

    def __repr__(self) -> str:
        return f"CommandsSubscription: channel={self.channel}, group={self.group}"

    model_config = {"arbitrary_types_allowed": True}

    @classmethod
    def create(
        cls,
        channel: str,
        group: Optional[str] = None,
        on_receive_command_callback: Callable[[CommandMessageReceived], None] = None,
        on_error_callback: Optional[Callable[[str], None]] = None,
    ) -> "CommandsSubscription":
        try:
            return cls(
                channel=channel,
                group=group,
                on_receive_command_callback=on_receive_command_callback,
                on_error_callback=on_error_callback,
            )
        except ValidationError as e:
            raise ValueError(str(e))
