import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from kubemq.common.channel_validators import validate_channel_name
from kubemq.common.subscribe_type import SubscribeType
from kubemq.grpc import Subscribe
from kubemq.pubsub.event_message_received import EventReceived


@dataclass
class EventsSubscription:
    """Subscription configuration for events."""

    channel: str
    on_receive_event_callback: Callable[[EventReceived], None]
    group: str | None = None
    on_error_callback: Callable[[str], None] | None = None

    def __post_init__(self) -> None:
        """Validate subscription fields."""
        if not self.channel:
            raise ValueError("Event subscription must have a channel.")
        validate_channel_name(self.channel, allow_wildcards=True)

    def raise_on_receive_message(self, received_event: EventReceived) -> None:
        """Dispatch the received event to the callback."""
        if self.on_receive_event_callback:  # type: ignore[truthy-function]
            self.on_receive_event_callback(received_event)

    async def raise_on_receive_message_async(self, received_event: EventReceived) -> None:
        """Async-aware version that supports both sync and async callbacks."""
        if self.on_receive_event_callback:  # type: ignore[truthy-function]
            if asyncio.iscoroutinefunction(self.on_receive_event_callback):
                await self.on_receive_event_callback(received_event)
            else:
                self.on_receive_event_callback(received_event)

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
        """Encode the subscription to a protobuf Subscribe message."""
        request = Subscribe()
        request.Channel = self.channel
        request.Group = self.group or ""
        request.ClientID = client_id
        request.SubscribeTypeData = SubscribeType.Events.value  # type: ignore[assignment]
        return request

    def to_dict(self, **kwargs: Any) -> dict[str, Any]:
        """Serialize the model to a dictionary excluding callbacks."""
        return {
            "channel": self.channel,
            "group": self.group,
        }
