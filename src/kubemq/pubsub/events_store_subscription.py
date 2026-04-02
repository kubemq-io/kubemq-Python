import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any

from kubemq.common.channel_validators import validate_channel_name
from kubemq.common.subscribe_type import SubscribeType
from kubemq.grpc import Subscribe
from kubemq.pubsub.event_store_message_received import EventStoreReceived


class EventStoreStartPosition(Enum):
    """Type of events store subscription start position."""

    Undefined = 0
    StartFromNew = 1
    StartFromFirst = 2
    StartFromLast = 3
    StartAtSequence = 4
    StartAtTime = 5
    StartAtTimeDelta = 6


@dataclass
class EventsStoreSubscription:
    """Subscription configuration for events store."""

    channel: str
    on_receive_event_callback: Callable[[EventStoreReceived], None]
    group: str | None = None
    events_store_type: EventStoreStartPosition = EventStoreStartPosition.Undefined
    events_store_sequence_value: int = 0
    events_store_start_time: datetime | None = None
    events_store_time_delta_seconds: int = 0
    on_error_callback: Callable[[str], None] | None = None

    def __post_init__(self) -> None:
        """Validate subscription fields."""
        if not self.channel:
            raise ValueError("Event Store subscription must have a channel.")
        validate_channel_name(self.channel)

    def validate(self) -> None:
        """Validate subscription configuration before use.

        Raises:
            ValueError: If events_store_type is Undefined or required fields
                are missing for the selected start position.
        """
        if self.events_store_type == EventStoreStartPosition.Undefined:
            raise ValueError("Event Store subscription must have an events store type.")
        if (
            self.events_store_type == EventStoreStartPosition.StartAtSequence
            and self.events_store_sequence_value == 0
        ):
            raise ValueError(
                "Event Store subscription with StartAtSequence events store type must have a sequence value."
            )
        if (
            self.events_store_type == EventStoreStartPosition.StartAtTime
            and self.events_store_start_time is None
        ):
            raise ValueError(
                "Event Store subscription with StartAtTime events store type must have a start time."
            )
        if (
            self.events_store_type == EventStoreStartPosition.StartAtTimeDelta
            and self.events_store_time_delta_seconds <= 0
        ):
            raise ValueError(
                "Event Store subscription with StartAtTimeDelta events store type must have a time delta value > 0."
            )

    def raise_on_receive_message(self, received_event: EventStoreReceived) -> None:
        """Dispatch the received event to the callback."""
        if self.on_receive_event_callback:  # type: ignore[truthy-function]
            self.on_receive_event_callback(received_event)

    async def raise_on_receive_message_async(self, received_event: EventStoreReceived) -> None:
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
        self.validate()
        request = Subscribe()
        request.Channel = self.channel
        request.Group = self.group or ""
        request.EventsStoreTypeData = self.events_store_type.value  # type: ignore[assignment]

        if self.events_store_type == EventStoreStartPosition.StartAtSequence:
            request.EventsStoreTypeValue = self.events_store_sequence_value
        elif self.events_store_type == EventStoreStartPosition.StartAtTime:
            request.EventsStoreTypeValue = int(self.events_store_start_time.timestamp())  # type: ignore[union-attr]
        elif self.events_store_type == EventStoreStartPosition.StartAtTimeDelta:
            request.EventsStoreTypeValue = self.events_store_time_delta_seconds

        request.ClientID = client_id
        request.SubscribeTypeData = SubscribeType.EventsStore.value  # type: ignore[assignment]
        return request

    def to_dict(self, **kwargs: Any) -> dict[str, Any]:
        """Serialize the model to a dictionary with formatted fields."""
        d: dict[str, Any] = {
            "channel": self.channel,
            "group": self.group,
            "events_store_type": self.events_store_type.name,
            "events_store_sequence_value": self.events_store_sequence_value,
            "events_store_start_time": (
                self.events_store_start_time.isoformat() if self.events_store_start_time else None
            ),
            "events_store_time_delta_seconds": self.events_store_time_delta_seconds,
        }
        return d
