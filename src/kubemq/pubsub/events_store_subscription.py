import asyncio
from collections.abc import Callable
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator

from kubemq.common.channel_validators import validate_channel_name
from kubemq.common.subscribe_type import SubscribeType
from kubemq.grpc import Subscribe
from kubemq.pubsub.event_store_message_received import EventStoreMessageReceived


class EventsStoreType(Enum):
    """Type of events store subscription start position."""

    Undefined = 0
    StartNewOnly = 1
    StartFromFirst = 2
    StartFromLast = 3
    StartAtSequence = 4
    StartAtTime = 5
    StartAtTimeDelta = 6


class EventsStoreSubscription(BaseModel):
    """Subscription configuration for events store."""

    channel: str
    group: str | None = None
    events_store_type: EventsStoreType = EventsStoreType.Undefined
    events_store_sequence_value: int = 0
    events_store_start_time: datetime | None = None
    events_store_time_delta_seconds: int = 0
    on_receive_event_callback: Callable[[EventStoreMessageReceived], None]
    on_error_callback: Callable[[str], None] | None = None

    @field_validator("channel")
    def channel_must_exist(cls, v: str) -> str:
        """Validate that the channel is not empty."""
        if not v:
            raise ValueError("Event Store subscription must have a channel.")
        validate_channel_name(v)
        return v

    @field_validator("events_store_type")
    def events_store_type_must_be_defined(cls, v: EventsStoreType) -> EventsStoreType:
        """Validate that the events store type is defined."""
        if v == EventsStoreType.Undefined:
            raise ValueError("Event Store subscription must have an events store type.")
        return v

    @field_validator("events_store_sequence_value")
    def validate_sequence_value(cls, v: int, info: Any) -> int:
        """Validate sequence value for StartAtSequence type."""
        if (
            "events_store_type" in info.data
            and info.data["events_store_type"] == EventsStoreType.StartAtSequence
            and v == 0
        ):
            raise ValueError(
                "Event Store subscription with StartAtSequence events store type must have a sequence value."
            )
        return v

    @field_validator("events_store_start_time")
    def validate_start_time(cls, v: Any, info: Any) -> Any:
        """Validate start time for StartAtTime type."""
        if (
            "events_store_type" in info.data
            and info.data["events_store_type"] == EventsStoreType.StartAtTime
            and v is None
        ):
            raise ValueError(
                "Event Store subscription with StartAtTime events store type must have a start time."
            )
        return v

    @field_validator("events_store_time_delta_seconds")
    def validate_time_delta(cls, v: int, info: Any) -> int:
        """Validate time delta for StartAtTimeDelta type."""
        if (
            "events_store_type" in info.data
            and info.data["events_store_type"] == EventsStoreType.StartAtTimeDelta
            and v <= 0
        ):
            raise ValueError(
                "Event Store subscription with StartAtTimeDelta events store type must have a time delta value > 0."
            )
        return v

    def raise_on_receive_message(self, received_event: EventStoreMessageReceived) -> None:
        """Dispatch the received event to the callback."""
        if self.on_receive_event_callback:  # type: ignore[truthy-function]
            self.on_receive_event_callback(received_event)

    async def raise_on_receive_message_async(
        self, received_event: EventStoreMessageReceived
    ) -> None:
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
        request.EventsStoreTypeData = self.events_store_type.value  # type: ignore[assignment]

        if self.events_store_type == EventsStoreType.StartAtSequence:
            request.EventsStoreTypeValue = self.events_store_sequence_value
        elif self.events_store_type == EventsStoreType.StartAtTime:
            request.EventsStoreTypeValue = int(self.events_store_start_time.timestamp())  # type: ignore[union-attr]
        elif self.events_store_type == EventsStoreType.StartAtTimeDelta:
            request.EventsStoreTypeValue = self.events_store_time_delta_seconds

        request.ClientID = client_id
        request.SubscribeTypeData = SubscribeType.EventsStore.value  # type: ignore[assignment]
        return request

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def model_dump(self, **kwargs: Any) -> dict[str, Any]:
        """Serialize the model to a dictionary with formatted fields."""
        dump = super().model_dump(**kwargs)
        dump["events_store_type"] = self.events_store_type.name
        if self.events_store_start_time:
            dump["events_store_start_time"] = self.events_store_start_time.isoformat()
        # Remove callback functions from the dump
        dump.pop("on_receive_event_callback", None)
        dump.pop("on_error_callback", None)
        return dump
