from datetime import datetime
from typing import Callable, Optional
from enum import Enum
from pydantic import BaseModel, field_validator
from kubemq.grpc import Subscribe
from kubemq.common.subscribe_type import SubscribeType
from kubemq.pubsub import EventStoreMessageReceived


class EventsStoreType(Enum):
    Undefined = 0
    StartNewOnly = 1
    StartFromFirst = 2
    StartFromLast = 3
    StartAtSequence = 4
    StartAtTime = 5
    StartAtTimeDelta = 6


class EventsStoreSubscription(BaseModel):
    channel: str
    group: Optional[str] = None
    events_store_type: EventsStoreType = EventsStoreType.Undefined
    events_store_sequence_value: int = 0
    events_store_start_time: Optional[datetime] = None
    on_receive_event_callback: Callable[[EventStoreMessageReceived], None]
    on_error_callback: Optional[Callable[[str], None]] = None

    @field_validator("channel")
    def channel_must_exist(cls, v):
        if not v:
            raise ValueError("Event Store subscription must have a channel.")
        return v

    @field_validator("events_store_type")
    def events_store_type_must_be_defined(cls, v):
        if v == EventsStoreType.Undefined:
            raise ValueError("Event Store subscription must have an events store type.")
        return v

    @field_validator("events_store_sequence_value")
    def validate_sequence_value(cls, v, values):
        if (
            "events_store_type" in values
            and values["events_store_type"] == EventsStoreType.StartAtSequence
            and v == 0
        ):
            raise ValueError(
                "Event Store subscription with StartAtSequence events store type must have a sequence value."
            )
        return v

    @field_validator("events_store_start_time")
    def validate_start_time(cls, v, values):
        if (
            "events_store_type" in values
            and values["events_store_type"] == EventsStoreType.StartAtTime
            and v is None
        ):
            raise ValueError(
                "Event Store subscription with StartAtTime events store type must have a start time."
            )
        return v

    def raise_on_receive_message(self, received_event: EventStoreMessageReceived):
        if self.on_receive_event_callback:
            self.on_receive_event_callback(received_event)

    def raise_on_error(self, msg: str):
        if self.on_error_callback:
            self.on_error_callback(msg)

    def encode(self, client_id: str = "") -> Subscribe:
        request = Subscribe()
        request.Channel = self.channel
        request.Group = self.group or ""
        request.EventsStoreTypeData = self.events_store_type.value

        if self.events_store_type == EventsStoreType.StartAtSequence:
            request.EventsStoreTypeValue = self.events_store_sequence_value
        elif self.events_store_type == EventsStoreType.StartAtTime:
            request.EventsStoreTypeValue = int(self.events_store_start_time.timestamp())

        request.ClientID = client_id
        request.SubscribeTypeData = SubscribeType.EventsStore.value
        return request

    class Config:
        arbitrary_types_allowed = True

    def model_dump(self, **kwargs):
        dump = super().model_dump(**kwargs)
        dump["events_store_type"] = self.events_store_type.name
        if self.events_store_start_time:
            dump["events_store_start_time"] = self.events_store_start_time.isoformat()
        # Remove callback functions from the dump
        dump.pop("on_receive_event_callback", None)
        dump.pop("on_error_callback", None)
        return dump
