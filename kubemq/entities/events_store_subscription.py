import datetime
from typing import Callable
from kubemq.grpc import Subscribe
from kubemq.subscription.subscribe_type import SubscribeType
from kubemq.entities.event_store_message_received import EventStoreMessageMessage

from enum import Enum


class EventsStoreType(Enum):
    Undefined = 0
    StartNewOnly = 1
    StartFromFirst = 2
    StartFromLast = 3
    StartAtSequence = 4
    StartAtTime = 5
    StartAtTimeDelta = 6


class EventsStoreSubscription:
    def __init__(self,
                 channel: str = None,
                 group: str = None,
                 events_store_type: EventsStoreType = EventsStoreType.Undefined,
                 events_store_sequence_value: int = 0,
                 events_store_start_time: datetime = None,
                 on_receive_event_callback: Callable[[EventStoreMessageMessage], None] = None,
                 on_error_callback: Callable[[str], None] = None):
        self._channel: str = channel
        self._group: str = group
        self._events_store_type: EventsStoreType = events_store_type
        self._events_store_sequence_value: int = events_store_sequence_value
        self._events_store_start_time: datetime = events_store_start_time
        self._on_receive_event_callback = on_receive_event_callback
        self._on_error_callback = on_error_callback

    @property
    def channel(self) -> str:
        return self._channel

    @property
    def group(self) -> str:
        return self._group

    @property
    def events_store_type(self) -> EventsStoreType:
        return self._events_store_type

    @property
    def events_store_sequence_value(self) -> int:
        return self._events_store_sequence_value

    @property
    def events_store_start_time(self) -> datetime:
        return self._events_store_start_time

    @property
    def on_receive_event_callback(self) -> Callable[[EventStoreMessageMessage], None]:
        return self._on_receive_event_callback

    @property
    def on_error_callback(self) -> Callable[[str], None]:
        return self._on_error_callback

    def raise_on_receive_message(self, received_event: EventStoreMessageMessage):
        if self._on_receive_event_callback:
            self._on_receive_event_callback(received_event)

    def raise_on_error(self, msg: str):
        if self._on_error_callback:
            self._on_error_callback(msg)

    def _validate(self):
        if not self._channel:
            raise ValueError("Event Store subscription must have a channel.")
        if not self._on_receive_event_callback:
            raise ValueError("Event Store subscription must have a on_receive_event_callback function.")
        if self._events_store_type == EventsStoreType.Undefined:
            raise ValueError("Event Store subscription must have an events store type.")
        if self._events_store_type == EventsStoreType.StartAtSequence and self._events_store_sequence_value == 0:
            raise ValueError("Event Store subscription with StartAtSequence events store type must have a sequence value.")
        if self._events_store_type == EventsStoreType.StartAtTime and not self._events_store_start_time:
            raise ValueError("Event Store subscription with StartAtTime events store type must have a start time.")

    def _to_subscribe_request(self, client_id: str = "") -> Subscribe:
        request = Subscribe()
        request.Channel = self._channel
        request.Group = self._group
        if self._events_store_type == EventsStoreType.StartNewOnly:
            request.EventsStoreTypeData = EventsStoreType.StartNewOnly.value
        elif self._events_store_type == EventsStoreType.StartFromFirst:
            request.EventsStoreTypeData = EventsStoreType.StartFromFirst.value
        elif self._events_store_type == EventsStoreType.StartFromLast:
            request.EventsStoreTypeData = EventsStoreType.StartFromLast.value
        elif self._events_store_type == EventsStoreType.StartAtSequence:
            request.EventsStoreTypeData = self._events_store_sequence_value
            request.EventsStoreTypeValue = self._events_store_sequence_value
        elif self._events_store_type == EventsStoreType.StartAtTime:
            request.EventsStoreTypeData = EventsStoreType.StartAtTime.value
            request.EventsStoreTypeValue = int(self._events_store_start_time.timestamp())
        request.ClientID = client_id
        request.SubscribeTypeData = SubscribeType.EventsStore.value
        return request

    def __repr__(self):
        return f"EventsStoreSubscription: channel={self._channel}, group={self._group}"