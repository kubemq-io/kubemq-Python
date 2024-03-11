import datetime
from typing import Callable
from kubemq.grpc import Subscribe
from kubemq.common.subscribe_type import SubscribeType
from kubemq.pubsub import EventStoreMessageReceived

from enum import Enum


class EventsStoreType(Enum):
    """
    Represents the type of event store.

    Enum Values:
    - Undefined: 0
    - StartNewOnly: 1
    - StartFromFirst: 2
    - StartFromLast: 3
    - StartAtSequence: 4
    - StartAtTime: 5
    - StartAtTimeDelta: 6
    """
    Undefined = 0
    StartNewOnly = 1
    StartFromFirst = 2
    StartFromLast = 3
    StartAtSequence = 4
    StartAtTime = 5
    StartAtTimeDelta = 6


class EventsStoreSubscription:
    """
    Class representing a subscription to an events store.

    Parameters:
    - channel (str): The channel to subscribe to.
    - group (str): The group to subscribe to.
    - events_store_type (EventsStoreType): The type of events store.
    - events_store_sequence_value (int): The sequence value of the events store.
    - events_store_start_time (datetime): The start time of the events store.
    - on_receive_event_callback (Callable[[EventStoreMessageReceived], None]): Callback function to handle received events.
    - on_error_callback (Callable[[str], None]): Callback function to handle errors.

    Methods:
    - raise_on_receive_message(received_event: EventStoreMessageReceived): Raises the on_receive_event_callback with the received event.
    - raise_on_error(msg: str): Raises the on_error_callback with the given error message.
    - validate(): Validates the subscription parameters.
    - encode(client_id: str = "") -> Subscribe: Encodes the subscription into a Subscribe request.
    - __repr__(): Returns a string representation of the subscription.

    """
    def __init__(self,
                 channel: str = None,
                 group: str = None,
                 events_store_type: EventsStoreType = EventsStoreType.Undefined,
                 events_store_sequence_value: int = 0,
                 events_store_start_time: datetime = None,
                 on_receive_event_callback: Callable[[EventStoreMessageReceived], None] = None,
                 on_error_callback: Callable[[str], None] = None):
        self.channel: str = channel
        self.group: str = group
        self.events_store_type: EventsStoreType = events_store_type
        self.events_store_sequence_value: int = events_store_sequence_value
        self.events_store_start_time: datetime = events_store_start_time
        self.on_receive_event_callback = on_receive_event_callback
        self.on_error_callback = on_error_callback

    def raise_on_receive_message(self, received_event: EventStoreMessageReceived):
        if self.on_receive_event_callback:
            self.on_receive_event_callback(received_event)

    def raise_on_error(self, msg: str):
        if self.on_error_callback:
            self.on_error_callback(msg)

    def validate(self):
        if not self.channel:
            raise ValueError("Event Store subscription must have a channel.")
        if not self.on_receive_event_callback:
            raise ValueError("Event Store subscription must have a on_receive_event_callback function.")
        if self.events_store_type == EventsStoreType.Undefined:
            raise ValueError("Event Store subscription must have an events store type.")
        if self.events_store_type == EventsStoreType.StartAtSequence and self.events_store_sequence_value == 0:
            raise ValueError("Event Store subscription with StartAtSequence events store type must have a sequence value.")
        if self.events_store_type == EventsStoreType.StartAtTime and not self.events_store_start_time:
            raise ValueError("Event Store subscription with StartAtTime events store type must have a start time.")

    def encode(self, client_id: str = "") -> Subscribe:
        request = Subscribe()
        request.Channel = self.channel
        request.Group = self.group
        if self.events_store_type == EventsStoreType.StartNewOnly:
            request.EventsStoreTypeData = EventsStoreType.StartNewOnly.value
        elif self.events_store_type == EventsStoreType.StartFromFirst:
            request.EventsStoreTypeData = EventsStoreType.StartFromFirst.value
        elif self.events_store_type == EventsStoreType.StartFromLast:
            request.EventsStoreTypeData = EventsStoreType.StartFromLast.value
        elif self.events_store_type == EventsStoreType.StartAtSequence:
            request.EventsStoreTypeData = self.events_store_sequence_value
            request.EventsStoreTypeValue = self.events_store_sequence_value
        elif self.events_store_type == EventsStoreType.StartAtTime:
            request.EventsStoreTypeData = EventsStoreType.StartAtTime.value
            request.EventsStoreTypeValue = int(self.events_store_start_time.timestamp())
        request.ClientID = client_id
        request.SubscribeTypeData = SubscribeType.EventsStore.value
        return request

    def __repr__(self):
        return f"EventsStoreSubscription: channel={self.channel}, group={self.group}, events_store_type={self.events_store_type.name}, events_store_sequence_value={self.events_store_sequence_value}, events_store_start_time={self.events_store_start_time}"