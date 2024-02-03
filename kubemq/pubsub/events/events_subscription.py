from typing import Callable
from kubemq.grpc import Subscribe
from kubemq.subscription.subscribe_type import SubscribeType
from kubemq.pubsub.events.event_received import EventReceived


class EventsSubscription:

    def __init__(self, channel: str = None, group: str = None,
                 on_receive_event_callback=Callable[[EventReceived], None], on_error_callback=Callable[[str], None]):
        self._channel: str = channel
        self._group: str = group
        self._on_receive_event_callback = on_receive_event_callback if on_receive_event_callback else None
        self._on_error_callback = on_error_callback if on_error_callback else None

    @property
    def channel(self) -> str:
        return self._channel

    @property
    def group(self) -> str:
        return self._group

    def set_channel(self, value: str) -> 'EventsSubscription':
        self._channel = value
        return self

    def set_group(self, value: str) -> 'EventsSubscription':
        self._group = value
        return self

    def add_on_receive_event_callback(self, callback: Callable[[EventReceived], None]) -> 'EventsSubscription':
        self._on_receive_event_callback = callback
        return self

    def add_on_error_callback(self, callback: Callable[[str], None]) -> 'EventsSubscription':
        self._on_error_callback = callback
        return self

    def raise_on_receive_event(self, received_event: EventReceived):
        if self._on_receive_event_callback:
            self._on_receive_event_callback(received_event)

    def raise_on_error(self, msg: str):
        if self._on_error_callback:
            self._on_error_callback(msg)

    def validate(self):
        if not self._channel:
            raise ValueError("Event subscription must have a channel.")
        if not self._on_receive_event_callback:
            raise ValueError("Event subscription must have an OnReceiveEvent callback function.")

    def to_subscribe_request(self, client_id: str = "") -> Subscribe:
        request = Subscribe()
        request.Channel = self._channel
        request.Group = self._group
        request.ClientID = client_id
        request.SubscribeTypeData = SubscribeType.Events.value
        return request
