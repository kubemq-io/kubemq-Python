from typing import Callable
from kubemq.grpc import Subscribe
from kubemq.common.subscribe_type import SubscribeType
from kubemq.pubsub import EventMessageReceived


class EventsSubscription:
    """
    Class representing an event subscription.

    Args:
        channel (str, optional): The channel to subscribe to. Defaults to None.
        group (str, optional): The group to subscribe to. Defaults to None.
        on_receive_event_callback (Callable[[EventMessageReceived], None], optional):
            The callback function to be called when an event is received. Defaults to None.
        on_error_callback (Callable[[str], None], optional):
            The callback function to be called when an error occurs. Defaults to None.

    Attributes:
        channel (str): The channel to subscribe to.
        group (str): The group to subscribe to.
        on_receive_event_callback (Callable[[EventMessageReceived], None]):
            The callback function to be called when an event is received.
        on_error_callback (Callable[[str], None]):
            The callback function to be called when an error occurs.

    Methods:
        raise_on_receive_message(received_event: EventMessageReceived):
            Raises the on_receive_event_callback with the received event if it is defined.
        raise_on_error(msg: str):
            Raises the on_error_callback with the specified error message if it is defined.
        validate():
            Validates that the event subscription has a channel and an on_receive_event_callback function.
        encode(client_id: str = "") -> Subscribe:
            Encodes the event subscription into a Subscribe object.
        __repr__() -> str:
            Returns a string representation of the EventsSubscription.

    Raises:
        ValueError: If the event subscription does not have a channel or an on_receive_event_callback function.
    """
    def __init__(self, channel: str = None,
                 group: str = None,
                 on_receive_event_callback: Callable[[EventMessageReceived], None] = None,
                 on_error_callback: Callable[[str], None] = None):
        self.channel: str = channel
        self.group: str = group
        self.on_receive_event_callback = on_receive_event_callback
        self.on_error_callback = on_error_callback



    def raise_on_receive_message(self, received_event: EventMessageReceived):
        if self.on_receive_event_callback:
            self.on_receive_event_callback(received_event)

    def raise_on_error(self, msg: str):
        if self.on_error_callback:
            self.on_error_callback(msg)

    def validate(self):
        if not self.channel:
            raise ValueError("Event subscription must have a channel.")
        if not self.on_receive_event_callback:
            raise ValueError("Event subscription must have a on_receive_event_callback function.")

    def encode(self, client_id: str = "") -> Subscribe:
        request = Subscribe()
        request.Channel = self.channel
        request.Group = self.group
        request.ClientID = client_id
        request.SubscribeTypeData = SubscribeType.Events.value
        return request

    def __repr__(self):
        return f"EventsSubscription: channel={self._channel}, group={self._group}"