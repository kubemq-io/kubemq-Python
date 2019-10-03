from builtins import input
from random import randint

from kubemq.events.subscriber import Subscriber
from kubemq.subscription.events_store_type import EventsStoreType
from kubemq.subscription.subscribe_request import SubscribeRequest
from kubemq.subscription.subscribe_type import SubscribeType


def create_subscribe_request(
        subscribe_type=SubscribeType.SubscribeTypeUndefined,
        events_store_type=EventsStoreType.Undefined,
        events_store_type_value=0):
    return SubscribeRequest(
        channel="MyTestChannelName",
        client_id=str(randint(9, 19999)),
        events_store_type=events_store_type,
        events_store_type_value=events_store_type_value,
        group="",
        subscribe_type=subscribe_type
    )


def handle_incoming_events(event):
    if event:
        print("Subscriber Received Event: Metadata:'%s', Channel:'%s', Body:'%s tags:%s'" % (
            event.metadata,
            event.channel,
            event.body,
            event.tags
        ))


if __name__ == "__main__":
    print("Subscribing to event on channel example")

    # Subscribe to events with store
    subscriber = Subscriber()
    subscribe_request = create_subscribe_request(SubscribeType.EventsStore, EventsStoreType.StartAtSequence, 2)
    subscriber.subscribe_to_events(subscribe_request, handle_incoming_events)

    # Subscribe to events without store
    subscriber = Subscriber()
    subscribe_request = create_subscribe_request(SubscribeType.Events)
    subscriber.subscribe_to_events(subscribe_request, handle_incoming_events)

    input("Press 'Enter' to stop the application...\n")
