from builtins import input
from random import randint
from kubemq.tools.listener_cancellation_token import ListenerCancellationToken
from kubemq.events.subscriber import Subscriber
from kubemq.subscription.subscribe_type import SubscribeType
from kubemq.subscription.events_store_type import EventsStoreType
from kubemq.subscription.subscribe_request import SubscribeRequest


def create_subscribe_request(
        subscribe_type=SubscribeType.Events,
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

def handle_incoming_error(error_msg):
        print("received error:%s'" % (
            error_msg
        ))


if __name__ == "__main__":
    print("Subscribing to event on channel example")
    cancel_token=ListenerCancellationToken()
    # Subscribe to events with store
    subscriber = Subscriber("localhost:50000")
    subscribe_request = create_subscribe_request(SubscribeType.EventsStore, EventsStoreType.StartAtSequence, 2)
    subscriber.subscribe_to_events(subscribe_request, handle_incoming_events,handle_incoming_error,cancel_token)

    # Subscribe to events without store
    subscriber = Subscriber("localhost:50000")
    subscribe_request = create_subscribe_request(SubscribeType.Events)
    subscriber.subscribe_to_events(subscribe_request, handle_incoming_events,handle_incoming_error,cancel_token)
    
    input("Press 'Enter' to stop Listen...\n")
    cancel_token.cancel()
    input("Press 'Enter' to stop the application...\n")
