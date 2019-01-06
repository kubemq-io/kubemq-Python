import datetime
from builtins import input
from random import randint

from kubemq.commandquery.responder import Responder
from kubemq.commandquery.response import Response
from kubemq.subscription.events_store_type import EventsStoreType
from kubemq.subscription.subscribe_request import SubscribeRequest
from kubemq.subscription.subscribe_type import SubscribeType

client_id = str(randint(9, 19999))


def create_subscribe_request(
        subscribe_type=SubscribeType.SubscribeTypeUndefined,
        events_store_type=EventsStoreType.Undefined,
        events_store_type_value=0):
    return SubscribeRequest(
        channel="MyTestChannelName",
        client_id=client_id,
        events_store_type=events_store_type,
        events_store_type_value=events_store_type_value,
        group="",
        subscribe_type=subscribe_type
    )


def handle_incoming_request(request):
    if request:
        print("Subscriber Received request: Metadata:'%s', Channel:'%s', Body:'%s'" % (
            request.metadata,
            request.channel,
            request.body
        ))

        response = Response(request)
        response.body = "OK".encode('UTF-8')
        response.cache_hit = False
        response.error = "None"
        response.client_id = client_id
        response.executed = True
        response.metadata = "OK"
        response.timestamp = datetime.datetime.now()
        return response


if __name__ == "__main__":
    print("Starting CommandQueryResponder example...\n")

    responder = Responder()

    subscribe_request = create_subscribe_request(SubscribeType.Queries)
    responder.subscribe_to_requests(subscribe_request, handle_incoming_request)

    subscribe_request = create_subscribe_request(SubscribeType.Commands)
    responder.subscribe_to_requests(subscribe_request, handle_incoming_request)

    input("Press 'Enter' to stop the application...\n")
