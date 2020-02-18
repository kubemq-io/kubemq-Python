import datetime
from builtins import input
from random import randint

from kubemq.commandquery.responder import Responder
from kubemq.commandquery.response import Response
from kubemq.subscription.events_store_type import EventsStoreType
from kubemq.subscription.subscribe_request import SubscribeRequest
from kubemq.subscription.subscribe_type import SubscribeType
from kubemq.tools.listener_cancellation_token import ListenerCancellationToken


def create_subscribe_request(
        subscribe_type=SubscribeType.SubscribeTypeUndefined,
        events_store_type=EventsStoreType.Undefined,
        events_store_type_value=0):
    client_id = str(randint(9, 19999))
    return SubscribeRequest(
        channel="MyTestChannelName",
        client_id=client_id,
        events_store_type=events_store_type,
        events_store_type_value=events_store_type_value,
        group="",
        subscribe_type=subscribe_type
    )


def handle_incoming_request(request):
    client_id = str(randint(9, 19999))
    if request:
        print("Subscriber Received request: Metadata:'%s', Channel:'%s', Body:'%s' tags:%s" % (
            request.metadata,
            request.channel,
            request.body,
            request.tags
        ))

        response = Response(request)
        response.body = "OK".encode('UTF-8')
        response.cache_hit = False
        response.error = "None"
        response.client_id = client_id
        response.executed = True
        response.metadata = "OK"
        response.timestamp = datetime.datetime.now()
        response.tags=request.tags
        return response

def handle_incoming_error(error_msg):
        print("received error:%s'" % (
            error_msg
        ))


if __name__ == "__main__":
    print("Starting CommandQueryResponder example...\n")
    cancel_token=ListenerCancellationToken()
    responder = Responder("localhost:50000")

    subscribe_request = create_subscribe_request(SubscribeType.Queries)
    responder.subscribe_to_requests(subscribe_request, handle_incoming_request,handle_incoming_error,cancel_token)

    subscribe_request = create_subscribe_request(SubscribeType.Commands)
    responder.subscribe_to_requests(subscribe_request, handle_incoming_request,handle_incoming_error,cancel_token)

    input("Press 'Enter' to stop Listen...\n")
    cancel_token.cancel()
    input("Press 'Enter' to stop the application...\n")