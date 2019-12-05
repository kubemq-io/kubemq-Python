
# MIT License
# Copyright (c) 2018 KubeMQ
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE. 


import datetime
from builtins import input
from random import randint

from kubemq.commandquery.responder import Responder
from kubemq.commandquery.response import Response
from kubemq.subscription.events_store_type import EventsStoreType
from kubemq.subscription.subscribe_request import SubscribeRequest
from kubemq.subscription.subscribe_type import SubscribeType
from kubemq.tools.listener_cancellation_token import ListenerCancellationToken





def handle_incoming_request(request):
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
        response.client_id = 'hello-world-sender'
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
    cancel_token=ListenerCancellationToken()
    receiver = Responder("localhost:50000")

    subscribe_request = SubscribeRequest(
        channel="testing_Command_channel",
        client_id='hello-world-sender',
        events_store_type=SubscribeType.SubscribeTypeUndefined,
        events_store_type_value=0,
        group="",
        subscribe_type=SubscribeType.Commands
    )
    receiver.subscribe_to_requests(subscribe_request, handle_incoming_request,handle_incoming_error,cancel_token)

    input("Press 'Enter' to stop Listen...\n")
    cancel_token.cancel()
    input("Press 'Enter' to stop the application...\n")