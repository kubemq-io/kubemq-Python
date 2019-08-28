# MIT License
#
# Copyright (c) 2018 KubeMQ
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
import logging
import threading

from kubemq.basic.grpc_client import GrpcClient
from kubemq.commandquery.request_receive import RequestReceive


class Responder(GrpcClient):
    """An instance that responsible on receiving request from the kubeMQ."""

    def __init__(self, kubemq_address=None):
        """
        Initialize a new Responder to subscribe to Response.

        :param str kubemq_address: KubeMQ server address. if None will be parsed from Config or environment parameter.
        """
        GrpcClient.__init__(self)
        if kubemq_address:
            self._kubemq_address = kubemq_address

    def subscribe_to_requests(self, subscribe_request, handler):
        """
        Register to kubeMQ Channel using handler.
        :param SubscribeRequest subscribe_request: represent by that will determine the subscription configuration.
        :param handler: Method the perform when receiving RequestReceive
        :return: A thread running the Subscribe Request.
        """

        if not subscribe_request.channel:
            raise ValueError("Channel parameter is mandatory.")

        if not subscribe_request.is_valid_type("CommandQuery"):
            raise ValueError("Invalid Subscribe Type for this Class.")

        inner_subscribe_request = subscribe_request.to_inner_subscribe_request()

        call = self.get_kubemq_client().SubscribeToRequests(inner_subscribe_request, metadata=self._metadata)

        def subscribe_task():
            while True:
                try:
                    event_receive = call.next()
                    logging.debug("Responder InnerRequest. ID:'%s', Channel:'%s', ReplyChannel:'%s'" % (
                        event_receive.RequestID,
                        event_receive.Channel,
                        event_receive.ReplyChannel
                    ))

                    if handler:
                        try:
                            response = handler(RequestReceive(event_receive))

                            logging.debug("Responder InnerResponse. ID:'%s', ReplyChannel:'%s'" % (
                                response.request_id,
                                response.reply_channel
                            ))

                            self.get_kubemq_client().SendResponse(response.convert(), self._metadata)
                        except Exception as e:
                            logging.exception("An exception occurred while handling the response:'%s'" % (e))
                            raise  # re-raise the original exception, keeping full stack trace

                except Exception as e:
                    logging.exception("An exception occurred while listening for request:'%s'" % (e))
                    raise  # re-raise the original exception, keeping full stack trace

        thread = threading.Thread(target=subscribe_task, args=())
        thread.daemon = True
        thread.start()
        return thread
