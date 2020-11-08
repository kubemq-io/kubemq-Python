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
import grpc
from kubemq.grpc import Empty
from kubemq.basic.grpc_client import GrpcClient
from kubemq.commandquery.request_receive import RequestReceive
from kubemq.tools.listener_cancellation_token import ListenerCancellationToken


class Responder(GrpcClient):
    """An instance that responsible on receiving request from the kubeMQ."""

    def __init__(self, kubemq_address=None, encryptionHeader=None):
        """
        Initialize a new Responder to subscribe to Response.

        :param str kubemq_address: KubeMQ server address. if None will be parsed from Config or environment parameter.
        """
        GrpcClient.__init__(self, encryptionHeader)
        if kubemq_address:
            self._kubemq_address = kubemq_address

    def ping(self):
        """ping check connection to the kubemq"""
        ping_result = self.get_kubemq_client().Ping(Empty())
        logging.debug("Responder KubeMQ address:%s ping result:%s'" % (self._kubemq_address, ping_result))
        return ping_result

    def subscribe_to_requests(self, subscribe_request, handler, error_handler,
                              listener_cancellation_token=ListenerCancellationToken()):
        """
        Register to kubeMQ Channel using handler.
        :param SubscribeRequest subscribe_request: represent by that will determine the subscription configuration.
        :param handler: Method the perform when receiving RequestReceive
        :param error_handler: Method the perform when receiving error from kubemq
        :param listener_cancellation_token: cancellation token, once cancel is called will cancel the subscribe to kubemq
        :return: A thread running the Subscribe Request.
        """

        if not subscribe_request.channel:
            raise ValueError("Channel parameter is mandatory.")

        if not subscribe_request.is_valid_type("CommandQuery"):
            raise ValueError("Invalid Subscribe Type for this Class.")

        inner_subscribe_request = subscribe_request.to_inner_subscribe_request()

        call = self.get_kubemq_client().SubscribeToRequests(inner_subscribe_request, metadata=self._metadata)

        def subscribe_task(listener_cancellation_token):
            while True:
                try:
                    event_receive = call.next()
                    logging.debug("Responder InnerRequest. ID:'%s', Channel:'%s', ReplyChannel:'%s tags:'%s''" % (
                        event_receive.RequestID,
                        event_receive.Channel,
                        event_receive.ReplyChannel,
                        event_receive.Tags
                    ))

                    if handler:
                        try:
                            response = handler(RequestReceive(event_receive))

                            logging.debug("Responder InnerResponse. ID:'%s', ReplyChannel:'%s'" % (
                                response.request_id,
                                response.reply_channel
                            ))

                            self.get_kubemq_client().SendResponse(response.convert(), None, self._metadata)
                        except grpc.RpcError as error:
                            if (listener_cancellation_token.is_cancelled):
                                logging.info("Sub closed by listener request")
                                error_handler(str(error))
                            else:
                                logging.exception("Subscriber Received Error: Error:'%s'" % (error))
                                error_handler(str(error))
                        except Exception as e:
                            logging.exception("Subscriber Received Error: Error:'%s'" % (e))
                            error_handler(str(e))

                except Exception as e:
                    logging.exception("An exception occurred while listening for request:'%s'" % (e))
                    error_handler(str(e))
                    raise  # re-raise the original exception, keeping full stack trace

        def check_sub_to_valid(listener_cancellation_token):
            while True:
                if (listener_cancellation_token.is_cancelled):
                    logging.info("Sub closed by listener request")
                    call.cancel()
                    return

        thread = threading.Thread(target=subscribe_task, args=(listener_cancellation_token,))
        thread.daemon = True
        thread.start()

        listener_thread = threading.Thread(target=check_sub_to_valid, args=(listener_cancellation_token,))
        listener_thread.daemon = True
        listener_thread.start()
        return thread
