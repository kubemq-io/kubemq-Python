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
from kubemq.grpc import Empty
from kubemq.basic.grpc_client import GrpcClient
from kubemq.commandquery.response import Response


class Initiator(GrpcClient):
    """Represents the instance that is responsible to send requests to the kubemq."""

    def __init__(self, kubemq_address=None, encryptionHeader=None):
        """
        Initialize a new Initiator.

        :param str kubemq_address: KubeMQ server address. if None will be parsed from Config or environment parameter.
        """
        GrpcClient.__init__(self, encryptionHeader)
        if kubemq_address:
            self._kubemq_address = kubemq_address

    def ping(self):
        """ping check connection to the kubemq"""
        ping_result = self.get_kubemq_client().Ping(Empty())
        logging.debug("Initiator KubeMQ address:%s ping result:%s'" % (self._kubemq_address, ping_result))
        return ping_result

    def send_request_async(self, request, handler):
        """Publish a single request using the KubeMQ."""

        def process_response(call):
            handler(Response(call.result()))

        try:
            inner_request = request.convert()
            call_future = self.get_kubemq_client().SendRequest.future(inner_request, None, self._metadata)
            call_future.add_done_callback(process_response)
        except Exception as e:
            logging.exception("Grpc Exception in send_request_async'%s'" % (e))
            raise

    def send_request(self, request):
        """Publish a single request using the KubeMQ."""
        try:
            inner_request = request.convert()
            inner_response = self.get_kubemq_client().SendRequest(inner_request, None, self._metadata)
            return Response(inner_response)
        except Exception as e:
            logging.exception("Grpc Exception in send_request:'%s'" % (e))
            raise
