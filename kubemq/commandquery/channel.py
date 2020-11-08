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
import sys

from kubemq.commandquery.lowlevel.initiator import Initiator
from kubemq.commandquery.lowlevel.request import Request as LowLevelRequest


class Channel:
    """Represents a Initiator with predefined parameters."""

    def __init__(self, channel_parameters=None, request_type=None, channel_name=None, client_id=None, timeout=None,
                 cache_key=None,
                 cache_ttl=None, kubemq_address=None, encryptionHeader=None):
        # Initializes a new instance of the RequestChannel class using RequestChannelParameters.
        if channel_parameters:
            self.request_type = channel_parameters.request_type
            self.channel_name = channel_parameters.channel_name
            self.client_id = channel_parameters.client_id
            self.timeout = channel_parameters.timeout
            self.cache_key = channel_parameters.cache_key
            self.cache_ttl = channel_parameters.cache_ttl
            self.kubemq_address = channel_parameters.kubemq_address
            self.encryptionHeader = channel_parameters.encryptionHeader
        # Initializes a new instance of the RequestChannel class using a set of parameters.
        else:
            self.request_type = request_type

            self.channel_name = channel_name
            """Represents The channel name to send to using the KubeMQ."""

            self.client_id = client_id
            """Represents the sender ID that the Request will be send under."""

            self.timeout = timeout
            """Represents the limit for waiting for response (Milliseconds)."""

            self.cache_key = cache_key
            """Represents if the request should be saved from Cache and under what "Key"(System.String) to save it."""

            self.cache_ttl = cache_ttl
            """Cache time to live : for how long does the request should be saved in Cache."""

            self.kubemq_address = kubemq_address

            self.encryptionHeader = encryptionHeader
            """the header for authentication using kubemq."""

        # Validate arguments

        if not self.channel_name:
            raise ValueError("channel_name argument is mandatory")

        if not self.request_type:
            raise ValueError("request_type argument is mandatory")

        if not self.timeout and timeout <= 0:
            raise ValueError("timeout argument is mandatory and must between 1 to {}" % sys.maxsize)

        self._initiator = Initiator(self.kubemq_address, self.encryptionHeader)

    def send_request(self, request, override_params=None):
        """
        Publish a single request using the KubeMQ, response will return in the passed handler.

        Parameters
        ----------
        :param Request request: The request that will be sent to the kubeMQ.
        :param RequestParameters override_params: overriding `timeout`, `cache-key` and `cache_ttl` for the Request.
        :return Response
        """
        return self._initiator.send_request(self.create_low_level_request(request, override_params))

    def send_request_async(self, request, handler, override_params=None):
        """
        Publish a single async request using the KubeMQ, response will return in the passed handler.

        Parameters
        ----------
        :param Request request: The request that will be sent to the kubeMQ.
        :param handler: handler for processing response
        :param RequestParameters override_params: overriding `timeout`, `cache-key` and `cache_ttl` for the Request.
        :return Response
        """
        return self._initiator.send_request_async(self.create_low_level_request(request, override_params), handler)

    def create_low_level_request(self, request, override_params=None):
        request = LowLevelRequest(
            request_type=self.request_type,
            channel=self.channel_name,
            client_id=self.client_id,
            timeout=self.timeout,
            cache_key=self.cache_key,
            cache_ttl=self.cache_ttl,
            request_id=request.request_id,
            body=request.body,
            metadata=request.metadata,
            tags=request.tags
        )

        if override_params:
            if override_params.timeout:
                request.timeout = override_params.timeout
            if override_params.cache_key:
                request.cache_key = override_params.cache_key
            if override_params.cache_ttl:
                request.cache_ttl = override_params.cache_ttl

        return request
