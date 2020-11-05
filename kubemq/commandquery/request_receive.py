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


class RequestReceive:

    def __init__(self, inner_request):
        """
        :param inner_request: inner KubeMQ.Grpc.Request sent from KubeMQ.
        """

        self.request_id = inner_request.RequestID
        """Represents a Request identifier."""

        self.request_type = inner_request.RequestTypeData
        """Represents the type of request operation"""

        self.client_id = inner_request.ClientID
        """Represents the sender ID that the Request return from."""

        self.channel = inner_request.Channel
        """Represents The channel name to send to using the KubeMQ."""

        self.metadata = inner_request.Metadata
        """Represents text as str"""

        self.body = inner_request.Body
        """Represents The content of the KubeMQRequestReceive."""

        self.reply_channel = inner_request.ReplyChannel
        """Represents The channel name that the response returned from."""

        self.timeout = inner_request.Timeout
        """Represents the limit for waiting for response (Milliseconds)."""

        self.cache_key = inner_request.CacheKey
        """Represents if the request should be saved from Cache and under what "Key"(str) to save it."""

        self.cache_ttl = inner_request.CacheTTL
        """Cache time to live : for how long does the request should be saved in Cache."""

        self.tags = inner_request.Tags
        """Represents key value pairs that help distinguish the message"""
