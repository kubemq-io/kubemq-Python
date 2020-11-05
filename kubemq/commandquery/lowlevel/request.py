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

from kubemq.tools.id_generator import get_next_id
from kubemq.grpc import Request as InnerRequest


class Request:
    """Represents the Request used in RequestReply to send information using the KubeMQ."""

    def __init__(self, inner_request=None, request_id=None, request_type=None, client_id=None, channel=None,
                 reply_channel=None, metadata=None, body=None, timeout=None, cache_key=None, cache_ttl=None, tags=None):
        """Initializes a new instance of the Request with a set of parameters."""
        if inner_request:
            self.request_id = inner_request.RequestID or get_next_id()
            """Represents a Request identifier."""

            self.request_type = inner_request.RequestTypeData
            """Represents the type of the request operation."""

            self.client_id = inner_request.ClientID
            """Represents the sender ID that the Request will be send under."""

            self.channel = inner_request.Channel
            """Represents The channel name to send to using the KubeMQ."""

            self.reply_channel = inner_request.ReplyChannel
            """Represents The channel name to return response to."""

            self.metadata = inner_request.Metadata
            """Represents metadata text attached to the request."""

            self.body = inner_request.Body
            """Represents The content of the request."""

            self.timeout = inner_request.Timeout
            """Represents the limit for waiting for response (Milliseconds)."""

            self.cache_key = inner_request.CacheKey
            """Represents if the request should be saved from Cache and under what "Key"(Str) to save it."""

            self.cache_ttl = inner_request.CacheTTL
            """Cache time to live : for how long does the request should be saved in Cache."""

            self.tags = inner_request.Tags
            """Represents key value pairs that help distinguish the message"""

        else:
            self.request_id = request_id
            self.request_type = request_type
            self.client_id = client_id
            self.channel = channel
            self.reply_channel = reply_channel
            self.metadata = metadata
            self.body = body
            self.timeout = timeout
            self.cache_key = cache_key
            self.cache_ttl = cache_ttl
            self.tags = tags

    def convert(self):
        """Convert a Request to an InnerRequest"""
        return InnerRequest(
            RequestID=self.request_id or get_next_id(),
            RequestTypeData=self.request_type.value,
            ClientID=self.client_id or "",
            Channel=self.channel,
            Metadata=self.metadata or "",
            Body=self.body,
            Timeout=self.timeout,
            CacheKey=self.cache_key,
            CacheTTL=self.cache_ttl,
            Tags=self.tags
        )
