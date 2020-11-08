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
from datetime import datetime

from kubemq.commandquery.request_receive import RequestReceive
from kubemq.grpc import Response as InnerResponse

epoch = datetime.utcfromtimestamp(0)


class Response:
    """The response that receiving from KubeMQ after sending a request."""

    def __init__(self, request):
        if isinstance(request, RequestReceive):
            self.request_id = request.request_id
            """Represents a Response identifier."""
            self.reply_channel = request.reply_channel
            """Channel name for the Response. Set and used internally by KubeMQ server."""

        elif isinstance(request, InnerResponse):

            self.client_id = request.ClientID or ""
            """Represents the sender ID that the Response will be send under."""

            self.request_id = request.RequestID
            """Represents a Response identifier."""

            self.reply_channel = request.ReplyChannel
            """Channel name for the Response. Set and used internally by KubeMQ server."""

            self.metadata = request.Metadata or ""
            """Represents text as str."""

            self.body = request.Body
            """Represents The content of the Response."""

            self.cache_hit = request.CacheHit
            """Represents if the response was received from Cache."""

            self.timestamp = datetime.fromtimestamp(request.Timestamp)
            """Represents if the response Time."""

            self.executed = request.Executed
            """Represents if the response was executed."""

            self.error = request.Error
            """Error message"""

            self.tags = request.Tags
            """Represents key value pairs that help distinguish the message"""

        else:
            raise Exception("Unknown type" + str(type(request)))

    def convert(self):
        return InnerResponse(
            ClientID=self.client_id or "",
            RequestID=self.request_id,
            ReplyChannel=self.reply_channel,
            Metadata=self.metadata or "",
            Body=self.body,
            CacheHit=self.cache_hit,
            Timestamp=int((self.timestamp - epoch).total_seconds()),
            Executed=self.executed,
            Error=self.error
        )
