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
from kubemq.commandquery.request_type import RequestType
from kubemq.tools.id_generator import get_guid

class CommandRequest:
    """Represents the Request used in RequestReply to send information using the KubeMQ."""

    def __init__(self, request_id=None, client_id=None, channel=None,
                 reply_channel=None, metadata=None, body=None, timeout=None, tags=None):
        self.request_id = request_id
        self.client_id = client_id
        self.channel = channel
        self.metadata = metadata
        self.body = body
        self.timeout = timeout
        self.tags = tags

    def convert(self):
        """Convert a Request to an InnerRequest"""
        return InnerRequest(
            RequestID=self.request_id or get_guid(),
            RequestTypeData=RequestType.Command.value,
            ClientID=self.client_id or get_guid(),
            Channel=self.channel,
            Metadata=self.metadata or "",
            Body=self.body,
            Timeout=self.timeout,
            Tags=self.tags
        )
