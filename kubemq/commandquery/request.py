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


class Request:
    """Represents the Request used in a Channel."""

    def __init__(self, request_id=None, metadata=None, body=None, tags=None):
        """
        Initializes a new instance of a Request for a Channel

        :param str request_id: Represents a Request identifier.
        :param str metadata: Represents metadata for a Request.
        :param bytes body: Represents The content of the Request.
        """
        self.request_id = request_id
        """Represents a Request identifier."""

        self.metadata = metadata
        """Represents metadata for a Request."""

        self.body = body
        """Represents The content of the Request."""

        self.tags = tags
        """Represents key value pairs that help distinguish the message"""

    def __repr__(self):
        return "<requst request_id:%s metadata:%s body:%s tags:%s>" % (
            self.request_id,
            self.metadata,
            self.body,
            self.tags
        )
