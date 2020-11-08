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


class AckAllMessagesResponse:
    def __init__(self, ack_all_messages_response=None):
        if ack_all_messages_response:
            self.request_id = ack_all_messages_response.RequestID
            """Represents Unique identifier for the Request."""

            self.is_error = ack_all_messages_response.IsError
            """Returned from KubeMQ, false if no error."""

            self.error = ack_all_messages_response.Error
            """Error message, valid only if IsError true."""
            if ack_all_messages_response.AffectedMessages:
                self.affected_messages = ack_all_messages_response.AffectedMessages
            else:
                self.affected_messages = None
            """"Number of affected messages."""

    def __repr__(self):
        return "<AckAllMessagesResponse request_id:%s is_error:%s error:%s affected_messages:%s>" % (
            self.request_id,
            self.is_error,
            self.error,
            self.affected_messages
        )
