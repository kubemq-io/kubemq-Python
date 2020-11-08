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

from kubemq.queue import send_message_result as create_result
from kubemq.grpc import QueueMessagesBatchResponse as QueueMessagesBatchResponse


def convert_from_queue_messages_batch_response(queue_messages_batch_response):
    """Convert from QueueMessagesBatchResponse to SendBatchMessageResult"""
    batch_message_result = SendBatchMessageResult()
    batch_message_result.batch_id = queue_messages_batch_response.BatchID
    batch_message_result.result = queue_messages_batch_response.Results
    batch_message_result.have_errors = queue_messages_batch_response.HaveErrors
    return batch_message_result


class SendBatchMessageResult:
    def __init__(self, queue_messages_batch_response=None):
        if queue_messages_batch_response:
            self.batch_id = queue_messages_batch_response.BatchID
            """Represents Unique identifier for the Request."""

            self.have_errors = queue_messages_batch_response.HaveErrors
            """Returned from KubeMQ, false if no error."""

            self.result = queue_messages_batch_response.Error
            """Error message, valid only if IsError true."""

    def __repr__(self):
        return "<SendMessageResult batch_id:%s have_errors:%s result:%s>" % (
            self.batch_id,
            self.have_errors,
            self.result
        )

    def convert_to_send_message_result(self, results):
        """convert a few results to SendMessageResult """
        for result in results:
            yield create_result.SendMessageResult(result)
