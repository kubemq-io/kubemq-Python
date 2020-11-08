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

from kubemq.grpc import QueueMessage as QueueMessage
from kubemq.grpc import QueueMessagesBatchRequest
from kubemq.tools.id_generator import get_next_id as get_next_id


class Message:
    def __init__(self, queue_message=None, message_id=None, client_id=None, queue=None, metadata=None, body=None,
                 tags=None, attributes=None, policy=None):
        if queue_message:
            self.message_id = queue_message.MessageID
            """Represents identifier to help distinguish the message"""

            self.client_id = queue_message.ClientID
            """Represents identifier to help distinguish the sender"""

            self.queue = queue_message.Channel
            """Represents the channel name to send the message to."""

            self.metadata = queue_message.Metadata
            """Represents text as str."""

            self.body = queue_message.Body
            """Represents The content of the Message."""

            self.tags = tags
            """Represents key value pairs that help distinguish the message"""

            self.attributes = queue_message.Attributes
            """Contain general data on the message."""

            self.policy = queue_message.Policy
            """A set of 'rules' that can be assign to the message"""
        else:
            self.message_id = message_id
            """Represents identifier to help distinguish the message"""

            self.client_id = client_id
            """Represents identifier to help distinguish the sender"""

            self.queue = queue
            """Represents the channel name to send the message to."""

            self.metadata = metadata
            """Represents text as str."""

            self.body = body
            """Represents The content of the Message."""

            self.tags = tags
            """Represents key value pairs that help distinguish the message"""

            self.attributes = attributes
            """Contain general data on the message."""

            self.policy = policy
            """A set of 'rules' that can be assign to the message"""

    def __repr__(self):
        return "<Message message_id:%s client_id:%s queue:%s metadata:%s body:%s tags:%s attributes:%s policy:%s>" % (
            self.message_id,
            self.client_id,
            self.queue,
            self.metadata,
            self.body,
            self.tags,
            self.attributes,
            self.policy,
        )

    def convert_to_queue_message(self, queue=None):
        """Convert a Message to an QueueMessage"""
        return QueueMessage(
            MessageID=self.message_id or get_next_id(),
            ClientID=self.client_id or queue.client_id,
            Channel=self.queue or queue.queue_name,
            Metadata=self.metadata,
            Body=self.body,
            Tags=self.tags or "",
            Attributes=self.attributes,
            Policy=self.policy
        )


def convert_queue_message_batch_request(uuid, messages):
    """Convert a messages to an QueueMessagesBatchRequest"""
    return QueueMessagesBatchRequest(
        BatchID=uuid,
        Messages=messages,
    )


def to_queue_messages(messages, queue=None, channel_name=None):
    """Convert  messages to a single message"""
    queue_messages = []
    for message in messages:
        if message.queue is None and channel_name is not None:
            message.queue = channel_name
        queue_messages.append(message.convert_to_queue_message(queue))

    return queue_messages
