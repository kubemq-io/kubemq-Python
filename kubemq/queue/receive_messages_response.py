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
from kubemq.queue.message import Message as Message


class ReceiveMessagesResponse:
    def __init__(self, receive_queue_messages_response=None):
        if receive_queue_messages_response:
            self.error = receive_queue_messages_response.Error
            self.is_error = receive_queue_messages_response.IsError
            self.is_peek = receive_queue_messages_response.IsPeak
            my_messages = []
            for message in receive_queue_messages_response.Messages:
                my_messages.append(Message(message))
            self.messages = receive_queue_messages_response.Messages
            self.messages_expired = receive_queue_messages_response.MessagesExpired
            self.messages_received = receive_queue_messages_response.MessagesReceived
            self.request_id = receive_queue_messages_response.RequestID

    def __repr__(self):
        return "<ReceiveMessagesResponse error:%s is_error:%s is_peek:%s messages:%s messages_expired:%s messages_received:%s request_id:%s>" % (
            self.error,
            self.is_error,
            self.is_peek,
            self.messages,
            self.messages_expired,
            self.messages_received,
            self.request_id,
        )
