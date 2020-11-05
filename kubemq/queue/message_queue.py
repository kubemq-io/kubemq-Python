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
import logging
import uuid
from kubemq.basic.grpc_client import GrpcClient
from kubemq.queue.send_message_result import SendMessageResult
from kubemq.queue.ack_all_messages_response import AckAllMessagesResponse
from kubemq.queue.message import to_queue_messages as to_queue_messages
from kubemq.queue.message import convert_queue_message_batch_request as convert_queue_message_batch_request
from kubemq.queue.receive_messages_response import ReceiveMessagesResponse as ReceiveMessagesResponse
from kubemq.grpc import Empty
from kubemq.grpc import ReceiveQueueMessagesRequest
from kubemq.grpc import AckAllQueueMessagesRequest
from kubemq.queue.send_batch_message_result import \
    convert_from_queue_messages_batch_response as convert_from_queue_messages_batch_response
from kubemq.tools.id_generator import get_next_id as get_next_id
from kubemq.queue.transaction import Transaction


class MessageQueue(GrpcClient):
    """Represents a MessageQueue pattern."""

    def __init__(self, queue_name=None, client_id=None, kubemq_address=None, max_number_of_messages=32,
                 wait_time_seconds_queue_messages=1, encryptionHeader=None):

        """
        Initializes a new MessageQueue using params .
        :param kubemq_address: The address the of the KubeMQ including the GRPC Port ,Example: "LocalHost:50000".
        :param client_id: Represents the sender ID that the messages will be send under.
        :param max_number_of_messages: Number of received messages.
        :param wait_time_seconds_queue_messages: Wait time for received messages.
        :param queue_name: Represents The queue name to send to using the KubeMQ.
        :param byte[] encryptionHeader: the encrypted header requested by kubemq authentication.
        """
        GrpcClient.__init__(self, encryptionHeader)
        if kubemq_address:
            self._kubemq_address = kubemq_address
        if client_id:
            self.client_id = client_id

        self.max_number_of_messages = max_number_of_messages

        self.wait_time_seconds_queue_messages = wait_time_seconds_queue_messages
        self.encryptionHeader = encryptionHeader
        self.queue_name = queue_name
        self.transaction = None

    def create_transaction(self):
        if self.transaction is None:
            self.transaction = Transaction(self)
        return self.transaction

    def send_queue_message(self, message):
        """Publish a single message using the KubeMQ."""
        try:
            message.queue = self.queue_name
            message.client_id = self.client_id
            inner_queue_message = message.convert_to_queue_message(self)
            inner_response = self.get_kubemq_client().SendQueueMessage(inner_queue_message, metadata=self._metadata)
            return SendMessageResult(inner_response)
        except Exception as e:
            logging.exception("Grpc Exception in send_queue_message_result:'%s'" % (e))
            raise

    def send_queue_messages_batch(self, messages):
        """"Publish a group of messages to a queue"""
        try:
            id = get_next_id()
            inner_response = self.get_kubemq_client().SendQueueMessagesBatch(
                convert_queue_message_batch_request(id, to_queue_messages(messages, self)), metadata=self._metadata)
            return convert_from_queue_messages_batch_response(inner_response)
        except Exception as e:
            logging.exception("Grpc Exception in send_queue_messages_batch:'%s'" % (e))
            raise

    def receive_queue_messages(self, number_of_messages=None):
        """Receive messages from queue"""
        try:
            id = get_next_id()
            inner_response = self.get_kubemq_client().ReceiveQueueMessages(
                self.convert_to_receive_queue_messages_request(id, False, number_of_messages), metadata=self._metadata)
            return ReceiveMessagesResponse(inner_response)
        except Exception as e:
            logging.exception("Grpc Exception in send_queue_messages:'%s'" % (e))
            raise

    def peek_queue_message(self, number_of_messages=None):
        """peek queue messages from queue"""
        try:
            id = get_next_id()
            inner_response = self.get_kubemq_client().ReceiveQueueMessages(
                self.convert_to_receive_queue_messages_request(id, True, number_of_messages), metadata=self._metadata)
            return ReceiveMessagesResponse(inner_response)
        except Exception as e:
            logging.exception("Grpc Exception in send_queue_messages_batch:'%s'" % (e))
            raise

    def ack_all_queue_messages(self):
        """acknowledge queue messages from queue"""
        try:
            inner_response = self.get_kubemq_client().AckAllQueueMessages(
                self.convert_to_ack_all_queue_message_request(), metadata=self._metadata)
            return AckAllMessagesResponse(inner_response)
        except Exception as e:
            logging.exception("Grpc Exception in ack_all_queue_messages:'%s'" % (e))
            raise

    def ping(self):
        """ping check connection to the kubemq"""
        ping_result = self.get_kubemq_client().Ping(Empty())
        logging.debug("MessageQueue KubeMQ address:%s ping result:%s'" % (self._kubemq_address, ping_result))
        return ping_result

    def convert_to_ack_all_queue_message_request(self):
        """create AckAllQueueMessagesRequest from MessageQueue"""
        return AckAllQueueMessagesRequest(
            RequestID=get_next_id(),
            ClientID=self.client_id,
            Channel=self.queue_name,
            WaitTimeSeconds=self.wait_time_seconds_queue_messages,
        )

    def convert_to_receive_queue_messages_request(self, request_id, is_peek=False, max_number_of_message=None):
        """create ReceiveQueueMessagesRequest from MessageQueue"""
        if max_number_of_message is None:
            max_number_of_message = self.max_number_of_messages
        return ReceiveQueueMessagesRequest(
            RequestID=request_id,
            ClientID=self.client_id,
            Channel=self.queue_name,
            MaxNumberOfMessages=max_number_of_message,
            WaitTimeSeconds=self.wait_time_seconds_queue_messages,
            IsPeak=is_peek,
        )

    def __repr__(self):
        return "<MessageQueue kubemq_address:%s client_id:%s max_number_of_messages:%s wait_time_seconds_queue_messages:%s>" % (
            self._kubemq_address,
            self.client_id,
            self.max_number_of_messages,
            self.wait_time_seconds_queue_messages
        )
