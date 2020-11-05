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
import enum
import logging
import threading
from queue import Queue

from grpc import StatusCode

from kubemq.queue.message import Message
from kubemq.basic.grpc_client import GrpcClient
from kubemq.queue.transaction_messages import TransactionMessagesResponse
from kubemq.queue.transaction_messages import create_stream_queue_message_ack_request
from kubemq.queue.transaction_messages import create_stream_queue_message_extend_visibility_request
from kubemq.queue.transaction_messages import create_stream_queue_message_modify_request
from kubemq.queue.transaction_messages import create_stream_queue_message_receive_request
from kubemq.queue.transaction_messages import create_stream_queue_message_reject_request
from kubemq.queue.transaction_messages import create_stream_queue_message_resend_request
from kubemq.tools.id_generator import get_next_id


class ControlMessages(enum.Enum):
    CLOSE_STREAM = enum.auto()


class Transaction(GrpcClient):
    """Represents a MessageQueue pattern.

    Attributes:
        queue: should be called from queue.transaction()".
        stream: If stream is active.
        inner_stream: represent the stream receive from kubemq
        _kubemq_address:represent kubemq connection string.
        lock:inner lock for checking stream
    """

    def __init__(self, queue):
        """
        Initializes a new Transaction using MessageQueue .
        :param queue: should be called from queue.transaction()".
        """
        super().__init__(queue.encryptionHeader)
        self.queue = queue
        self.stream = False
        self.inner_stream = None
        self._kubemq_address = queue.get_kubemq_address()
        self.client = self.get_kubemq_client()
        self.lock = threading.Lock()
        self.stream_observer = None

    def receive(self, visibility_seconds=1, wait_time_seconds=1):
        """Receive queue messages request , waiting for response or timeout."""
        if self.open_stream():
            raise Exception("Stream already open , please call ack")

        try:
            msg = create_stream_queue_message_receive_request(self.queue, visibility_seconds, wait_time_seconds)
            self.stream_observer.put(msg)

            return TransactionMessagesResponse(next(self.inner_stream))
        except Exception as e:
            logging.exception("Exception in receive: '%s'" % e)
            raise

    def ack_message(self, msg_sequence):
        """Will mark Message de-queued on queue."""
        if not self.check_call_is_in_transaction():
            raise Exception("no active message to ack, call Receive first")

        try:
            msg = create_stream_queue_message_ack_request(self.queue, msg_sequence)
            self.stream_observer.put(msg)

            return TransactionMessagesResponse(next(self.inner_stream))
        except Exception as e:
            logging.exception("Exception in ack:'%s'" % e)
            raise

    def rejected_message(self, msg_sequence):
        """Will return message to queue."""
        if not self.check_call_is_in_transaction():
            return TransactionMessagesResponse(None, None, True, "no active message to reject, call Receive first")
        else:
            try:
                msg = create_stream_queue_message_reject_request(self.queue, msg_sequence)
                self.stream_observer.put(msg)

                return TransactionMessagesResponse(next(self.inner_stream))
            except Exception as e:
                logging.exception("Exception in reject:'%s'" % e)
                raise

    def extend_visibility(self, visibility_seconds):
        """Extend the visibility time for the current receive message."""
        if not self.check_call_is_in_transaction():
            return TransactionMessagesResponse(None, None, True,
                                               "no active message to extend visibility, call Receive first")
        else:
            try:
                msg = create_stream_queue_message_extend_visibility_request(self.queue, visibility_seconds)
                self.stream_observer.put(msg)

                return TransactionMessagesResponse(next(self.inner_stream))
            except Exception as e:
                logging.exception("Exception in Extend:'%s'" % e)
                raise

    def resend(self, queue_name):
        """Resend the current received message to a new channel and ack the current message."""
        if not self.check_call_is_in_transaction():
            return TransactionMessagesResponse(None, None, True, "no active message to resend, call Receive first")
        else:
            try:
                msg = create_stream_queue_message_resend_request(self.queue, queue_name)
                self.stream_observer.put(msg)

                return TransactionMessagesResponse(next(self.inner_stream))
            except Exception as e:
                logging.exception("Exception in resend:'%s'" % e)
                raise

    def modify(self, msg):
        """Resend the new message to a new channel."""
        if not self.check_call_is_in_transaction():
            return TransactionMessagesResponse(None, None, True,
                                               "no active message to modify, call Receive first")
        else:
            try:
                if isinstance(msg, Message):
                    msg.client_id = msg.client_id or self.queue.client_id
                    msg = msg.convert_to_queue_message()
                msg.ClientID = msg.ClientID
                msg.MessageID = get_next_id()

                msg = create_stream_queue_message_modify_request(self.queue, msg)
                self.stream_observer.put(msg)

                return TransactionMessagesResponse(next(self.inner_stream))
            except Exception as e:
                logging.exception("Exception in resend:'%s'" % e)
                raise

    def open_stream(self):
        with self.lock:
            if self.stream:
                return True

            stream_observer = Queue()

            def stream():
                while True:
                    msg = stream_observer.get()

                    if msg == ControlMessages.CLOSE_STREAM:
                        stream_observer.task_done()
                        break

                    if msg:
                        yield msg
                        stream_observer.task_done()

            def done(call):
                stream_observer.put(ControlMessages.CLOSE_STREAM)
                self.stream = False

            self.inner_stream = self.client.StreamQueueMessage(stream(), metadata=self.queue._metadata)
            self.inner_stream.add_done_callback(done)

            self.stream_observer = stream_observer
            self.stream = True
            return False

    def close_stream(self):
        with self.lock:
            if self.stream:
                self.stream = False
                self.inner_stream.cancel()
                return True
            else:
                logging.error("Stream is closed")
                return False

    def check_call_is_in_transaction(self):
        with self.lock:
            return self.stream
