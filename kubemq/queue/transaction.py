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
from kubemq.basic.grpc_client import GrpcClient
from kubemq.queue.transaction_messages import TransactionMessagesResponse
from kubemq.queue.transaction_messages import create_stream_queue_message_ack_request
from kubemq.queue.transaction_messages import create_stream_queue_message_receive_request
from kubemq.queue.transaction_messages import create_stream_queue_message_reject_request
from kubemq.queue.transaction_messages import create_stream_queue_message_extend_visibility_request
from kubemq.queue.transaction_messages import create_stream_queue_message_resend_request
from kubemq.queue.transaction_messages import create_stream_queue_message_modify_request
from kubemq.tools.id_generator import get_next_id
class Transaction(GrpcClient):
    """Represents a Queue pattern. TO DO Cancellation TOKEN!"""

    def __init__(self,queue,in_transaction=None):

        """
        Initializes a new Transaction using Queue .
        :param queue: should be called from queue.transaction()".
        """
        self.queue=queue
        self.stream =None
        self.InTransaction=False
        self._kubemq_address=queue._kubemq_address
        

    async def receive(self,visibility_seconds=1,wait_time_seconds=None):
        """Receive queue messages request , waiting for response or timeout."""
        if self.open_stream()==False:
            return TransactionMessagesResponse(None,None,True,"active queue message wait for ack/reject")
        else:
            try:
                stream_queue_response=await self.get_kubemq_client().StreamQueueMessage(create_stream_queue_message_receive_request(self.queue,visibility_seconds,wait_time_seconds))
            except Exception as e:
                logging.exception("Exception in receive:'%s'" % (e))
                raise
            return TransactionMessagesResponse(stream_queue_response)
    
    async def ack_message(self,msg_sequence):
        """Will mark Message dequeued on queue."""
        if self.open_stream()==False:
            return TransactionMessagesResponse(None,None,True,"no active message to ack, call Receive first")
        else:
            try:
                stream_queue_response=await self.get_kubemq_client().StreamQueueMessage(create_stream_queue_message_ack_request(self.queue,msg_sequence))
            except Exception as e:
                logging.exception("Exception in ack:'%s'" % (e))
                raise
            return TransactionMessagesResponse(stream_queue_response)

    async def rejected_message(self,msg_sequence):
        """Will return message to queue."""
        if self.open_stream()==False:
            return TransactionMessagesResponse(None,None,True,"no active message to reject, call Receive first")
        else:
            try:
                stream_queue_response=await self.get_kubemq_client().StreamQueueMessage(create_stream_queue_message_reject_request(self.queue,msg_sequence))
            except Exception as e:
                logging.exception("Exception in rejected:'%s'" % (e))
                raise
            return TransactionMessagesResponse(stream_queue_response)

    async def extend_visibility(self,visibility_seconds):
        """Extend the visibility time for the current receive message."""
        if self.open_stream()==False:
            return TransactionMessagesResponse(None,None,True,"no active message to extend visibility, call Receive first")
        else:
            try:
                stream_queue_response=await self.get_kubemq_client().StreamQueueMessage(create_stream_queue_message_extend_visibility_request(self.queue,visibility_seconds))
            except Exception as e:
                logging.exception("Exception in extend visibility:'%s'" % (e))
                raise
            return TransactionMessagesResponse(stream_queue_response)

    async def resend(self,queue_name):
        """Resend the current received message to a new channel and ack the current message."""
        if self.open_stream()==False:
            return TransactionMessagesResponse(None,None,True,"no active message to resend, call Receive first")
        else:
            try:
                stream_queue_response=await self.get_kubemq_client().StreamQueueMessage(create_stream_queue_message_resend_request(self.queue,queue_name))
            except Exception as e:
                logging.exception("Exception in resend visibility:'%s'" % (e))
                raise
            return TransactionMessagesResponse(stream_queue_response)

    async def modify(self,msg):
        """Resend the new message to a new channel."""
        if self.open_stream()==False:
            return TransactionMessagesResponse(None,None,True,"no active message to rmodifyesend, call Receive first")
        else:
            try:
                msg.ClientID=self.queue.ClientID
                msg.MessageID=get_next_id()
                msg.Queue=msg.Queue or self.queue.queue_name
                msg.Metadata=msg.Metadata or ""
                stream_queue_response=await self.get_kubemq_client().StreamQueueMessage(create_stream_queue_message_modify_request(self.queue,msg))
            except Exception as e:
                logging.exception("Exception in modify visibility:'%s'" % (e))
                raise
            return TransactionMessagesResponse(stream_queue_response)    

    def open_stream(self):
        if self.check_call_is_in_transaction()==False:
            self.stream=self.get_kubemq_client().StreamQueueMessage(None,None)
            return True
        else:
            return False

    def check_call_is_in_transaction(self):
        try:
            if self.stream is None:
                return False
            if self.stream.GetStatus().StatusCode==0:
                return False
            return False
        except Exception as ex:
            if getattr(ex, 'message', repr(ex))=="Status can only be accessed once the call has finished.":
                return True
            else:
                raise ex