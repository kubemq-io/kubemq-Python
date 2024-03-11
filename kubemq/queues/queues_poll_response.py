import uuid
from typing import Callable
from kubemq.grpc import QueuesDownstreamRequest, QueuesDownstreamResponse, QueuesDownstreamRequestType
from kubemq.queues.queues_message_received import QueueMessageReceived


class QueuesPollResponse:

    def __init__(self):
        self.ref_request_id: str = ""
        self.transaction_id: str = ""
        self.messages: [QueueMessageReceived] = []
        self.error: str = ""
        self.is_error: bool = False
        self.is_transaction_completed: bool = False
        self.active_offsets: [int] = []
        self.response_handler: Callable[[QueuesDownstreamRequest], None]
        self.receiver_client_id: str = ""

    def ack_all(self):
        if self.is_transaction_completed:
            raise ValueError("transaction is already completed")
        request = QueuesDownstreamRequest()
        request.RequestID = str(uuid.uuid4())
        request.ClientID = self.receiver_client_id
        request.RequestTypeData = QueuesDownstreamRequestType.AckAll
        request.RefTransactionId = self.transaction_id
        request.SequenceRange.extend(self.active_offsets)
        self.response_handler(request)

    def reject_all(self):
        if self.is_transaction_completed:
            raise ValueError("transaction is already completed")
        request = QueuesDownstreamRequest()
        request.RequestID = str(uuid.uuid4())
        request.ClientID = self.receiver_client_id
        request.RequestTypeData = QueuesDownstreamRequestType.NAckAll
        request.RefTransactionId = self.transaction_id
        request.SequenceRange.extend(self.active_offsets)
        result = self.response_handler(request)
        if result.IsError:
            raise ValueError(result.Error)

    def re_queue_all(self, channel: str):
        if not channel:
            raise ValueError("re-queue channel cannot be empty")
        request = QueuesDownstreamRequest()
        request.RequestID = str(uuid.uuid4())
        request.ClientID = self.receiver_client_id
        request.RequestTypeData = QueuesDownstreamRequestType.ReQueueAll
        request.RefTransactionId = self.transaction_id
        request.SequenceRange.extend(self.active_offsets)
        request.ReQueueChannel = channel
        result = self.response_handler(request)
        if result.IsError:
            raise ValueError(result.Error)

    def decode(self,
               response: QueuesDownstreamResponse,
               receiver_client_id: str,
               response_handler: Callable[
                                      [QueuesDownstreamRequest], QueuesDownstreamResponse]) -> 'QueuesPollResponse':
        self.ref_request_id = response.RefRequestId
        self.transaction_id = response.TransactionId
        self.error = response.Error
        self.is_error = response.IsError
        self.is_transaction_completed = response.TransactionComplete
        self.active_offsets.extend([response.ActiveOffsets])
        self.response_handler = response_handler
        self.receiver_client_id = receiver_client_id
        for message in response.Messages:
            self.messages.append(QueueMessageReceived().decode(
                message,
                response.TransactionId,
                response.TransactionComplete,
                receiver_client_id,
                response_handler))
        return self
