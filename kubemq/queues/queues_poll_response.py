import uuid
from typing import Callable
from kubemq.grpc import QueuesDownstreamRequest, QueuesDownstreamResponse, QueuesDownstreamRequestType
from kubemq.queues.queues_message_received import QueueMessageReceived


class QueuesPollResponse:
    """
    This class represents the response received when polling a queue.

    Attributes:
        ref_request_id (str): The reference request ID.
        transaction_id (str): The transaction ID.
        messages (List[QueueMessageReceived]): The list of queue messages received.
        error (str): The error message, if any.
        is_error (bool): A flag indicating whether an error occurred.
        is_transaction_completed (bool): A flag indicating whether the transaction is completed.
        active_offsets (List[int]): The active offsets.
        response_handler (Callable[[QueuesDownstreamRequest], None]): The response handler function.
        receiver_client_id (str): The receiver client ID.

    Methods:
        ack_all: Sends an acknowledgement for all messages in the transaction.
        reject_all: Rejects all messages in the transaction.
        re_queue_all: Re-queues all messages in the transaction to a specified channel.
        decode: Decodes the response received from the downstream and populates the response object.

    """
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
