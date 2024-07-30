from pydantic import BaseModel, Field
from typing import List, Callable, Optional
import uuid
from kubemq.grpc import (
    QueuesDownstreamRequest,
    QueuesDownstreamResponse,
    QueuesDownstreamRequestType,
)
from kubemq.queues.queues_message_received import QueueMessageReceived


class QueuesPollResponse(BaseModel):
    ref_request_id: str = Field(default="")
    transaction_id: str = Field(default="")
    messages: List[QueueMessageReceived] = Field(default_factory=list)
    error: str = Field(default="")
    is_error: bool = Field(default=False)
    is_transaction_completed: bool = Field(default=False)
    active_offsets: List[int] = Field(default_factory=list)
    receiver_client_id: str = Field(default="")
    response_handler: Optional[
        Callable[[QueuesDownstreamRequest], QueuesDownstreamResponse]
    ] = Field(default=None)

    def ack_all(self):
        if self.is_transaction_completed:
            raise ValueError("transaction is already completed")
        request = QueuesDownstreamRequest()
        request.RequestID = str(uuid.uuid4())
        request.ClientID = self.receiver_client_id
        request.RequestTypeData = QueuesDownstreamRequestType.AckAll
        request.RefTransactionId = self.transaction_id
        request.SequenceRange.extend(self.active_offsets)
        if self.response_handler:
            self.response_handler(request)
        else:
            raise ValueError("response_handler is not set")

    def reject_all(self):
        if self.is_transaction_completed:
            raise ValueError("transaction is already completed")
        request = QueuesDownstreamRequest()
        request.RequestID = str(uuid.uuid4())
        request.ClientID = self.receiver_client_id
        request.RequestTypeData = QueuesDownstreamRequestType.NAckAll
        request.RefTransactionId = self.transaction_id
        request.SequenceRange.extend(self.active_offsets)
        if self.response_handler:
            result = self.response_handler(request)
            if result.IsError:
                raise ValueError(result.Error)
        else:
            raise ValueError("response_handler is not set")

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
        if self.response_handler:
            result = self.response_handler(request)
            if result.IsError:
                raise ValueError(result.Error)
        else:
            raise ValueError("response_handler is not set")

    @classmethod
    def decode(
        cls,
        response: QueuesDownstreamResponse,
        receiver_client_id: str,
        response_handler: Callable[[QueuesDownstreamRequest], QueuesDownstreamResponse],
    ) -> "QueuesPollResponse":
        messages = [
            QueueMessageReceived.decode(
                message,
                response.TransactionId,
                response.TransactionComplete,
                receiver_client_id,
                response_handler,
            )
            for message in response.Messages
        ]

        return cls(
            ref_request_id=response.RefRequestId,
            transaction_id=response.TransactionId,
            messages=messages,
            error=response.Error,
            is_error=response.IsError,
            is_transaction_completed=response.TransactionComplete,
            active_offsets=list(response.ActiveOffsets),
            receiver_client_id=receiver_client_id,
            response_handler=response_handler,
        )

    class Config:
        arbitrary_types_allowed = True
