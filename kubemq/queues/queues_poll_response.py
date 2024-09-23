from pydantic import BaseModel, Field
from typing import List, Callable, Optional
import uuid
from kubemq.grpc import (
    QueuesDownstreamRequest,
    QueuesDownstreamResponse,
    QueuesDownstreamRequestType,
)
import threading
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
    visibility_seconds: int = Field(default=0)
    is_auto_acked: bool = Field(default=False)

    def __init__(self, **data):
        super().__init__(**data)
        self._lock = threading.Lock()

    def ack_all(self):
        self._do_operation(QueuesDownstreamRequestType.AckAll)

    def reject_all(self):
        self._do_operation(QueuesDownstreamRequestType.NAckAll)

    def re_queue_all(self, channel: str):
        self._do_operation(QueuesDownstreamRequestType.ReQueueAll, channel)

    def _do_operation(self, request_type: QueuesDownstreamRequestType, re_queue_channel: str = ""):
        if self.is_auto_acked:
            raise ValueError("transaction was set with auto ack, transaction operations are not allowed")
        if self.is_transaction_completed:
            raise ValueError("transaction is already completed")
        if not self.response_handler:
            raise ValueError("response_handler is not set")
        with self._lock:
            request = QueuesDownstreamRequest()
            request.RequestID = str(uuid.uuid4())
            request.ClientID = self.receiver_client_id
            request.RequestTypeData = request_type
            request.ReQueueChannel = re_queue_channel
            request.RefTransactionId = self.transaction_id
            request.SequenceRange.extend(self.active_offsets)
            self.response_handler(request)
            self.is_transaction_completed=True
            for message in self.messages:
                message._mark_transaction_completed()

    @classmethod
    def decode(
        cls,
        response: QueuesDownstreamResponse,
        receiver_client_id: str,
        response_handler: Callable[[QueuesDownstreamRequest], QueuesDownstreamResponse],
        request_visibility_seconds: int = 0,
        request_auto_ack: bool = False,
    ) -> "QueuesPollResponse":
        messages = [
            QueueMessageReceived.decode(
                message,
                response.TransactionId,
                response.TransactionComplete,
                receiver_client_id,
                response_handler,
                visibility_seconds=request_visibility_seconds,
                is_auto_acked=request_auto_ack
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
            visibility_seconds=request_visibility_seconds,
            is_auto_acked=request_auto_ack,
        )

    class Config:
        arbitrary_types_allowed = True

    def __str__(self):
        return f"QueuesPollResponse: ref_request_id={self.ref_request_id}, transaction_id={self.transaction_id}, messages={self.messages}, error={self.error}, is_error={self.is_error}, is_transaction_completed={self.is_transaction_completed}, active_offsets={self.active_offsets}, receiver_client_id={self.receiver_client_id}, response_handler={self.response_handler}, visibility_seconds={self.visibility_seconds}, is_auto_acked={self.is_auto_acked}"