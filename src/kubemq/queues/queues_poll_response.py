from __future__ import annotations

import dataclasses
import logging
import threading
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

from kubemq.grpc import (
    QueuesDownstreamRequest,
    QueuesDownstreamRequestType,
    QueuesDownstreamResponse,
)
from kubemq.queues.queues_message_received import QueueMessageReceived


# Define a protocol for the response handler
class ResponseHandlerProtocol(Protocol):
    """Protocol for response handler callables."""

    def __call__(self, request: QueuesDownstreamRequest) -> QueuesDownstreamResponse:
        """Handle a downstream request and return a response."""


@dataclass
class QueuesPollResponse:
    """Represents a response from polling messages from a queue.

    Thread Safety:
        Safe to read from multiple threads. Internal lock protects
        ack/reject operations.
    """

    ref_request_id: str = ""
    transaction_id: str = ""
    messages: list[QueueMessageReceived] = field(default_factory=list)
    error: str = ""
    is_error: bool = False
    is_transaction_completed: bool = False
    active_offsets: list[int] = field(default_factory=list)
    receiver_client_id: str = ""
    response_handler: Callable[[QueuesDownstreamRequest], QueuesDownstreamResponse] | None = None
    is_auto_acked: bool = False
    request_type_data: int = 0
    response_metadata: dict[str, str] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    # Public API methods
    def ack_all(self) -> None:
        """Acknowledge all messages in the response."""
        self._do_operation(QueuesDownstreamRequestType.AckAll)

    def reject_all(self) -> None:
        """Reject all messages in the response."""
        self._do_operation(QueuesDownstreamRequestType.NAckAll)

    def re_queue_all(self, channel: str) -> None:
        """Re-queue all messages in the response to another channel."""
        self._do_operation(QueuesDownstreamRequestType.ReQueueAll, channel)

    def get_active_offsets(self) -> list[int]:
        """Query the server for the current active offsets of this transaction."""
        response = self._do_operation_with_response(QueuesDownstreamRequestType.ActiveOffsets)
        if response is not None:
            return list(response.ActiveOffsets)
        return list(self.active_offsets)

    def get_transaction_status(self) -> bool:
        """Query the server for the transaction completion status."""
        response = self._do_operation_with_response(QueuesDownstreamRequestType.TransactionStatus)
        if response is not None:
            return response.TransactionComplete
        return self.is_transaction_completed

    def close_transaction(self) -> None:
        """Close the transaction by sending a CloseByClient request."""
        self._do_operation(QueuesDownstreamRequestType.CloseByClient)

    # Utility methods
    def count(self) -> int:
        """Get the number of messages in the response."""
        return len(self.messages)

    def is_empty(self) -> bool:
        """Check if the response contains no messages."""
        return len(self.messages) == 0

    def with_updates(self, **kwargs: Any) -> QueuesPollResponse:
        """Create a new response with updated values."""
        with self._lock:
            return dataclasses.replace(self, **kwargs)

    # Internal helper methods
    def _do_operation_with_response(
        self,
        request_type: QueuesDownstreamRequestType,
        re_queue_channel: str = "",
        metadata: dict[str, str] | None = None,
    ) -> QueuesDownstreamResponse | None:
        """Perform an operation and return the server response."""
        with self._lock:
            if not self.response_handler:
                raise ValueError("Response handler is not set")

            try:
                request = QueuesDownstreamRequest()
                request.RequestID = str(uuid.uuid4())
                request.ClientID = self.receiver_client_id
                request.RequestTypeData = request_type
                request.ReQueueChannel = re_queue_channel
                request.RefTransactionId = self.transaction_id
                request.SequenceRange.extend(self.active_offsets)
                if metadata:
                    for k, v in metadata.items():
                        request.Metadata[k] = v
                return self.response_handler(request)
            except Exception as e:
                logging.error(f"Error performing operation {request_type}: {str(e)}")
                raise ValueError(f"Failed to perform operation: {str(e)}") from e

    def _do_operation(
        self,
        request_type: QueuesDownstreamRequestType,
        re_queue_channel: str = "",
        metadata: dict[str, str] | None = None,
    ) -> None:
        """Perform an operation on all messages in the response."""
        with self._lock:
            if self.is_auto_acked:
                raise ValueError(
                    "Transaction was set with auto ack, transaction operations are not allowed"
                )
            if self.is_transaction_completed:
                raise ValueError("Transaction is already completed")
            if not self.response_handler:
                raise ValueError("Response handler is not set")

            try:
                request = QueuesDownstreamRequest()
                request.RequestID = str(uuid.uuid4())
                request.ClientID = self.receiver_client_id
                request.RequestTypeData = request_type
                request.ReQueueChannel = re_queue_channel
                request.RefTransactionId = self.transaction_id
                request.SequenceRange.extend(self.active_offsets)
                if metadata:
                    for k, v in metadata.items():
                        request.Metadata[k] = v
                self.response_handler(request)
                self.is_transaction_completed = True
                for message in self.messages:
                    message._mark_transaction_completed()
            except Exception as e:
                logging.error(f"Error performing operation {request_type}: {str(e)}")
                raise ValueError(f"Failed to perform operation: {str(e)}") from e

    # Decoding methods
    @classmethod
    def decode(
        cls,
        response: QueuesDownstreamResponse,
        receiver_client_id: str,
        response_handler: Callable[[QueuesDownstreamRequest], QueuesDownstreamResponse],
        request_auto_ack: bool = False,
    ) -> QueuesPollResponse:
        """Create a QueuesPollResponse from a protobuf QueuesDownstreamResponse."""
        if not response:
            raise ValueError("Cannot decode None response")

        try:
            messages = [
                QueueMessageReceived.decode(
                    message,
                    response.TransactionId,
                    response.TransactionComplete,
                    receiver_client_id,
                    response_handler,
                    is_auto_acked=request_auto_ack,
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
                is_auto_acked=request_auto_ack,
                request_type_data=int(response.RequestTypeData) if response.RequestTypeData else 0,
                response_metadata=dict(response.Metadata)
                if hasattr(response, "Metadata") and response.Metadata
                else {},
            )
        except Exception as e:
            raise ValueError(f"Failed to decode response: {str(e)}") from e

    # String representations
    def __str__(self) -> str:
        """Get a string representation of the poll response."""
        try:
            return (
                f"QueuesPollResponse: ref_request_id={self.ref_request_id}, "
                f"transaction_id={self.transaction_id}, "
                f"message_count={len(self.messages)}, "
                f"is_error={self.is_error}, "
                f"error={self.error if self.is_error else 'None'}, "
                f"is_transaction_completed={self.is_transaction_completed}"
            )
        except Exception as e:
            return f"QueuesPollResponse: [Error displaying response: {str(e)}]"

    def __repr__(self) -> str:
        """Get a detailed representation of the poll response."""
        return (
            f"QueuesPollResponse(ref_request_id={self.ref_request_id!r}, "
            f"transaction_id={self.transaction_id!r}, "
            f"messages={self.messages!r}, "
            f"error={self.error!r}, "
            f"is_error={self.is_error}, "
            f"is_transaction_completed={self.is_transaction_completed}, "
            f"active_offsets={self.active_offsets!r}, "
            f"receiver_client_id={self.receiver_client_id!r}, "
            f"is_auto_acked={self.is_auto_acked})"
        )
