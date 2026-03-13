from __future__ import annotations

import logging
import threading
import uuid
from typing import Callable, Protocol

from pydantic import BaseModel, ConfigDict, Field, field_validator

from kubemq.grpc import (
    QueuesDownstreamRequest,
    QueuesDownstreamRequestType,
    QueuesDownstreamResponse,
)
from kubemq.queues.queues_message_received import QueueMessageReceived


# Define a protocol for the response handler
class ResponseHandlerProtocol(Protocol):
    def __call__(self, request: QueuesDownstreamRequest) -> QueuesDownstreamResponse: ...


class QueuesPollResponse(BaseModel):
    """
    Represents a response from polling messages from a queue.

    This class encapsulates the response received when polling messages from a KubeMQ queue.
    It provides methods for acknowledging, rejecting, and re-queuing messages, as well as
    utility methods for working with the received messages.

    Attributes:
        ref_request_id: The ID of the request that this response is for.
        transaction_id: The ID of the transaction.
        messages: The list of messages received from the queue.
        error: The error message if an error occurred.
        is_error: Whether an error occurred.
        is_transaction_completed: Whether the transaction is completed.
        active_offsets: The list of active offsets for the messages.
        receiver_client_id: The client ID of the receiver.
        response_handler: The handler for sending responses.
        visibility_seconds: The visibility time in seconds for the messages.
        is_auto_acked: Whether the messages are automatically acknowledged.

    Examples:
        ```python
        # Process messages from a poll response
        response = client.receive_queues_messages(channel="my-queue", max_messages=10)

        # Check if there was an error
        if response.is_error:
            print(f"Error: {response.error}")
        else:
            # Process the messages
            for message in response.messages:
                process_message(message)

            # Acknowledge all messages
            response.ack_all()
        ```

    Thread Safety:
        Safe to read from multiple threads. Internal lock protects
        ack/reject operations.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    # Instance attributes
    ref_request_id: str = Field(
        default="", description="The ID of the request that this response is for"
    )
    transaction_id: str = Field(default="", description="The ID of the transaction")
    messages: list[QueueMessageReceived] = Field(
        default_factory=list, description="The list of messages received from the queue"
    )
    error: str = Field(default="", description="The error message if an error occurred")
    is_error: bool = Field(default=False, description="Whether an error occurred")
    is_transaction_completed: bool = Field(
        default=False, description="Whether the transaction is completed"
    )
    active_offsets: list[int] = Field(
        default_factory=list, description="The list of active offsets for the messages"
    )
    receiver_client_id: str = Field(default="", description="The client ID of the receiver")
    response_handler: Callable[[QueuesDownstreamRequest], QueuesDownstreamResponse] | None = Field(
        default=None, description="The handler for sending responses"
    )
    visibility_seconds: int = Field(
        default=0, description="The visibility time in seconds for the messages"
    )
    is_auto_acked: bool = Field(
        default=False, description="Whether the messages are automatically acknowledged"
    )
    request_type_data: int = Field(
        default=0, description="The request type from the server response"
    )
    response_metadata: dict[str, str] = Field(
        default_factory=dict, description="Metadata from the server response"
    )

    # Constructor
    def __init__(self, **data):
        super().__init__(**data)
        self._lock = threading.Lock()

    # Validators
    @field_validator("visibility_seconds")
    def validate_visibility_seconds(cls, v: int) -> int:
        """
        Validate that the visibility seconds is within acceptable limits.

        Args:
            v: The visibility seconds value to validate

        Returns:
            The validated visibility seconds value

        Raises:
            ValueError: If the visibility seconds is negative
        """
        if v < 0:
            raise ValueError("Visibility seconds cannot be negative")
        return v

    # Public API methods
    def ack_all(self) -> None:
        """
        Acknowledge all messages in the response.

        This method sends an acknowledgment to the server for all messages,
        indicating that they have been successfully processed and can be
        removed from the queue.

        Raises:
            ValueError: If the messages are auto-acknowledged, the transaction is already
                       completed, or the response handler is not set.
        """
        self._do_operation(QueuesDownstreamRequestType.AckAll)

    def reject_all(self) -> None:
        """
        Reject all messages in the response.

        This method sends a rejection to the server for all messages,
        indicating that they could not be processed and should be
        handled according to the server's configuration.

        Raises:
            ValueError: If the messages are auto-acknowledged, the transaction is already
                       completed, or the response handler is not set.
        """
        self._do_operation(QueuesDownstreamRequestType.NAckAll)

    def re_queue_all(self, channel: str) -> None:
        """
        Re-queue all messages in the response to another channel.

        This method sends a request to re-queue all messages to another channel.

        Args:
            channel: The channel to re-queue the messages to.

        Raises:
            ValueError: If the messages are auto-acknowledged, the transaction is already
                       completed, or the response handler is not set.
        """
        self._do_operation(QueuesDownstreamRequestType.ReQueueAll, channel)

    def get_active_offsets(self) -> list[int]:
        """Query the server for the current active offsets of this transaction.

        Returns:
            List of active offset values.
        """
        response = self._do_operation_with_response(QueuesDownstreamRequestType.ActiveOffsets)
        if response is not None:
            return list(response.ActiveOffsets)
        return list(self.active_offsets)

    def get_transaction_status(self) -> bool:
        """Query the server for the transaction completion status.

        Returns:
            True if the transaction is complete, False otherwise.
        """
        response = self._do_operation_with_response(QueuesDownstreamRequestType.TransactionStatus)
        if response is not None:
            return response.TransactionComplete
        return self.is_transaction_completed

    def close_transaction(self) -> None:
        """Close the transaction by sending a CloseByClient request."""
        self._do_operation(QueuesDownstreamRequestType.CloseByClient)

    # Utility methods
    def count(self) -> int:
        """
        Get the number of messages in the response.

        Returns:
            The number of messages in the response.
        """
        return len(self.messages)

    def is_empty(self) -> bool:
        """
        Check if the response contains no messages.

        Returns:
            True if the response contains no messages, False otherwise.
        """
        return len(self.messages) == 0

    def with_updates(self, **kwargs) -> QueuesPollResponse:
        """
        Create a new response with updated values.

        Args:
            **kwargs: The fields to update and their new values

        Returns:
            A new instance with the updated values
        """
        with self._lock:
            data = self.model_dump()
            data.update(kwargs)
            return self.__class__(**data)

    # Internal helper methods
    def _do_operation_with_response(
        self,
        request_type: QueuesDownstreamRequestType,
        re_queue_channel: str = "",
        metadata: dict[str, str] | None = None,
    ) -> QueuesDownstreamResponse | None:
        """Perform an operation and return the server response.

        Returns:
            The QueuesDownstreamResponse from the server, or None on error.
        """
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
        """
        Perform an operation on all messages in the response.

        Args:
            request_type: The type of operation to perform.
            re_queue_channel: The channel to re-queue the messages to (if applicable).
            metadata: Optional key-value metadata to attach to the downstream request.

        Raises:
            ValueError: If the messages are auto-acknowledged, the transaction is already
                       completed, or the response handler is not set.

        Thread Safety:
            This method is thread-safe.
        """
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
        request_visibility_seconds: int = 0,
        request_auto_ack: bool = False,
    ) -> QueuesPollResponse:
        """
        Create a QueuesPollResponse from a protobuf QueuesDownstreamResponse.

        Args:
            response: The protobuf response to decode
            receiver_client_id: The client ID of the receiver
            response_handler: The handler for sending responses
            request_visibility_seconds: The visibility time in seconds for the messages
            request_auto_ack: Whether the messages are automatically acknowledged

        Returns:
            A new QueuesPollResponse instance

        Raises:
            ValueError: If the response is invalid
        """
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
                    visibility_seconds=request_visibility_seconds,
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
                visibility_seconds=request_visibility_seconds,
                is_auto_acked=request_auto_ack,
                request_type_data=int(response.RequestTypeData) if response.RequestTypeData else 0,
                response_metadata=dict(response.Metadata) if hasattr(response, "Metadata") and response.Metadata else {},
            )
        except Exception as e:
            raise ValueError(f"Failed to decode response: {str(e)}") from e

    # String representations
    def __str__(self) -> str:
        """
        Get a string representation of the poll response.

        Returns:
            A string representation of the poll response
        """
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
        """
        Get a detailed representation of the poll response.

        Returns:
            A detailed representation of the poll response
        """
        return (
            f"QueuesPollResponse(ref_request_id={self.ref_request_id!r}, "
            f"transaction_id={self.transaction_id!r}, "
            f"messages={self.messages!r}, "
            f"error={self.error!r}, "
            f"is_error={self.is_error}, "
            f"is_transaction_completed={self.is_transaction_completed}, "
            f"active_offsets={self.active_offsets!r}, "
            f"receiver_client_id={self.receiver_client_id!r}, "
            f"visibility_seconds={self.visibility_seconds}, "
            f"is_auto_acked={self.is_auto_acked})"
        )
