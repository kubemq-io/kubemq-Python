from pydantic import BaseModel, Field
from typing import Dict, Callable, Optional
from datetime import datetime
from kubemq.grpc import QueueMessage as pbQueueMessage
from kubemq.grpc import (
    QueuesDownstreamRequest,
    QueuesDownstreamRequestType,
    QueuesDownstreamResponse,
)
import uuid
import threading


class QueueMessageReceived(BaseModel):
    id: str = Field(default="")
    channel: str = Field(default="")
    metadata: str = Field(default="")
    body: bytes = Field(default=b"")
    from_client_id: str = Field(default="")
    tags: Dict[str, str] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=lambda: datetime.fromtimestamp(0))
    sequence: int = Field(default=0)
    receive_count: int = Field(default=0)
    is_re_routed: bool = Field(default=False)
    re_route_from_queue: str = Field(default="")
    expired_at: datetime = Field(default_factory=lambda: datetime.fromtimestamp(0))
    delayed_to: datetime = Field(default_factory=lambda: datetime.fromtimestamp(0))
    transaction_id: str = Field(default="")
    is_transaction_completed: bool = Field(default=False)
    receiver_client_id: str = Field(default="")
    response_handler: Optional[
        Callable[[QueuesDownstreamRequest], QueuesDownstreamResponse]
    ] = Field(default=None)
    visibility_seconds: int = Field(default=0)
    is_auto_acked: bool = Field(default=False)
    _visibility_timer: Optional[threading.Timer] = None
    _message_completed: bool = False
    _timer_expired: bool = False

    def __init__(self, **data):
        super().__init__(**data)
        self._lock = threading.Lock()

    def ack(self):
        self._do_operation(QueuesDownstreamRequestType.AckRange)

    def reject(self):
        self._do_operation(QueuesDownstreamRequestType.NAckRange)


    def re_queue(self, channel: str):
        self._do_operation(QueuesDownstreamRequestType.ReQueueRange, channel)

    @classmethod
    def decode(
            cls,
            message: pbQueueMessage,
            transaction_id: str,
            transaction_is_completed: bool,
            receiver_client_id: str,
            response_handler: Callable[[QueuesDownstreamRequest], QueuesDownstreamResponse],
            visibility_seconds: int,
            is_auto_acked: bool,
    ) -> "QueueMessageReceived":
        instance = cls(
            id=message.MessageID,
            channel=message.Channel,
            metadata=message.Metadata,
            body=message.Body,
            from_client_id=message.ClientID,
            tags={tag: message.Tags[tag] for tag in message.Tags},
            timestamp=(
                datetime.fromtimestamp(message.Attributes.Timestamp / 1e9)
                if message.Attributes
                else datetime.fromtimestamp(0)
            ),
            sequence=message.Attributes.Sequence if message.Attributes else 0,
            receive_count=message.Attributes.ReceiveCount if message.Attributes else 0,
            is_re_routed=message.Attributes.ReRouted if message.Attributes else False,
            re_route_from_queue=(
                message.Attributes.ReRoutedFromQueue if message.Attributes else ""
            ),
            expired_at=(
                datetime.fromtimestamp(message.Attributes.ExpirationAt / 1e6)
                if message.Attributes
                else datetime.fromtimestamp(0)
            ),
            delayed_to=(
                datetime.fromtimestamp(message.Attributes.DelayedTo / 1e6)
                if message.Attributes
                else datetime.fromtimestamp(0)
            ),
            transaction_id=transaction_id,
            is_transaction_completed=transaction_is_completed,
            receiver_client_id=receiver_client_id,
            response_handler=response_handler,
            visibility_seconds=visibility_seconds,
            is_auto_acked=is_auto_acked
        )

        if instance.visibility_seconds > 0:
            instance._start_visibility_timer()

        return instance

    def _do_operation(self, request_type: QueuesDownstreamRequestType, re_queue_channel: str = ""):
        if self.is_auto_acked:
            raise ValueError("transaction was set with auto ack, message operations are not allowed")
        if self.is_transaction_completed:
            raise ValueError("transaction is already completed")
        if self._message_completed:
            raise ValueError("message transaction is already completed")
        if not self.response_handler:
            raise ValueError("response_handler is not set")
        with self._lock:
            request = QueuesDownstreamRequest()
            request.RequestID = str(uuid.uuid4())
            request.ClientID = self.receiver_client_id
            request.Channel = self.channel
            request.RequestTypeData = request_type
            request.RefTransactionId = self.transaction_id
            request.SequenceRange.append(self.sequence)
            request.ReQueueChannel = re_queue_channel
            self._message_completed = True
            if self._visibility_timer and not self._timer_expired:
                self._visibility_timer.cancel()
            self.response_handler(request)

    def _start_visibility_timer(self):
        with self._lock:
            self._visibility_timer = threading.Timer(self.visibility_seconds, self._on_visibility_expired)
            self._visibility_timer.start()


    def _on_visibility_expired(self):
        with self._lock:
            self._timer_expired = True
            self._visibility_timer = None
        self.reject()
        raise ValueError("message visibility expired")

    def extend_visibility_timer(self, additional_seconds: int):
        if additional_seconds <= 0:
            raise ValueError("additional_seconds must be greater than 0")
        if not self._visibility_timer:
            raise ValueError("message visibility was not set for this transaction")
        if self._timer_expired:
            raise ValueError("message visibility expired, cannot perform operation")
        remaining_time = self._visibility_timer.interval - self._visibility_timer.interval
        new_duration = remaining_time + additional_seconds
        with self._lock:
            if self._message_completed:
                raise ValueError("message transaction is already completed")
            self._visibility_timer.cancel()
            self._visibility_timer = threading.Timer(new_duration, self._on_visibility_expired)
            self._visibility_timer.start()

    def _mark_transaction_completed(self):
        with self._lock:
            self._message_completed = True
            self.is_transaction_completed=True
            if self._visibility_timer:
                self._visibility_timer.cancel()
    class Config:
        arbitrary_types_allowed = True

    def __str__(self):
        return (
            f"QueueMessageReceived: id={self.id}, channel={self.channel}, metadata={self.metadata}, "
            f"from_client_id={self.from_client_id}, tags={self.tags}, timestamp={self.timestamp}, "
            f"sequence={self.sequence}, receive_count={self.receive_count}, is_re_routed={self.is_re_routed}, "
            f"re_route_from_queue={self.re_route_from_queue}, expired_at={self.expired_at}, "
            f"delayed_to={self.delayed_to}, transaction_id={self.transaction_id}, "
            f"is_transaction_completed={self.is_transaction_completed}, receiver_client_id={self.receiver_client_id}, "
            f"visibility_seconds={self.visibility_seconds}, is_auto_acked={self.is_auto_acked}"
        )