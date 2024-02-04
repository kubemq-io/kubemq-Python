import uuid
from typing import Callable
from kubemq.grpc import QueuesDownstreamRequest, QueuesDownstreamRequestType
from kubemq.entities.queue_message_received import QueueMessageReceived


class QueuesSubscription:

    def __init__(self, channel: str = None,
                 poll_max_messages: int = 1,
                 poll_wait_timeout_in_seconds: int = 60,
                 auto_ack_messages: bool = False,
                 on_receive_queue_callback: Callable[[QueueMessageReceived], None] = None,
                 on_error_callback: Callable[[str], None] = None):
        self._channel: str = channel
        self._poll_max_messages: int = poll_max_messages
        self._poll_wait_timeout_in_seconds: int = poll_wait_timeout_in_seconds
        self._auto_ack_messages: bool = auto_ack_messages
        self._on_receive_queue_callback = on_receive_queue_callback
        self._on_error_callback = on_error_callback

    @property
    def channel(self) -> str:
        return self._channel

    @property
    def auto_ack_messages(self) -> bool:
        return self._auto_ack_messages

    @property
    def poll_max_messages(self) -> int:
        return self._poll_max_messages

    @property
    def poll_wait_timeout_in_seconds(self) -> int:
        return self._poll_wait_timeout_in_seconds

    @property
    def on_receive_queue_callback(self) -> Callable[[QueueMessageReceived], None]:
        return self._on_receive_queue_callback

    @property
    def on_error_callback(self) -> Callable[[str], None]:
        return self._on_error_callback

    def raise_on_receive_message(self, received_queue: QueueMessageReceived):
        if self._on_receive_queue_callback:
            self._on_receive_queue_callback(received_queue)

    def raise_on_error(self, msg: str):
        if self._on_error_callback:
            self._on_error_callback(msg)

    def validate(self):
        if not self._channel:
            raise ValueError("queue subscription must have a channel.")
        if not self._on_receive_queue_callback:
            raise ValueError("queue subscription must have a on_receive_queue_callback function.")
        if self._poll_max_messages < 1:
            raise ValueError("queue subscription poll_max_messages must be greater than 0.")
        if self._poll_wait_timeout_in_seconds < 1:
            raise ValueError("queue subscription poll_wait_timeout_in_seconds must be greater than 0.")

    def to_queues_downstream_request(self, client_id: str = "") -> QueuesDownstreamRequest:
        request = QueuesDownstreamRequest()
        request.RequestID = str(uuid.uuid4())
        request.ClientID = client_id
        request.Channel = self._channel
        request.MaxItems = self._poll_max_messages
        request.WaitTimeout = self._poll_wait_timeout_in_seconds
        request.AutoAck = self._auto_ack_messages
        request.RequestTypeData = QueuesDownstreamRequestType.Get.value
        return request
