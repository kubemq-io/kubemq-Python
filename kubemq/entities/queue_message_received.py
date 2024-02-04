import uuid
from typing import Dict
from datetime import datetime
from kubemq.grpc import QueueMessage as pbQueueMessage

class QueueMessageReceived:

    def __init__(self):
        self._id: str = ""
        self._channel: str = ""
        self._metadata: str = ""
        self._body: bytes = b""
        self._tags: Dict[str, str] = {}
        self._timestamp: datetime = datetime.fromtimestamp(0)
        self._sequence: int = 0
        self._receive_count: int = 0
        self._is_re_routed: bool = False
        self._re_route_from_queue: str = ""
        self._expired_at: datetime = datetime.fromtimestamp(0)
        self._delayed_to: datetime = datetime.fromtimestamp(0)
        self._transaction_id: str = ""

    @property
    def id(self) -> str:
        return self._id

    @property
    def channel(self) -> str:
        return self._channel

    @property
    def metadata(self) -> str:
        return self._metadata

    @property
    def body(self) -> bytes:
        return self._body

    @property
    def tags(self) -> Dict[str, str]:
        return self._tags

    @property
    def timestamp(self) -> datetime:
        return self._timestamp

    @property
    def sequence(self) -> int:
        return self._sequence

    @property
    def receive_count(self) -> int:
        return self._receive_count

    @property
    def is_re_routed(self) -> bool:
        return self._is_re_routed

    @property
    def re_route_from_queue(self) -> str:
        return self._re_route_from_queue

    @property
    def expired_at(self) -> datetime:
        return self._expired_at

    @property
    def delayed_to(self) -> datetime:
        return self._delayed_to

    def from_kubemq_queue(self, pb_queue: pbQueueMessage) -> 'QueueMessageReceived':
        self._id = pb_queue.MessageID
        self._channel = pb_queue.Channel
        self._metadata = pb_queue.Metadata
        self._body = pb_queue.Body
        for tag in pb_queue.Tags:
            self._tags[tag] = pb_queue.Tags[tag]
        if pb_queue.Attributes:
            self._timestamp = datetime.fromtimestamp(pb_queue.Attributes.Timestamp/1e9)
            self._sequence = pb_queue.Attributes.Sequence
            self._receive_count = pb_queue.Attributes.ReceiveCount
            self._is_re_routed = pb_queue.Attributes.ReRouted
            self._re_route_from_queue = pb_queue.Attributes.ReRoutedFromQueue
            self._expired_at = datetime.fromtimestamp(pb_queue.Attributes.ExpirationAt/1e9)
            self._delayed_to = datetime.fromtimestamp(pb_queue.Attributes.DelayedTo/1e9)
        return self
