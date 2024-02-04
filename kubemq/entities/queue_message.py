import uuid
from typing import Dict
from kubemq.grpc import QueueMessage as pbQueueMessage
from kubemq.grpc import QueuesUpstreamRequest as pbQueuesUpstreamRequest
from kubemq.grpc import QueueMessagePolicy as pbQueueMessagePolicy


class QueueMessage:

    def __init__(self, id: str = None,
                 channel: str = None,
                 metadata: str = None,
                 body: bytes = None,
                 tags: Dict[str, str] = None,
                 delay_in_seconds: int = 0,
                 expiration_in_seconds: int = 0,
                 attempts_before_dead_letter_queue: int = 0,
                 dead_letter_queue: str = "",
                 ):
        self._id: str = id
        self._channel: str = channel
        self._metadata: str = metadata
        self._body: bytes = body
        self._tags: Dict[str, str] = tags if tags else {}
        self._delay_in_seconds: int = delay_in_seconds
        self._expiration_in_seconds: int = expiration_in_seconds
        self._attempts_before_dead_letter_queue: int = attempts_before_dead_letter_queue
        self._dead_letter_queue: str = dead_letter_queue

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
    def delay_in_seconds(self) -> int:
        return self._delay_in_seconds

    @property
    def expiration_in_seconds(self) -> int:
        return self._expiration_in_seconds

    @property
    def attempts_before_dead_letter_queue(self) -> int:
        return self._attempts_before_dead_letter_queue

    @property
    def dead_letter_queue(self) -> str:
        return self._dead_letter_queue

    def _validate(self) -> 'QueueMessage':
        if not self._channel:
            raise ValueError("Queue message must have a channel.")

        if not self._metadata and not self._body and not self._tags:
            raise ValueError("Queue message must have at least one of the following: metadata, body, or tags.")

        if self._attempts_before_dead_letter_queue < 0:
            raise ValueError("Queue message attempts_before_dead_letter_queue must be a positive number.")

        if self._delay_in_seconds < 0:
            raise ValueError("Queue message delay_in_seconds must be a positive number.")

        if self._expiration_in_seconds < 0:
            raise ValueError("Queue message expiration_in_seconds must be a positive number.")

        return self

    def _to_kubemq_queue_stream(self, client_id: str) -> pbQueuesUpstreamRequest:
        pb_queue_stream = pbQueuesUpstreamRequest()
        pb_queue_stream.RequestID = str(uuid.uuid4())
        pb_queue_stream.Messages.extend([self._to_kubemq_queue(client_id)])
        return pb_queue_stream

    def _to_kubemq_queue(self, client_id: str) -> pbQueueMessage:
        if not self._id:
            self._id = str(uuid.uuid4())
        pb_queue = pbQueueMessage()  # Assuming pb is an imported module
        pb_queue.MessageID = self._id
        pb_queue.ClientID = client_id
        pb_queue.Channel = self._channel
        pb_queue.Metadata = self._metadata or ""
        pb_queue.Body = self._body
        pb_queue.Tags.update(self._tags)
        pb_queue.Policy = pbQueueMessagePolicy(
            DelaySeconds=self._delay_in_seconds * 1000,
            ExpirationSeconds=self._expiration_in_seconds * 1000,
            MaxReceiveCount=self._attempts_before_dead_letter_queue,
            MaxReceiveQueue=self._dead_letter_queue
        )
        return pb_queue
