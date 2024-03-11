import uuid
from typing import Dict
from kubemq.grpc import QueueMessage as pbQueueMessage
from kubemq.grpc import QueuesUpstreamRequest as pbQueuesUpstreamRequest
from kubemq.grpc import QueueMessagePolicy as pbQueueMessagePolicy


class QueueMessage:
    """A class representing a message in a queue.

    Attributes:
        id (str): The unique identifier for the message.
        channel (str): The channel of the message.
        metadata (str): The metadata associated with the message.
        body (bytes): The body of the message.
        tags (Dict[str, str]): The tags associated with the message.
        delay_in_seconds (int): The delay in seconds before the message becomes available in the queue.
        expiration_in_seconds (int): The expiration time in seconds for the message.
        attempts_before_dead_letter_queue (int): The number of receive attempts allowed for the message before it is moved to the dead letter queue.
        dead_letter_queue (str): The dead letter queue where the message will be moved after reaching the maximum receive attempts.

    Methods:
        validate(): Validates the message attributes and ensures that the required attributes are set.
        encode(client_id: str) -> pbQueuesUpstreamRequest: Encodes the message into a protocol buffer format for sending to the queue server.
        encode_message(client_id: str) -> pbQueueMessage: Encodes the message into a protocol buffer format.
    """
    def __init__(self, id: str = None,
                 channel: str = None,
                 metadata: str = None,
                 body: bytes = b"",
                 tags: Dict[str, str] = None,
                 delay_in_seconds: int = 0,
                 expiration_in_seconds: int = 0,
                 attempts_before_dead_letter_queue: int = 0,
                 dead_letter_queue: str = "",
                 ):
        self.id: str = id
        self.channel: str = channel
        self.metadata: str = metadata
        self.body: bytes = body
        self.tags: Dict[str, str] = tags if tags else {}
        self.delay_in_seconds: int = delay_in_seconds
        self.expiration_in_seconds: int = expiration_in_seconds
        self.attempts_before_dead_letter_queue: int = attempts_before_dead_letter_queue
        self.dead_letter_queue: str = dead_letter_queue

    def validate(self) -> 'QueueMessage':
        if not self.channel:
            raise ValueError("Queue message must have a channel.")

        if not self.metadata and not self.body and not self.tags:
            raise ValueError("Queue message must have at least one of the following: metadata, body, or tags.")

        if self.attempts_before_dead_letter_queue < 0:
            raise ValueError("Queue message attempts_before_dead_letter_queue must be a positive number.")

        if self.delay_in_seconds < 0:
            raise ValueError("Queue message delay_in_seconds must be a positive number.")

        if self.expiration_in_seconds < 0:
            raise ValueError("Queue message expiration_in_seconds must be a positive number.")

        return self

    def encode(self, client_id: str) -> pbQueuesUpstreamRequest:
        pb_queue_stream = pbQueuesUpstreamRequest()
        pb_queue_stream.RequestID = str(uuid.uuid4())
        pb_message = self.encode_message(client_id)
        pb_queue_stream.Messages.append(pb_message)
        return pb_queue_stream

    def encode_message(self, client_id: str) -> pbQueueMessage:
        pb_queue = pbQueueMessage()  # Assuming pb is an imported module
        pb_queue.MessageID = self.id or str(uuid.uuid4())
        pb_queue.ClientID = client_id
        pb_queue.Channel = self.channel
        pb_queue.Metadata = self.metadata or ""
        pb_queue.Body = self.body
        pb_queue.Tags.update(self.tags)
        pb_queue.Policy.DelaySeconds = self.delay_in_seconds or 0
        pb_queue.Policy.ExpirationSeconds = self.expiration_in_seconds or 0
        pb_queue.Policy.MaxReceiveCount = self.attempts_before_dead_letter_queue or 0
        pb_queue.Policy.MaxReceiveQueue = self.dead_letter_queue or ""
        return pb_queue

    def __repr__(self):
        return f"QueueMessage: id={self.id}, channel={self.channel}, metadata={self.metadata}, body={self.body}, tags={self.tags}, delay_in_seconds={self.delay_in_seconds}, expiration_in_seconds={self.expiration_in_seconds}, attempts_before_dead_letter_queue={self.attempts_before_dead_letter_queue}, dead_letter_queue={self.dead_letter_queue}"