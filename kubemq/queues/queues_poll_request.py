import uuid
from typing import Callable
from kubemq.grpc import QueuesDownstreamRequest, QueuesDownstreamRequestType
from kubemq.queues.queues_message_received import QueueMessageReceived


class QueuesPollRequest:
    """
    Class representing a request to poll messages from a queue.

    Attributes:
        channel (str): The channel to subscribe to.
        poll_max_messages (int): The maximum number of messages to poll in a single request.
        poll_wait_timeout_in_seconds (int): The maximum time to wait for messages in seconds.
        auto_ack_messages (bool): Whether to automatically acknowledge received messages.

    Methods:
        validate(): Validates the request parameters.
        encode(client_id: str) -> QueuesDownstreamRequest: Encodes the request into a downstream request object.
        __repr__(): Returns a string representation of the QueuesPollRequest object.
    """
    def __init__(self, channel: str = None,
                 poll_max_messages: int = 1,
                 poll_wait_timeout_in_seconds: int = 60,
                 auto_ack_messages: bool = False,
                 ):
        self.channel: str = channel
        self.poll_max_messages: int = poll_max_messages
        self.poll_wait_timeout_in_seconds: int = poll_wait_timeout_in_seconds
        self.auto_ack_messages: bool = auto_ack_messages

    def validate(self):
        if not self.channel:
            raise ValueError("queue subscription must have a channel.")
        if self.poll_max_messages < 1:
            raise ValueError("queue subscription poll_max_messages must be greater than 0.")
        if self.poll_wait_timeout_in_seconds < 1:
            raise ValueError("queue subscription poll_wait_timeout_in_seconds must be greater than 0.")

    def encode(self, client_id: str = "") -> QueuesDownstreamRequest:
        request = QueuesDownstreamRequest()
        request.RequestID = str(uuid.uuid4())
        request.ClientID = client_id
        request.Channel = self.channel
        request.MaxItems = self.poll_max_messages
        request.WaitTimeout = self.poll_wait_timeout_in_seconds * 1000
        request.AutoAck = self.auto_ack_messages
        request.RequestTypeData = QueuesDownstreamRequestType.Get
        return request

    def __repr__(self):
        return f"QueuesSubscription: channel={self.channel}, poll_max_messages={self.poll_max_messages}, " \
               f"poll_wait_timeout_in_seconds={self.poll_wait_timeout_in_seconds}, auto_ack_messages={self.auto_ack_messages}"