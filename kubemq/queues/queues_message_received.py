import uuid
from typing import Dict, Callable
from datetime import datetime
from kubemq.grpc import QueueMessage as pbQueueMessage
from kubemq.grpc import QueuesDownstreamRequest, QueuesDownstreamRequestType, QueuesDownstreamResponse


class QueueMessageReceived:
    """
    Class representing a received queue message.

    Attributes:
        id (str): Unique identifier of the message.
        channel (str): Name of the channel the message belongs to.
        metadata (str): Additional metadata associated with the message.
        body (bytes): The content of the message in bytes.
        from_client_id (str): Identifier of the client who sent the message.
        tags (Dict[str, str]): Dictionary of key-value pairs representing tags associated with the message.
        timestamp (datetime): Timestamp indicating when the message was received.
        sequence (int): Sequential number indicating the order in which the message was received.
        receive_count (int): Number of times the message has been received.
        is_re_routed (bool): Flag indicating whether the message has been re-routed from another queue.
        re_route_from_queue (str): Name of the queue the message was re-routed from.
        expired_at (datetime): Timestamp indicating when the message will expire.
        delayed_to (datetime): Timestamp indicating when the message will be available for processing.
        transaction_id (str): Identifier of the transaction the message belongs to.
        is_transaction_completed (bool): Flag indicating whether the transaction is completed.
        response_handler (Callable[[QueuesDownstreamRequest], QueuesDownstreamResponse]): Callback function for sending downstream requests.
        receiver_client_id (str): Identifier of the client who received the message.

    Methods:
        ack():
            Sends an acknowledgment for the message.
            Raises a ValueError if the transaction is already completed.

        reject():
            Rejects the message.
            Raises a ValueError if the transaction is already completed or if the rejection fails.

        re_queue(channel: str):
            Re-queues the message to the specified channel.
            Raises a ValueError if the re-queue channel is empty or if the re-queue fails.

        decode(message: pbQueueMessage, transaction_id: str, transaction_is_completed: bool,
               receiver_client_id: str, response_handler: Callable[[QueuesDownstreamRequest], QueuesDownstreamResponse]) -> 'QueueMessageReceived':
            Decodes the given `pbQueueMessage` protobuf message and initialize the instance attributes.
            Returns the updated `QueueMessageReceived` object.

        __repr__() -> str:
            Returns a string representation of the `QueueMessageReceived` object.
    """
    def __init__(self):
        self.id: str = ""
        self.channel: str = ""
        self.metadata: str = ""
        self.body: bytes = b""
        self.from_client_id = ""
        self.tags: Dict[str, str] = {}
        self.timestamp: datetime = datetime.fromtimestamp(0)
        self.sequence: int = 0
        self.receive_count: int = 0
        self.is_re_routed: bool = False
        self.re_route_from_queue: str = ""
        self.expired_at: datetime = datetime.fromtimestamp(0)
        self.delayed_to: datetime = datetime.fromtimestamp(0)
        self.transaction_id: str = ""
        self.is_transaction_completed: bool = False
        self.response_handler: Callable[[QueuesDownstreamRequest], QueuesDownstreamResponse]
        self.receiver_client_id: str = ""

    def ack(self):
        if self.is_transaction_completed:
            raise ValueError("transaction is already completed")
        request = QueuesDownstreamRequest()
        request.RequestID = str(uuid.uuid4())
        request.ClientID = self.receiver_client_id
        request.Channel = self.channel
        request.RequestTypeData = QueuesDownstreamRequestType.AckRange
        request.RefTransactionId = self.transaction_id
        request.SequenceRange.append(self.sequence)
        self.response_handler(request)

    def reject(self):
        if self.is_transaction_completed:
            raise ValueError("transaction is already completed")
        request = QueuesDownstreamRequest()
        request.RequestID = str(uuid.uuid4())
        request.ClientID = self.receiver_client_id
        request.Channel = self.channel
        request.RequestTypeData = QueuesDownstreamRequestType.NAckRange
        request.RefTransactionId = self.transaction_id
        request.SequenceRange.append(self.sequence)
        result = self.response_handler(request)
        if result.IsError:
            raise ValueError(result.Error)

    def re_queue(self, channel: str):
        if not channel:
            raise ValueError("re-queue channel cannot be empty")
        request = QueuesDownstreamRequest()
        request.RequestID = str(uuid.uuid4())
        request.ClientID = self.receiver_client_id
        request.Channel = self.channel
        request.RequestTypeData = QueuesDownstreamRequestType.ReQueueRange
        request.RefTransactionId = self.transaction_id
        request.SequenceRange.append(self.sequence)
        request.ReQueueChannel = channel
        result = self.response_handler(request)
        if result.IsError:
            raise ValueError(result.Error)

    def decode(self, message: pbQueueMessage,
               transaction_id: str,
               transaction_is_completed: bool,
               receiver_client_id: str,
               response_handler: Callable[
                   [QueuesDownstreamRequest], QueuesDownstreamResponse]) -> 'QueueMessageReceived':
        self.id = message.MessageID
        self.channel = message.Channel
        self.metadata = message.Metadata
        self.body = message.Body
        self.from_client_id = message.ClientID
        for tag in message.Tags:
            self.tags[tag] = message.Tags[tag]
        if message.Attributes:
            self.timestamp = datetime.fromtimestamp(message.Attributes.Timestamp / 1e9)
            self.sequence = message.Attributes.Sequence
            self.receive_count = message.Attributes.ReceiveCount
            self.is_re_routed = message.Attributes.ReRouted
            self.re_route_from_queue = message.Attributes.ReRoutedFromQueue
            self.expired_at = datetime.fromtimestamp(message.Attributes.ExpirationAt / 1e6)
            self.delayed_to = datetime.fromtimestamp(message.Attributes.DelayedTo / 1e6)
        self.transaction_id = transaction_id
        self.is_transaction_completed = transaction_is_completed
        self.receiver_client_id = receiver_client_id
        self.response_handler = response_handler
        return self

    def __repr__(self):
        return f"QueueMessageReceived: id={self.id}, channel={self.channel}, metadata={self.metadata}, body={self.body}, from_client_id={self.from_client_id}, timestamp={self.timestamp}, sequence={self.sequence}, receive_count={self.receive_count}, is_re_routed={self.is_re_routed}, re_route_from_queue={self.re_route_from_queue}, expired_at={self.expired_at}, delayed_to={self.delayed_to}, transaction_id={self.transaction_id}, is_transaction_completed={self.is_transaction_completed}, tags={self.tags}"
