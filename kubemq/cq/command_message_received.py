import datetime
from typing import Dict
from kubemq.grpc import Request as pbRequest


class CommandMessageReceived:
    """
    CommandMessageReceived class represents a received command message.

    Attributes:
        id (str): The ID of the command message.
        from_client_id (str): The ID of the client that sent the command message.
        timestamp (datetime): The timestamp when the command message was received.
        channel (str): The channel on which the command message was received.
        metadata (str): Additional metadata associated with the command message.
        body (bytes): The body of the command message.
        reply_channel (str): The channel to which any reply to the command message should be sent.
        tags (Dict[str, str]): Additional tags associated with the command message.

    Methods:
        decode(command_receive: pbRequest) -> CommandMessageReceived:
            Static method that takes a pbRequest object as input and decodes it into a CommandMessageReceived object.

        __repr__() -> str:
            Returns a string representation of the CommandMessageReceived object.

    """
    def __init__(self):
        self.id: str = ""
        self.from_client_id: str = ""
        self.timestamp: datetime = datetime.datetime.fromtimestamp(0)
        self.channel: str = ""
        self.metadata: str = ""
        self.body: bytes = b""
        self.reply_channel: str = ""
        self.tags: Dict[str, str] = {}

    @staticmethod
    def decode(command_receive: pbRequest) -> 'CommandMessageReceived':
        message = CommandMessageReceived()
        message.id = command_receive.RequestID
        message.from_client_id = command_receive.ClientID
        message.timestamp = datetime.datetime.now()
        message.channel = command_receive.Channel
        message.metadata = command_receive.Metadata
        message.body = command_receive.Body
        message.reply_channel = command_receive.ReplyChannel
        return message

    def __repr__(self):
        return f"CommandMessageReceived: id={self.id}, channel={self.channel}, metadata={self.metadata}, body={self.body}, from_client_id={self.from_client_id}, timestamp={self.timestamp}, reply_channel={self.reply_channel}, tags={self.tags}"
