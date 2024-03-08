import datetime
from typing import Dict
from kubemq.grpc import Request as pbRequest


class CommandMessageReceived:
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
