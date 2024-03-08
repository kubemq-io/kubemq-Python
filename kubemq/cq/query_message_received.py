from datetime import datetime, timedelta
from typing import Dict
from kubemq.grpc import Request as pbRequest


class QueryMessageReceived:
    def __init__(self):
        self.id: str = ""
        self.from_client_id: str = ""
        self.timestamp: datetime = datetime.fromtimestamp(0)
        self.channel: str = ""
        self.metadata: str = ""
        self.body: bytes = b""
        self.reply_channel: str = ""
        self.tags: Dict[str, str] = {}

    @staticmethod
    def decode(query_receive: pbRequest) -> 'QueryMessageReceived':
        message = QueryMessageReceived()
        message.id = query_receive.RequestID
        message.from_client_id = query_receive.ClientID
        message.timestamp = datetime.now()
        message.channel = query_receive.Channel
        message.metadata = query_receive.Metadata
        message.body = query_receive.Body
        message.reply_channel = query_receive.ReplyChannel
        message.tags = query_receive.Tags
        return message

    def __repr__(self):
        return f"QueryMessageReceived: id={self.id}, channel={self.channel}, metadata={self.metadata}, body={self.body}, from_client_id={self.from_client_id}, timestamp={self.timestamp}, reply_channel={self.reply_channel}, tags={self.tags}"
