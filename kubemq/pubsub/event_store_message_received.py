from datetime import datetime, timedelta
from typing import Dict, ByteString
from kubemq.grpc import EventReceive as pbEventReceive


class EventStoreMessageReceived:
    def __init__(self):
        self.id: str = ""
        self.from_client_id: str = ""
        self.timestamp: datetime = datetime.fromtimestamp(0)
        self.channel: str = ""
        self.metadata: str = ""
        self.body: bytes = b""
        self.sequence: int = 0
        self.tags: Dict[str, str] = {}

    @staticmethod
    def decode(event_receive: pbEventReceive) -> 'EventStoreMessageReceived':
        from_client_id = event_receive.Tags.get("x-kubemq-client-id", "") if event_receive.Tags else ""
        tags = event_receive.Tags if event_receive.Tags else {}
        message = EventStoreMessageReceived()
        message.id = event_receive.EventID
        message.from_client_id = from_client_id
        message.timestamp = datetime.fromtimestamp(event_receive.Timestamp / 1e9)
        message.channel = event_receive.Channel
        message.metadata = event_receive.Metadata
        message.body = event_receive.Body
        message.sequence = event_receive.Sequence
        message.tags = tags
        return message

    def __repr__(self):
        return f"EventStoreMessageReceived: id={self.id}, channel={self.channel}, metadata={self.metadata}, body={self.body}, from_client_id={self.from_client_id}, timestamp={self.timestamp}, sequence={self.sequence}, tags={self.tags}"
