from datetime import datetime
from typing import Dict
from kubemq.grpc import EventReceive as pbEventReceive


class EventMessageReceived:
    def __init__(self):
        self.id: str = ""
        self.from_client_id: str = ""
        self.timestamp: datetime = datetime.now()
        self.channel: str = ""
        self.metadata: str = ""
        self.body: bytes = b""
        self.tags: Dict[str, str] = {}

    @staticmethod
    def decode(event_receive: pbEventReceive) -> 'EventMessageReceived':
        from_client_id = event_receive.Tags.get("x-kubemq-client-id", "") if event_receive.Tags else ""
        tags = event_receive.Tags if event_receive.Tags else {}
        message = EventMessageReceived()
        message.id = event_receive.EventID
        message.from_client_id = from_client_id
        message.channel = event_receive.Channel
        message.metadata = event_receive.Metadata
        message.body = event_receive.Body
        message.tags = tags
        return message

    def __repr__(self):
        return f"EventMessageReceived: id={self.id}, channel={self.channel}, metadata={self.metadata}, body={self.body}, from_client_id={self.from_client_id}, timestamp={self.timestamp}, tags={self.tags}"