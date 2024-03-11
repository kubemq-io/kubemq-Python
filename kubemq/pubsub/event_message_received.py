from datetime import datetime
from typing import Dict
from kubemq.grpc import EventReceive as pbEventReceive


class EventMessageReceived:
    """

    This class represents a received event message.

    Attributes:
        id (str): The unique identifier of the event message.
        from_client_id (str): The client ID from which the event message originated.
        timestamp (datetime): The timestamp when the event message was received.
        channel (str): The channel to which the event message was sent.
        metadata (str): The metadata associated with the event message.
        body (bytes): The body of the event message.
        tags (Dict[str, str]): Additional tags associated with the event message.

    Methods:
        decode(event_receive: pbEventReceive) -> EventMessageReceived:
            Static method that decodes an event_receive object of type pbEventReceive and returns an instance of EventMessageReceived.

        __repr__() -> str:
            Returns a string representation of the EventMessageReceived object.

    """
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