import uuid
from typing import Dict, Optional
from kubemq.grpc import Event as pbEvent


class EventStoreMessage:
    """
    EventStoreMessage

    Class representing a message to be stored in an event store.

    Attributes:
        id (str): Unique identifier for the message. If not provided, a random UUID will be generated.
        channel (str): Channel for the message.
        metadata (str): Metadata associated with the message.
        body (bytes): Body of the message.
        tags (Dict[str, str]): Additional tags associated with the message.

    Methods:
        validate() -> 'EventStoreMessage':
            Validates that the necessary fields are present in the message.
            Raises a ValueError if any required field is missing.
            Returns self.

        encode(client_id: str) -> pbEvent:
            Encodes the message into a pbEvent object for transmission.
            Adds the client_id as a tag to the message.
            Returns the pbEvent object.

        __repr__() -> str:
            Returns a string representation of the EventStoreMessage object.
    """
    def __init__(self, id: str = None,
                 channel: str = None,
                 metadata: str = None,
                 body: bytes = b'',
                 tags: Dict[str, str] = None):
        self.id: str = id
        self.channel: str = channel
        self.metadata: str = metadata
        self.body: bytes = body
        self.tags: Dict[str, str] = tags if tags else {}

    def validate(self) -> 'EventStoreMessage':
        if not self.channel:
            raise ValueError("Event Store message must have a channel.")

        if not self.metadata and not self.body and not self.tags:
            raise ValueError("Event Store message must have at least one of the following: metadata, body, or tags.")

        return self

    def encode(self, client_id: str) -> pbEvent:
        self.tags["x-kubemq-client-id"] = client_id
        pb_event = pbEvent()
        pb_event.EventID = self.id or str(uuid.uuid4())
        pb_event.ClientID = client_id
        pb_event.Channel = self.channel
        pb_event.Metadata = self.metadata or ""
        pb_event.Body = self.body
        pb_event.Store = True
        for key, value in self.tags.items():
            pb_event.Tags[key] = value
        return pb_event

    def __repr__(self):
        return f"EventStoreMessage: id={self.id}, channel={self.channel}, metadata={self.metadata}, body={self.body}, tags={self.tags}"