import uuid
from typing import Dict, Optional
from kubemq.grpc import Event as pbEvent


class EventMessage:
    """

    Class EventMessage

    Class representing an event message.

    Attributes:
        id (str): The ID of the event message.
        channel (str): The channel of the event message.
        metadata (str): The metadata of the event message.
        body (bytes): The body of the event message.
        tags (Dict[str, str]): The tags associated with the event message.

    Methods:
        validate() -> 'EventMessage':
            Validates the event message. Raises a ValueError if the channel is not provided or if at least one of metadata, body, or tags is not provided.

        encode(client_id: str) -> pbEvent:
            Encodes the event message to a pbEvent object (assuming pb is an imported module). Adds the client ID to the tags.
            Returns the pbEvent object.

        __repr__() -> str:
            Returns a string representation of the EventMessage object.
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

    def validate(self) -> 'EventMessage':
        if not self.channel:
            raise ValueError("Event message must have a channel.")

        if not self.metadata and not self.body and not self.tags:
            raise ValueError("Event message must have at least one of the following: metadata, body, or tags.")

        return self

    def encode(self, client_id: str) -> pbEvent:
        self.tags["x-kubemq-client-id"] = client_id
        pb_event = pbEvent()  # Assuming pb is an imported module
        pb_event.EventID = self.id or str(uuid.uuid4())
        pb_event.ClientID = client_id
        pb_event.Channel = self.channel
        pb_event.Metadata = self.metadata or ""
        pb_event.Body = self.body
        pb_event.Store = False
        for key, value in self.tags.items():
            pb_event.Tags[key] = value
        return pb_event

    def __repr__(self):
        return f"EventMessage: id={self.id}, channel={self.channel}, metadata={self.metadata}, body={self.body}, tags={self.tags}"
