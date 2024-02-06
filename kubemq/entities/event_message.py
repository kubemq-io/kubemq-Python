import uuid
from typing import Dict, Optional
from kubemq.grpc import Event as pbEvent


class EventMessage:
    """
    Represents an event message to be sent or received in the KubeMQ SDK.
    """

    def __init__(self, id: str = None,
                 channel: str = None,
                 metadata: str = None,
                 body: bytes = None,
                 tags: Dict[str, str] = None):
        self._id: str = id
        self._channel: str = channel
        self._metadata: str = metadata
        self._body: bytes = body
        self._tags: Dict[str, str] = tags if tags else {}

    @property
    def id(self) -> str:
        return self._id

    @property
    def channel(self) -> str:
        return self._channel

    @property
    def metadata(self) -> str:
        return self._metadata

    @property
    def body(self) -> bytes:
        return self._body

    @property
    def tags(self) -> Dict[str, str]:
        return self._tags


    def _validate(self) -> 'EventMessage':
        if not self._channel:
            raise ValueError("Event message must have a channel.")

        if not self._metadata and not self._body and not self._tags:
            raise ValueError("Event message must have at least one of the following: metadata, body, or tags.")

        return self

    def _to_kubemq_event(self, client_id: str) -> pbEvent:
        if not self._id:
            self._id = str(uuid.uuid4())

        self._tags["x-kubemq-client-id"] = client_id
        pb_event = pbEvent()  # Assuming pb is an imported module
        pb_event.EventID = self._id
        pb_event.ClientID = client_id
        pb_event.Channel = self._channel
        pb_event.Metadata = self._metadata or ""
        pb_event.Body = self._body
        pb_event.Store = False
        for key, value in self._tags.items():
            pb_event.Tags[key] = value

        return pb_event

    def __repr__(self):
        return f"EventMessage: id={self._id}, channel={self._channel}, metadata={self._metadata}, body={self._body}, tags={self._tags}"