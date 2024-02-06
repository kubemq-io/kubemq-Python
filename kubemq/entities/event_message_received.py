from datetime import datetime
from typing import Dict
from kubemq.grpc import EventReceive as pbEventReceive


class EventMessageReceived:
    def __init__(self, id: str = None,
                 from_client_id: str = None,
                 timestamp: datetime = None,
                 channel: str = None,
                 metadata: str = None,
                 body: bytes = None,
                 tags: Dict[str, str] = None):
        self._id: str = id
        self._from_client_id: str = from_client_id
        self._timestamp: datetime = timestamp
        self._channel: str = channel
        self._metadata: str = metadata
        self._body: bytes = body
        self._tags: Dict[str, str] = tags if tags else {}

    @property
    def id(self) -> str:
        return self._id

    @property
    def from_client_id(self) -> str:
        return self._from_client_id

    @property
    def timestamp(self) -> datetime:
        return self._timestamp

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

    @staticmethod
    def _from_event(event_receive: pbEventReceive) -> 'EventMessageReceived':
        from_client_id = event_receive.Tags.get("x-kubemq-client-id", "") if event_receive.Tags else ""
        tags = event_receive.Tags if event_receive.Tags else {}
        return EventMessageReceived(
            id=event_receive.EventID,
            from_client_id=from_client_id,
            timestamp=datetime.now(),
            channel=event_receive.Channel,
            metadata=event_receive.Metadata,
            body=event_receive.Body,
            tags=tags
        )

    def __repr__(self):
        return f"EventMessageReceived: id={self._id}, from_client_id={self._from_client_id}, timestamp={self._timestamp}, channel={self._channel}, metadata={self._metadata}, body={self._body}, tags={self._tags}"