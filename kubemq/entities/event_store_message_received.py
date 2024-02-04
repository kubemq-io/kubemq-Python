from datetime import datetime, timedelta
from typing import Dict, ByteString
from kubemq.grpc import EventReceive as pbEventReceive


class EventStoreMessageMessage:
    def __init__(self, id: str = None,
                 from_client_id: str = None,
                 timestamp: datetime = None,
                 channel: str = None,
                 metadata: str = None,
                 body: bytes = None,
                 sequence: int = 0,
                 tags: Dict[str, str] = None):
        self._id: str = id
        self._from_client_id: str = from_client_id
        self._timestamp: datetime = timestamp
        self._channel: str = channel
        self._metadata: str = metadata
        self._body: bytes = body
        self._sequence: int = sequence
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

    @property
    def sequence(self) -> int:
        return self._sequence

    @staticmethod
    def _from_event(event_receive: pbEventReceive) -> 'EventStoreMessageMessage':
        from_client_id = event_receive.Tags.get("x-kubemq-client-id", "") if event_receive.Tags else ""
        tags = event_receive.Tags if event_receive.Tags else {}
        epoch_s, ns = divmod(event_receive.Timestamp, 1_000_000_000)
        ts = datetime.fromtimestamp(epoch_s)
        ts += timedelta(microseconds=ns // 1_000)
        return EventStoreMessageMessage(
            id=event_receive.EventID,
            from_client_id=from_client_id,
            timestamp=ts,
            channel=event_receive.Channel,
            metadata=event_receive.Metadata,
            body=event_receive.Body,
            sequence=event_receive.Sequence,
            tags=tags
        )