from datetime import datetime
from typing import Dict
from kubemq.grpc import Request as pbRequest


class CommandMessageReceived:
    def __init__(self, id: str = None,
                 from_client_id: str = None,
                 timestamp: datetime = None,
                 channel: str = None,
                 metadata: str = None,
                 body: bytes = None,
                 reply_channel: str = None,
                 tags: Dict[str, str] = None):
        self._id: str = id
        self._from_client_id: str = from_client_id
        self._timestamp: datetime = timestamp
        self._channel: str = channel
        self._metadata: str = metadata
        self._body: bytes = body
        self._reply_channel: str = reply_channel
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
    def reply_channel(self) -> str:
        return self._reply_channel

    @staticmethod
    def _from_request(command_receive: pbRequest) -> 'CommandMessageReceived':
        tags = command_receive.Tags if command_receive.Tags else {}
        return CommandMessageReceived(
            id=command_receive.RequestID,
            from_client_id=command_receive.ClientID,
            timestamp=datetime.now(),
            channel=command_receive.Channel,
            metadata=command_receive.Metadata,
            body=command_receive.Body,
            reply_channel=command_receive.ReplyChannel,
            tags=tags
        )

    def __repr__(self):
        return f"CommandMessageReceived: id={self._id}, from_client_id={self._from_client_id}, timestamp={self._timestamp}, channel={self._channel}, metadata={self._metadata}, body={self._body}, reply_channel={self._reply_channel}, tags={self._tags}"