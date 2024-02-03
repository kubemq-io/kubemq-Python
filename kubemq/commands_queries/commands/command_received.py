from datetime import datetime, timedelta
from typing import Dict
from kubemq.grpc import Request as pbRequest


class CommandReceived:
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

    @staticmethod
    def from_request(command_receive: pbRequest) -> 'CommandReceived':
        from_client_id = command_receive.Tags.get("x-kubemq-client-id", "") if command_receive.Tags else ""
        tags = command_receive.Tags if command_receive.Tags else {}
        epoch_s, ns = divmod(command_receive.Timestamp, 1_000_000_000)
        ts = datetime.fromtimestamp(epoch_s)
        ts += timedelta(microseconds=ns // 1_000)
        return CommandReceived(
            id=command_receive.EventID,
            from_client_id=from_client_id,
            timestamp=datetime.now(),
            channel=command_receive.Channel,
            metadata=command_receive.Metadata,
            body=command_receive.Body,
            reply_channel=command_receive.ReplyChannel,
            tags=tags
        )