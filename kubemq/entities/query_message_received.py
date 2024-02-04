from datetime import datetime, timedelta
from typing import Dict
from kubemq.grpc import Request as pbRequest


class QueryMessageReceived:
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
    def _from_request(query_receive: pbRequest) -> 'QueryMessageReceived':
        tags = query_receive.Tags if query_receive.Tags else {}
        return QueryMessageReceived(
            id=query_receive.RequestID,
            from_client_id=query_receive.ClientID,
            timestamp=datetime.now(),
            channel=query_receive.Channel,
            metadata=query_receive.Metadata,
            body=query_receive.Body,
            reply_channel=query_receive.ReplyChannel,
            tags=tags
        )