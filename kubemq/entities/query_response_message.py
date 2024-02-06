from datetime import datetime
from typing import Dict
from kubemq.entities.query_message_received import QueryMessageReceived
from kubemq.grpc import Response as pbResponse


class QueryResponseMessage:

    def __init__(self, query_received: QueryMessageReceived = None,
                 metadata: str = None,
                 body: bytes = None,
                 tags: Dict[str, str] = None,
                 is_executed: bool = False,
                 error: str = "",
                 timestamp: datetime = None,
                 ):
        self._query_received: QueryMessageReceived = query_received
        self._client_id: str = ""
        self._request_id: str = ""
        self._is_executed: bool = is_executed
        self._timestamp: datetime = timestamp if timestamp else datetime.now()
        self._error: str = error
        self._metadata: str = metadata
        self._body: bytes = body
        self._tags: Dict[str, str] = tags if tags else {}

    @property
    def query_received(self) -> QueryMessageReceived:
        return self._query_received

    @property
    def is_executed(self) -> bool:
        return self._is_executed

    @property
    def client_id(self) -> str:
        return self._client_id

    @property
    def request_id(self) -> str:
        return self._request_id

    @property
    def error(self) -> str:
        return self._error

    @property
    def timestamp(self) -> datetime:
        return self._timestamp

    @property
    def metadata(self) -> str:
        return self._metadata

    @property
    def body(self) -> bytes:
        return self._body

    @property
    def tags(self) -> Dict[str, str]:
        return self._tags

    def _validate(self) -> 'QueryResponseMessage':
        if not self._query_received:
            raise ValueError("Query response must have a query request.")
        elif self._query_received._reply_channel == "":
            raise ValueError("Query response must have a reply channel.")
        return self

    def _from_kubemq_query_response(self, pb_response: pbResponse) -> 'QueryResponseMessage':
        self._client_id = pb_response.ClientID
        self._request_id = pb_response.RequestID
        self._is_executed = pb_response.Executed
        self._error = pb_response.Error
        self._timestamp = datetime.fromtimestamp(pb_response.Timestamp/1e9)
        self._metadata = pb_response.Metadata
        self._body = pb_response.Body
        self._tags = pb_response.Tags
        return self

    def _to_kubemq_query_response(self, client_id: str) -> pbResponse:
        pb_response = pbResponse()
        pb_response.ClientID = client_id
        pb_response.RequestID = self._query_received.id
        pb_response.ReplyChannel = self._query_received._reply_channel
        pb_response.Executed = self._is_executed
        pb_response.Error = self._error
        pb_response.Timestamp = int(self._timestamp.timestamp() * 1e9)
        pb_response.Metadata = self._query_received.metadata
        pb_response.Body = self._query_received.body
        for key, value in self._tags.items():
            pb_response.Tags[key] = value
        return pb_response


    def __str__(self):
        return f'QueryResponseMessage: client_id:{self._client_id}, request_id:{self._request_id}, is_executed:{self._is_executed}, error:{self._error}, timestamp:{self._timestamp}, metadata:{self._metadata}, body:{self._body}, tags:{self._tags}'