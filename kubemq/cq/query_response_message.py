from datetime import datetime
from typing import Dict
from kubemq.cq.query_message_received import QueryMessageReceived
from kubemq.grpc import Response as pbResponse


class QueryResponseMessage:
    """
    Class for representing a query response message.

    Attributes:
        query_received (QueryMessageReceived): The received query message.
        client_id (str): The client ID.
        request_id (str): The request ID.
        is_executed (bool): Indicates if the query has been executed.
        timestamp (datetime): The timestamp of the query response.
        error (str): The error message, if any.
        metadata (str): The metadata associated with the query response.
        body (bytes): The body of the query response.
        tags (Dict[str, str]): The tags associated with the query response.

    Methods:
        validate() -> QueryResponseMessage:
            Validates the query response and raises an error if it is invalid.

        decode(pb_response: pbResponse) -> QueryResponseMessage:
            Decodes the protocol buffer response and updates the attributes of the query response message.

        encode(client_id: str) -> pbResponse:
            Encodes the query response message into a protocol buffer response.

        __repr__() -> str:
            Returns a string representation of the query response message.
    """
    def __init__(self, query_received: QueryMessageReceived = None,
                 metadata: str = None,
                 body: bytes = b'',
                 tags: Dict[str, str] = None,
                 is_executed: bool = False,
                 error: str = "",
                 timestamp: datetime = None,
                 ):
        self.query_received: QueryMessageReceived = query_received
        self.client_id: str = ""
        self.request_id: str = ""
        self.is_executed: bool = is_executed
        self.timestamp: datetime = timestamp if timestamp else datetime.now()
        self.error: str = error
        self.metadata: str = metadata
        self.body: bytes = body
        self.tags: Dict[str, str] = tags if tags else {}

    def validate(self) -> 'QueryResponseMessage':
        if not self.query_received:
            raise ValueError("Query response must have a query request.")
        elif self.query_received.reply_channel == "":
            raise ValueError("Query response must have a reply channel.")
        return self

    def decode(self, pb_response: pbResponse) -> 'QueryResponseMessage':
        self.client_id = pb_response.ClientID
        self.request_id = pb_response.RequestID
        self.is_executed = pb_response.Executed
        self.error = pb_response.Error
        self.timestamp = datetime.fromtimestamp(pb_response.Timestamp / 1e9)
        self.metadata = pb_response.Metadata
        self.body = pb_response.Body
        self.tags = pb_response.Tags
        return self

    def encode(self, client_id: str) -> pbResponse:
        pb_response = pbResponse()
        pb_response.ClientID = client_id
        pb_response.RequestID = self.query_received.id
        pb_response.ReplyChannel = self.query_received.reply_channel
        pb_response.Executed = self.is_executed
        pb_response.Error = self.error
        pb_response.Timestamp = int(self.timestamp.timestamp() * 1e9)
        pb_response.Metadata = self.query_received.metadata
        pb_response.Body = self.query_received.body
        for key, value in self.tags.items():
            pb_response.Tags[key] = value
        return pb_response

    def __repr__(self):
        return f"QueryResponseMessage: client_id={self.client_id}, request_id={self.request_id}, is_executed={self.is_executed}, error={self.error}, timestamp={self.timestamp}"
