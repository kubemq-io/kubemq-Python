from datetime import datetime
from typing import Dict, Optional
from pydantic import BaseModel, Field, field_validator
from kubemq.cq.query_message_received import QueryMessageReceived
from kubemq.grpc import Response as pbResponse


class QueryResponseMessage(BaseModel):
    """
    Class for representing a query response message.

    Attributes:
        query_received (Optional[QueryMessageReceived]): The received query message.
        client_id (str): The client ID.
        request_id (str): The request ID.
        is_executed (bool): Indicates if the query has been executed.
        timestamp (datetime): The timestamp of the query response.
        error (str): The error message, if any.
        metadata (Optional[str]): The metadata associated with the query response.
        body (bytes): The body of the query response.
        tags (Dict[str, str]): The tags associated with the query response.
    """

    query_received: Optional[QueryMessageReceived] = None
    client_id: str = Field(default="")
    request_id: str = Field(default="")
    is_executed: bool = Field(default=False)
    timestamp: datetime = Field(default_factory=datetime.now)
    error: str = Field(default="")
    metadata: Optional[str] = None
    body: bytes = Field(default=b"")
    tags: Dict[str, str] = Field(default_factory=dict)

    model_config = {"arbitrary_types_allowed": True}

    @field_validator("query_received")
    def validate_query_received(cls, v):
        if v is None:
            raise ValueError("Query response must have a query request.")
        if v.reply_channel == "":
            raise ValueError("Query response must have a reply channel.")
        return v

    @classmethod
    def decode(cls, pb_response: pbResponse) -> "QueryResponseMessage":
        """
        Decodes the protocol buffer response and creates a new QueryResponseMessage instance.
        """
        return cls(
            client_id=pb_response.ClientID,
            request_id=pb_response.RequestID,
            is_executed=pb_response.Executed,
            error=pb_response.Error,
            timestamp=datetime.fromtimestamp(pb_response.Timestamp / 1e9),
            metadata=pb_response.Metadata,
            body=pb_response.Body,
            tags=dict(pb_response.Tags),
        )

    def encode(self, client_id: str) -> pbResponse:
        """
        Encodes the query response message into a protocol buffer response.
        """
        if not self.query_received:
            raise ValueError("Query received is required for encoding.")
        pb_response = pbResponse()
        pb_response.ClientID = client_id
        pb_response.RequestID = self.query_received.id
        pb_response.ReplyChannel = self.query_received.reply_channel
        pb_response.Executed = self.is_executed
        pb_response.Error = self.error
        pb_response.Timestamp = int(self.timestamp.timestamp() * 1e9)
        pb_response.Metadata = self.metadata or ""
        pb_response.Body = self.body or b""
        for key, value in self.tags.items():
            pb_response.Tags[key] = value
        return pb_response

    def __repr__(self) -> str:
        return (
            f"QueryResponseMessage: client_id={self.client_id}, "
            f"request_id={self.request_id}, is_executed={self.is_executed}, "
            f"error={self.error}, timestamp={self.timestamp}"
        )

    @classmethod
    def create(
        cls,
        query_received: Optional[QueryMessageReceived] = None,
        metadata: Optional[str] = None,
        body: bytes = b"",
        tags: Optional[Dict[str, str]] = None,
        is_executed: bool = False,
        error: str = "",
        timestamp: Optional[datetime] = None,
    ) -> "QueryResponseMessage":
        """
        Creates a new QueryResponseMessage instance.

        This method provides backwards compatibility with the original constructor.
        """
        return cls(
            query_received=query_received,
            metadata=metadata,
            body=body,
            tags=tags,
            is_executed=is_executed,
            error=error,
            timestamp=timestamp or datetime.now(),
        )
