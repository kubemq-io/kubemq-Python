from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from kubemq.cq.query_message_received import QueryReceived
from kubemq.grpc import Response as pbResponse


@dataclass
class QueryResponse:
    """Class for representing a query response message.

    Attributes:
        query_received (Optional[QueryReceived]): The received query message.
        client_id (str): The client ID.
        request_id (str): The request ID.
        is_executed (bool): Indicates if the query has been executed.
        timestamp (datetime): The timestamp of the query response.
        error (str): The error message, if any.
        metadata (Optional[str]): The metadata associated with the query response.
        body (bytes): The body of the query response.
        tags (Dict[str, str]): The tags associated with the query response.
    """

    query_received: QueryReceived | None = None
    client_id: str = ""
    request_id: str = ""
    is_executed: bool = False
    timestamp: datetime = field(default_factory=datetime.now)
    error: str = ""
    metadata: str | None = None
    body: bytes = b""
    tags: dict[str, str] = field(default_factory=dict)
    cache_hit: bool = False

    def __post_init__(self) -> None:
        """Validate query response when query_received is provided."""
        if self.query_received is not None and self.query_received.reply_channel == "":
            raise ValueError("Query response must have a reply channel.")

    @classmethod
    def decode(cls, pb_response: pbResponse) -> QueryResponse:
        """Decodes the protocol buffer response and creates a new QueryResponse instance."""
        return cls(
            client_id=pb_response.ClientID,
            request_id=pb_response.RequestID,
            is_executed=pb_response.Executed,
            error=pb_response.Error,
            timestamp=datetime.fromtimestamp(pb_response.Timestamp / 1e9),
            metadata=pb_response.Metadata,
            body=pb_response.Body,
            tags=dict(pb_response.Tags),
            cache_hit=pb_response.CacheHit,
        )

    def encode(self, client_id: str) -> pbResponse:
        """Encodes the query response message into a protocol buffer response.

        Returns:
            The protobuf Response ready for transmission.
        """
        if not self.query_received:
            raise ValueError("Query received is required for encoding.")
        if not self.query_received.id or self.query_received.id.strip() == "":
            raise ValueError("RequestID cannot be empty")
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
            f"QueryResponse: client_id={self.client_id}, "
            f"request_id={self.request_id}, is_executed={self.is_executed}, "
            f"error={self.error}, timestamp={self.timestamp}"
        )

    @classmethod
    def create(
        cls,
        query_received: QueryReceived | None = None,
        metadata: str | None = None,
        body: bytes = b"",
        tags: dict[str, str] | None = None,
        is_executed: bool = False,
        error: str = "",
        timestamp: datetime | None = None,
    ) -> QueryResponse:
        """Creates a new QueryResponse instance."""
        return cls(
            query_received=query_received,
            metadata=metadata,
            body=body,
            tags=tags or {},
            is_executed=is_executed,
            error=error,
            timestamp=timestamp or datetime.now(),
        )
