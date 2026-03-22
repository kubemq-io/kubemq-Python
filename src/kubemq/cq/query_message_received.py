from dataclasses import dataclass, field
from datetime import datetime

from kubemq.grpc import Request as pbRequest


@dataclass(frozen=True)
class QueryReceived:
    """Received query message from a subscription.

    Attributes:
        id (str): The unique ID of the query message.
        from_client_id (str): The ID of the client that sent the query message.
        timestamp (datetime): The timestamp when the query message was received.
        channel (str): The channel through which the query message was received.
        metadata (str): Additional metadata associated with the query message.
        body (bytes): The body of the query message.
        reply_channel (str): The channel through which the reply for the query message should be sent.
        tags (Dict[str, str]): A dictionary containing tags associated with the query message.

    See Also:
        QueryMessage: The outgoing counterpart for sending queries.
        CQClient.subscribe_to_queries: Subscribe to receive queries.

    Thread Safety:
        Instances are safe to read from multiple threads or asyncio
        tasks after receipt. Do not modify fields after receiving.
    """

    id: str = ""
    from_client_id: str = ""
    timestamp: datetime = field(default_factory=datetime.now)
    channel: str = ""
    metadata: str = ""
    body: bytes = b""
    reply_channel: str = ""
    tags: dict[str, str] = field(default_factory=dict)

    @classmethod
    def decode(cls, query_receive: pbRequest) -> "QueryReceived":
        """Decodes a protobuf request object and returns a QueryReceived instance."""
        return cls(
            id=query_receive.RequestID,
            from_client_id=query_receive.ClientID,
            timestamp=datetime.now(),
            channel=query_receive.Channel,
            metadata=query_receive.Metadata,
            body=query_receive.Body,
            reply_channel=query_receive.ReplyChannel,
            tags=dict(query_receive.Tags),
        )

    def __repr__(self) -> str:
        """Returns a string representation of the QueryReceived object."""
        return (
            f"QueryReceived: id={self.id}, channel={self.channel}, "
            f"metadata={self.metadata}, body={self.body!r}, "
            f"from_client_id={self.from_client_id}, timestamp={self.timestamp}, "
            f"reply_channel={self.reply_channel}, tags={self.tags}"
        )
