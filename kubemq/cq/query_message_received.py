from datetime import datetime
from typing import Dict
from pydantic import BaseModel, Field
from kubemq.grpc import Request as pbRequest


class QueryMessageReceived(BaseModel):
    """
    Class representing a query message received.

    Attributes:
        id (str): The unique ID of the query message.
        from_client_id (str): The ID of the client that sent the query message.
        timestamp (datetime): The timestamp when the query message was received.
        channel (str): The channel through which the query message was received.
        metadata (str): Additional metadata associated with the query message.
        body (bytes): The body of the query message.
        reply_channel (str): The channel through which the reply for the query message should be sent.
        tags (Dict[str, str]): A dictionary containing tags associated with the query message.
    """

    id: str = Field(default="")
    from_client_id: str = Field(default="")
    timestamp: datetime = Field(default_factory=datetime.now)
    channel: str = Field(default="")
    metadata: str = Field(default="")
    body: bytes = Field(default=b"")
    reply_channel: str = Field(default="")
    tags: Dict[str, str] = Field(default_factory=dict)

    model_config = {"arbitrary_types_allowed": True}

    @classmethod
    def decode(cls, query_receive: pbRequest) -> "QueryMessageReceived":
        """
        Decodes a protobuf request object and returns a QueryMessageReceived instance.
        """
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
        """
        Returns a string representation of the QueryMessageReceived object.
        """
        return (
            f"QueryMessageReceived: id={self.id}, channel={self.channel}, "
            f"metadata={self.metadata}, body={self.body}, "
            f"from_client_id={self.from_client_id}, timestamp={self.timestamp}, "
            f"reply_channel={self.reply_channel}, tags={self.tags}"
        )
