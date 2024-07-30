import uuid
from typing import Dict, Optional
from pydantic import BaseModel, Field, field_validator, model_validator
from kubemq.grpc import Request as pbQuery


class QueryMessage(BaseModel):
    """
    Class representing a query message.

    Attributes:
        id (Optional[str]): The ID of the query message.
        channel (str): The channel of the query message.
        metadata (Optional[str]): The metadata of the query message.
        body (bytes): The body of the query message.
        tags (Dict[str, str]): The tags of the query message.
        timeout_in_seconds (int): The timeout of the query message in seconds.
        cache_key (str): The cache key of the query message.
        cache_ttl_int_seconds (int): The cache TTL of the query message in seconds.
    """

    id: Optional[str] = Field(default_factory=lambda: str(uuid.uuid4()))
    channel: str
    metadata: Optional[str] = None
    body: bytes = Field(default=b"")
    tags: Dict[str, str] = Field(default_factory=dict)
    timeout_in_seconds: int = Field(gt=0)
    cache_key: str = ""
    cache_ttl_int_seconds: int = 0

    model_config = {"arbitrary_types_allowed": True}

    @field_validator("channel")
    def channel_must_exist(cls, v: str) -> str:
        if not v:
            raise ValueError("Query message must have a channel.")
        return v

    @model_validator(mode="after")
    def check_metadata_body_tags(self) -> "QueryMessage":
        if not self.metadata and not self.body and not self.tags:
            raise ValueError(
                "Query message must have at least one of the following: metadata, body, or tags."
            )
        return self

    def encode(self, client_id: str) -> pbQuery:
        pb_query = pbQuery()
        pb_query.RequestID = self.id
        pb_query.ClientID = client_id
        pb_query.Channel = self.channel
        pb_query.Metadata = self.metadata or ""
        pb_query.Body = self.body
        pb_query.Timeout = self.timeout_in_seconds * 1000
        pb_query.RequestTypeData = pbQuery.RequestType.Query
        for key, value in self.tags.items():
            pb_query.Tags[key] = value
        pb_query.CacheKey = self.cache_key
        pb_query.CacheTTL = self.cache_ttl_int_seconds * 1000
        return pb_query

    def __repr__(self) -> str:
        return (
            f"QueryMessage: id={self.id}, channel={self.channel}, "
            f"metadata={self.metadata}, body={self.body}, tags={self.tags}, "
            f"timeout_in_seconds={self.timeout_in_seconds}, "
            f"cache_key={self.cache_key}, "
            f"cache_ttl_int_seconds={self.cache_ttl_int_seconds}"
        )

    @classmethod
    def create(
        cls,
        id: Optional[str] = None,
        channel: Optional[str] = None,
        metadata: Optional[str] = None,
        body: bytes = b"",
        tags: Optional[Dict[str, str]] = None,
        timeout_in_seconds: int = 0,
        cache_key: str = "",
        cache_ttl_int_seconds: int = 0,
    ) -> "QueryMessage":
        """
        Creates a new QueryMessage instance.

        This method provides backwards compatibility with the original constructor.
        """
        return cls(
            id=id,
            channel=channel,
            metadata=metadata,
            body=body,
            tags=tags,
            timeout_in_seconds=timeout_in_seconds,
            cache_key=cache_key,
            cache_ttl_int_seconds=cache_ttl_int_seconds,
        )
