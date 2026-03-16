from __future__ import annotations

import uuid
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from kubemq.common.channel_validators import validate_channel_name
from kubemq.grpc import Request as pbQuery


class QueryMessage(BaseModel):
    """A query message for request-response patterns with optional caching.

    Instances are immutable after construction. Use ``with_updates()``
    or ``model_copy(update={...})`` to create modified copies.

    Thread Safety:
        Instances are immutable (frozen) and safe to read from multiple
        threads. However, reusing the same instance for multiple send
        operations is not recommended — create a new instance per send
        to ensure unique message IDs.

    Attributes:
        id: The ID of the query message.
        channel: The channel of the query message.
        metadata: The metadata of the query message.
        body: The body of the query message.
        tags: The tags of the query message.
        timeout_in_seconds: The timeout of the query message in seconds.
        cache_key: The cache key of the query message.
        cache_ttl_int_seconds: The cache TTL of the query message in seconds.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    id: str | None = Field(default_factory=lambda: str(uuid.uuid4()))
    channel: str
    metadata: str | None = None
    body: bytes = Field(default=b"")
    tags: dict[str, str] = Field(default_factory=dict)
    timeout_in_seconds: int = Field(gt=0)
    cache_key: str = ""
    cache_ttl_int_seconds: int = 0

    @field_validator("channel")
    def channel_must_exist(cls, v: str) -> str:
        """Validate that the channel is not empty."""
        if not v:
            raise ValueError("Query message must have a channel.")
        validate_channel_name(v)
        return v

    @model_validator(mode="after")
    def check_metadata_body_tags(self) -> QueryMessage:
        """Validate at least one content field and cache consistency."""
        if not self.metadata and not self.body and not self.tags:
            raise ValueError(
                "Query message must have at least one of the following: metadata, body, or tags."
            )
        if self.cache_key and self.cache_ttl_int_seconds <= 0:
            raise ValueError("cache_ttl_int_seconds must be > 0 when cache_key is set.")
        return self

    def encode(self, client_id: str, *, span: bytes = b"") -> pbQuery:
        """Encode the query message to a protobuf Request."""
        pb_query = pbQuery()
        pb_query.RequestID = self.id or str(uuid.uuid4())
        pb_query.ClientID = client_id
        pb_query.Channel = self.channel
        pb_query.Metadata = self.metadata or ""
        pb_query.Body = self.body
        pb_query.Timeout = self.timeout_in_seconds * 1000
        pb_query.RequestTypeData = pbQuery.RequestType.Query
        for key, value in self.tags.items():
            pb_query.Tags[key] = value
        pb_query.CacheKey = self.cache_key
        pb_query.CacheTTL = self.cache_ttl_int_seconds
        if span:
            pb_query.Span = span
        return pb_query

    def with_updates(self, **kwargs: Any) -> Self:
        """Create a new message with updated values.

        Since message instances are immutable, this creates a copy
        with the specified fields overridden.

        Returns:
            A new instance of the same type with updated fields.
        """
        return self.model_copy(update=kwargs)

    def __repr__(self) -> str:
        return (
            f"QueryMessage: id={self.id}, channel={self.channel}, "
            f"metadata={self.metadata}, body={self.body!r}, tags={self.tags}, "
            f"timeout_in_seconds={self.timeout_in_seconds}, "
            f"cache_key={self.cache_key}, "
            f"cache_ttl_int_seconds={self.cache_ttl_int_seconds}"
        )

    @classmethod
    def create(
        cls,
        id: str | None = None,
        channel: str | None = None,
        metadata: str | None = None,
        body: bytes = b"",
        tags: dict[str, str] | None = None,
        timeout_in_seconds: int = 0,
        cache_key: str = "",
        cache_ttl_int_seconds: int = 0,
    ) -> QueryMessage:
        """Creates a new QueryMessage instance.

        This method provides backwards compatibility with the original constructor.
        """
        return cls(
            id=id,
            channel=channel or "",
            metadata=metadata,
            body=body,
            tags=tags or {},
            timeout_in_seconds=timeout_in_seconds,
            cache_key=cache_key,
            cache_ttl_int_seconds=cache_ttl_int_seconds,
        )
