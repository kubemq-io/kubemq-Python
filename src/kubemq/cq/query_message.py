from __future__ import annotations

import dataclasses
import sys
from dataclasses import dataclass, field
from typing import Any

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

from kubemq.common.channel_validators import validate_channel_name
from kubemq.common.helpers import fast_id
from kubemq.grpc import Request as pbQuery


@dataclass(frozen=True)
class QueryMessage:
    """A query message for request-response patterns with optional caching.

    Instances are immutable after construction. Use ``with_updates()``
    to create modified copies.

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
        cache_ttl_in_seconds: The cache TTL of the query message in seconds.

    Raises:
        KubeMQValidationError: If channel is missing or empty, if all of
            metadata, body, and tags are empty (at least one must be set),
            or if cache_key is set but cache_ttl_in_seconds is not > 0.

    See Also:
        QueryReceived: The received counterpart when subscribing.
        CQClient.send_query: Send queries to a channel.
    """

    channel: str
    timeout_in_seconds: int
    id: str | None = field(default_factory=fast_id)
    metadata: str | None = None
    body: bytes = b""
    tags: dict[str, str] = field(default_factory=dict)
    cache_key: str = ""
    cache_ttl_in_seconds: int = 0

    def __post_init__(self) -> None:
        """Validate query message fields."""
        if not self.channel:
            raise ValueError("Query message must have a channel.")
        validate_channel_name(self.channel)
        if self.timeout_in_seconds <= 0:
            raise ValueError("timeout_in_seconds must be greater than 0")
        if not self.metadata and not self.body and not self.tags:
            raise ValueError(
                "Query message must have at least one of the following: metadata, body, or tags."
            )
        if self.cache_key and self.cache_ttl_in_seconds <= 0:
            raise ValueError("cache_ttl_in_seconds must be > 0 when cache_key is set.")

    def encode(self, client_id: str, *, span: bytes = b"") -> pbQuery:
        """Encode the query message to a protobuf Request.

        Returns:
            The protobuf Request ready for transmission.
        """
        pb_query = pbQuery()
        pb_query.RequestID = self.id or fast_id()
        pb_query.ClientID = client_id
        pb_query.Channel = self.channel
        pb_query.Metadata = self.metadata or ""
        pb_query.Body = self.body
        pb_query.Timeout = self.timeout_in_seconds * 1000
        pb_query.RequestTypeData = pbQuery.RequestType.Query
        for key, value in self.tags.items():
            pb_query.Tags[key] = value
        pb_query.CacheKey = self.cache_key
        pb_query.CacheTTL = self.cache_ttl_in_seconds
        if span:
            pb_query.Span = span
        return pb_query

    def with_updates(self, **kwargs: Any) -> Self:
        """Create a new message with updated values.

        Returns:
            A new QueryMessage with the specified fields replaced.
        """
        return dataclasses.replace(self, **kwargs)

    def __repr__(self) -> str:
        return (
            f"QueryMessage: id={self.id}, channel={self.channel}, "
            f"metadata={self.metadata}, body={self.body!r}, tags={self.tags}, "
            f"timeout_in_seconds={self.timeout_in_seconds}, "
            f"cache_key={self.cache_key}, "
            f"cache_ttl_in_seconds={self.cache_ttl_in_seconds}"
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
        cache_ttl_in_seconds: int = 0,
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
            cache_ttl_in_seconds=cache_ttl_in_seconds,
        )
