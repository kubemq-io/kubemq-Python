import uuid
from typing import Dict, Optional
from kubemq.grpc import Request as pbQuery


class QueryMessage:

    def __init__(self, id: str = None,
                 channel: str = None,
                 metadata: str = None,
                 body: bytes = None,
                 tags: Dict[str, str] = None,
                 timeout_in_seconds: int = 0,
                 cache_key: str = "",
                 cache_ttl_int_seconds: int = 0,
                 ):
        self._id: str = id
        self._channel: str = channel
        self._metadata: str = metadata
        self._body: bytes = body
        self._tags: Dict[str, str] = tags if tags else {}
        self._timeout_in_seconds: int = timeout_in_seconds
        self._cache_key: str = cache_key
        self._cache_ttl_int_seconds: int = cache_ttl_int_seconds

    @property
    def id(self) -> str:
        return self._id

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

    @property
    def timeout_in_seconds(self) -> int:
        return self._timeout_in_seconds

    @property
    def cache_key(self) -> str:
        return self._cache_key

    @property
    def cache_ttl_int_seconds(self) -> int:
        return self._cache_ttl_int_seconds


    def validate(self) -> 'QueryMessage':
        if not self._channel:
            raise ValueError("Query message must have a channel.")

        if not self._metadata and not self._body and not self._tags:
            raise ValueError("Query message must have at least one of the following: metadata, body, or tags.")

        if self._timeout_in_seconds <= 0:
            raise ValueError("Query message timeout must be a positive integer.")
        return self

    def to_kubemq_query(self, client_id: str) -> pbQuery:
        if not self._id:
            self._id = str(uuid.uuid4())
        pb_query = pbQuery()  # Assuming pb is an imported module
        pb_query.RequestID = self._id
        pb_query.ClientID = client_id
        pb_query.Channel = self._channel
        pb_query.Metadata = self._metadata or ""
        pb_query.Body = self._body
        pb_query.Timeout = self._timeout_in_seconds * 1000
        pb_query.RequestTypeData = pbQuery.RequestType.Query
        for key, value in self._tags.items():
            pb_query.Tags[key] = value
        pb_query.CacheKey = self._cache_key
        pb_query.CacheTTL = self._cache_ttl_int_seconds * 1000
        return pb_query
