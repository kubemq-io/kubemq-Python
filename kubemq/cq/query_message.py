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
        self.id: str = id
        self.channel: str = channel
        self.metadata: str = metadata
        self.body: bytes = body
        self.tags: Dict[str, str] = tags if tags else {}
        self.timeout_in_seconds: int = timeout_in_seconds
        self.cache_key: str = cache_key
        self.cache_ttl_int_seconds: int = cache_ttl_int_seconds

    def validate(self) -> 'QueryMessage':
        if not self.channel:
            raise ValueError("Query message must have a channel.")

        if not self.metadata and not self.body and not self.tags:
            raise ValueError("Query message must have at least one of the following: metadata, body, or tags.")

        if self.timeout_in_seconds <= 0:
            raise ValueError("Query message timeout must be a positive integer.")
        return self

    def encode(self, client_id: str) -> pbQuery:

        pb_query = pbQuery()  # Assuming pb is an imported module
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
        pb_query.CacheTTL = self.cache_ttl_int_seconds * 1000
        return pb_query

    def __repr__(self):
        return f"QueryMessage: id={self.id}, channel={self.channel}, metadata={self.metadata}, body={self.body}, tags={self.tags}, timeout_in_seconds={self.timeout_in_seconds}, cache_key={self.cache_key}, cache_ttl_int_seconds={self.cache_ttl_int_seconds}"