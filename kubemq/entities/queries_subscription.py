from typing import Callable
from kubemq.grpc import Subscribe
from kubemq.entities.subscribe_type import SubscribeType
from kubemq.entities.query_received_message import QueryReceivedMessage


class QueriesSubscription:

    def __init__(self, channel: str = None,
                 group: str = None,
                 on_receive_query_callback: Callable[[QueryReceivedMessage], None] = None,
                 on_error_callback: Callable[[str], None] = None):
        self._channel: str = channel
        self._group: str = group
        self._on_receive_query_callback = on_receive_query_callback
        self._on_error_callback = on_error_callback

    @property
    def channel(self) -> str:
        return self._channel

    @property
    def group(self) -> str:
        return self._group

    def set_channel(self, value: str) -> 'QueriesSubscription':
        self._channel = value
        return self

    def set_group(self, value: str) -> 'QueriesSubscription':
        self._group = value
        return self

    def add_on_receive_query_callback(self, callback: Callable[[QueryReceivedMessage], None]) -> 'QueriesSubscription':
        self._on_receive_query_callback = callback
        return self

    def add_on_error_callback(self, callback: Callable[[str], None]) -> 'QueriesSubscription':
        self._on_error_callback = callback
        return self

    def raise_on_receive_message(self, received_query: QueryReceivedMessage):
        if self._on_receive_query_callback:
            self._on_receive_query_callback(received_query)

    def raise_on_error(self, msg: str):
        if self._on_error_callback:
            self._on_error_callback(msg)

    def validate(self):
        if not self._channel:
            raise ValueError("query subscription must have a channel.")
        if not self._on_receive_query_callback:
            raise ValueError("query subscription must have a on_receive_query_callback function.")

    def to_subscribe_request(self, client_id: str = "") -> Subscribe:
        request = Subscribe()
        request.Channel = self._channel
        request.Group = self._group
        request.ClientID = client_id
        request.SubscribeTypeData = SubscribeType.Queries.value
        return request
