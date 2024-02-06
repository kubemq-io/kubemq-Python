from typing import Callable
from kubemq.grpc import Subscribe
from kubemq.entities.subscribe_type import SubscribeType
from kubemq.entities.query_message_received import QueryMessageReceived


class QueriesSubscription:

    def __init__(self, channel: str = None,
                 group: str = None,
                 on_receive_query_callback: Callable[[QueryMessageReceived], None] = None,
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

    @property
    def on_receive_query_callback(self) -> Callable[[QueryMessageReceived], None]:
        return self._on_receive_query_callback

    @property
    def on_error_callback(self) -> Callable[[str], None]:
        return self._on_error_callback

    def raise_on_receive_message(self, received_query: QueryMessageReceived):
        if self._on_receive_query_callback:
            self._on_receive_query_callback(received_query)

    def raise_on_error(self, msg: str):
        if self._on_error_callback:
            self._on_error_callback(msg)

    def _validate(self):
        if not self._channel:
            raise ValueError("query subscription must have a channel.")
        if not self._on_receive_query_callback:
            raise ValueError("query subscription must have a on_receive_query_callback function.")

    def _to_subscribe_request(self, client_id: str = "") -> Subscribe:
        request = Subscribe()
        request.Channel = self._channel
        request.Group = self._group
        request.ClientID = client_id
        request.SubscribeTypeData = SubscribeType.Queries.value
        return request

    def __repr__(self):
        return f"QueriesSubscription: channel={self._channel}, group={self._group}"