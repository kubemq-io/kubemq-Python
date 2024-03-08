from typing import Callable
from kubemq.grpc import Subscribe
from kubemq.common.subscribe_type import SubscribeType
from kubemq.cq.query_message_received import QueryMessageReceived


class QueriesSubscription:

    def __init__(self, channel: str = None,
                 group: str = None,
                 on_receive_query_callback: Callable[[QueryMessageReceived], None] = None,
                 on_error_callback: Callable[[str], None] = None):
        self.channel: str = channel
        self.group: str = group
        self.on_receive_query_callback = on_receive_query_callback
        self.on_error_callback = on_error_callback


    def raise_on_receive_message(self, received_query: QueryMessageReceived):
        if self._on_receive_query_callback:
            self._on_receive_query_callback(received_query)

    def raise_on_error(self, msg: str):
        if self._on_error_callback:
            self._on_error_callback(msg)

    def validate(self):
        if not self.channel:
            raise ValueError("query subscription must have a channel.")
        if not self.on_receive_query_callback:
            raise ValueError("query subscription must have a on_receive_query_callback function.")

    def encode(self, client_id: str = "") -> Subscribe:
        request = Subscribe()
        request.Channel = self.channel
        request.Group = self.group
        request.ClientID = client_id
        request.SubscribeTypeData = SubscribeType.Queries.value
        return request

    def __repr__(self):
        return f"QueriesSubscription: channel={self.channel}, group={self.group}"