from typing import Callable
from kubemq.grpc import Subscribe
from kubemq.common.subscribe_type import SubscribeType
from kubemq.cq.query_message_received import QueryMessageReceived


class QueriesSubscription:
    """
    QueriesSubscription class represents a subscription to receive query messages from a channel.

    Attributes:
        channel (str): The name of the channel to subscribe to.
        group (str): The optional name of the group to subscribe to.
        on_receive_query_callback (Callable[[QueryMessageReceived], None]): The callback function to be called when a query message is received.
        on_error_callback (Callable[[str], None]): The callback function to be called when an error occurs.

    Methods:
        raise_on_receive_message(received_query: QueryMessageReceived) -> None:
            Raises the on_receive_query_callback with the received query message.

        raise_on_error(msg: str) -> None:
            Raises the on_error_callback with the specified error message.

        validate() -> None:
            Validates the query subscription by checking if the channel and on_receive_query_callback are set.

        encode(client_id: str = "") -> Subscribe:
            Encodes the query subscription into a Subscribe message.

        __repr__() -> str:
            Returns a string representation of the QueriesSubscription object.
    """
    def __init__(self, channel: str = None,
                 group: str = None,
                 on_receive_query_callback: Callable[[QueryMessageReceived], None] = None,
                 on_error_callback: Callable[[str], None] = None):
        self.channel: str = channel
        self.group: str = group
        self.on_receive_query_callback = on_receive_query_callback
        self.on_error_callback = on_error_callback


    def raise_on_receive_message(self, received_query: QueryMessageReceived):
        if self.on_receive_query_callback:
            self.on_receive_query_callback(received_query)

    def raise_on_error(self, msg: str):
        if self.on_error_callback:
            self.on_error_callback(msg)

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