from pydantic import BaseModel, field_validator
from typing import Callable, Optional
from kubemq.grpc import Subscribe
from kubemq.common.subscribe_type import SubscribeType
from kubemq.cq.query_message_received import QueryMessageReceived


class QueriesSubscription(BaseModel):
    """
    QueriesSubscription class represents a subscription to receive query messages from a channel.

    Attributes:
        channel (str): The name of the channel to subscribe to.
        group (Optional[str]): The optional name of the group to subscribe to.
        on_receive_query_callback (Callable[[QueryMessageReceived], None]): The callback function to be called when a query message is received.
        on_error_callback (Optional[Callable[[str], None]]): The callback function to be called when an error occurs.
    """

    channel: str
    group: Optional[str] = None
    on_receive_query_callback: Callable[[QueryMessageReceived], None]
    on_error_callback: Optional[Callable[[str], None]] = None

    model_config = {"arbitrary_types_allowed": True}

    @field_validator("channel")
    def channel_must_exist(cls, v: str) -> str:
        if not v:
            raise ValueError("query subscription must have a channel.")
        return v

    @field_validator("on_receive_query_callback")
    def callback_must_exist(cls, v: Callable) -> Callable:
        if not callable(v):
            raise ValueError(
                "query subscription must have a on_receive_query_callback function."
            )
        return v

    def raise_on_receive_message(self, received_query: QueryMessageReceived) -> None:
        """Raises the on_receive_query_callback with the received query message."""
        self.on_receive_query_callback(received_query)

    def raise_on_error(self, msg: str) -> None:
        """Raises the on_error_callback with the specified error message."""
        if self.on_error_callback:
            self.on_error_callback(msg)

    def encode(self, client_id: str = "") -> Subscribe:
        """Encodes the query subscription into a Subscribe message."""
        request = Subscribe()
        request.Channel = self.channel
        request.Group = self.group or ""
        request.ClientID = client_id
        request.SubscribeTypeData = SubscribeType.Queries.value
        return request

    def __repr__(self) -> str:
        """Returns a string representation of the QueriesSubscription object."""
        return f"QueriesSubscription: channel={self.channel}, group={self.group}"

    @classmethod
    def create(
        cls,
        channel: str,
        group: Optional[str] = None,
        on_receive_query_callback: Callable[[QueryMessageReceived], None] = None,
        on_error_callback: Optional[Callable[[str], None]] = None,
    ) -> "QueriesSubscription":
        """
        Creates a new QueriesSubscription instance.

        This method provides backwards compatibility with the original constructor.
        """
        return cls(
            channel=channel,
            group=group,
            on_receive_query_callback=on_receive_query_callback,
            on_error_callback=on_error_callback,
        )
