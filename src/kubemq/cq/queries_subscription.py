import asyncio
from collections.abc import Callable
from dataclasses import dataclass

from kubemq.common.channel_validators import validate_channel_name
from kubemq.common.subscribe_type import SubscribeType
from kubemq.cq.query_message_received import QueryReceived
from kubemq.grpc import Subscribe


@dataclass
class QueriesSubscription:
    """QueriesSubscription class represents a subscription to receive query messages from a channel.

    Attributes:
        channel (str): The name of the channel to subscribe to.
        group (Optional[str]): The optional name of the group to subscribe to.
        on_receive_query_callback (Callable[[QueryReceived], None]): The callback function to be called when a query message is received.
        on_error_callback (Optional[Callable[[str], None]]): The callback function to be called when an error occurs.
    """

    channel: str
    on_receive_query_callback: Callable[[QueryReceived], None]
    group: str | None = None
    on_error_callback: Callable[[str], None] | None = None

    def __post_init__(self) -> None:
        """Validate subscription fields."""
        if not self.channel:
            raise ValueError("query subscription must have a channel.")
        validate_channel_name(self.channel)
        if not callable(self.on_receive_query_callback):
            raise ValueError("query subscription must have a on_receive_query_callback function.")

    def raise_on_receive_message(self, received_query: QueryReceived) -> None:
        """Raises the on_receive_query_callback with the received query message."""
        self.on_receive_query_callback(received_query)

    async def raise_on_receive_message_async(self, received_query: QueryReceived) -> None:
        """Async-aware version that supports both sync and async callbacks."""
        if asyncio.iscoroutinefunction(self.on_receive_query_callback):
            await self.on_receive_query_callback(received_query)
        else:
            self.on_receive_query_callback(received_query)

    def raise_on_error(self, msg: str) -> None:
        """Raises the on_error_callback with the specified error message."""
        if self.on_error_callback:
            self.on_error_callback(msg)

    async def raise_on_error_async(self, msg: str) -> None:
        """Async-aware version that supports both sync and async callbacks."""
        if self.on_error_callback:
            if asyncio.iscoroutinefunction(self.on_error_callback):
                await self.on_error_callback(msg)
            else:
                self.on_error_callback(msg)

    def encode(self, client_id: str = "") -> Subscribe:
        """Encodes the query subscription into a Subscribe message."""
        request = Subscribe()
        request.Channel = self.channel
        request.Group = self.group or ""
        request.ClientID = client_id
        request.SubscribeTypeData = SubscribeType.Queries.value  # type: ignore[assignment]
        return request

    def __repr__(self) -> str:
        """Returns a string representation of the QueriesSubscription object."""
        return f"QueriesSubscription: channel={self.channel}, group={self.group}"

    @classmethod
    def create(
        cls,
        channel: str,
        group: str | None = None,
        on_receive_query_callback: Callable[[QueryReceived], None] | None = None,
        on_error_callback: Callable[[str], None] | None = None,
    ) -> "QueriesSubscription":
        """Creates a new QueriesSubscription instance."""
        if on_receive_query_callback is None:
            raise ValueError("on_receive_query_callback is required")
        return cls(
            channel=channel,
            group=group,
            on_receive_query_callback=on_receive_query_callback,
            on_error_callback=on_error_callback,
        )
