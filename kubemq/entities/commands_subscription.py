from typing import Callable
from kubemq.grpc import Subscribe
from kubemq.entities.subscribe_type import SubscribeType
from kubemq.entities.command_message_received import CommandMessageReceived


class CommandsSubscription:

    def __init__(self, channel: str = None,
                 group: str = None,
                 on_receive_command_callback: Callable[[CommandMessageReceived], None] = None,
                 on_error_callback: Callable[[str], None] = None):
        self._channel: str = channel
        self._group: str = group
        self._on_receive_command_callback = on_receive_command_callback
        self._on_error_callback = on_error_callback

    @property
    def channel(self) -> str:
        return self._channel

    @property
    def group(self) -> str:
        return self._group

    @property
    def on_receive_command_callback(self) -> Callable[[CommandMessageReceived], None]:
        return self._on_receive_command_callback

    @property
    def on_error_callback(self) -> Callable[[str], None]:
        return self._on_error_callback

    def raise_on_receive_message(self, received_command: CommandMessageReceived):
        if self._on_receive_command_callback:
            self._on_receive_command_callback(received_command)

    def raise_on_error(self, msg: str):
        if self._on_error_callback:
            self._on_error_callback(msg)

    def _validate(self):
        if not self._channel:
            raise ValueError("command subscription must have a channel.")
        if not self._on_receive_command_callback:
            raise ValueError("command subscription must have a on_receive_command_callback function.")

    def _to_subscribe_request(self, client_id: str = "") -> Subscribe:
        request = Subscribe()
        request.Channel = self._channel
        request.Group = self._group
        request.ClientID = client_id
        request.SubscribeTypeData = SubscribeType.Commands.value
        return request

    def __repr__(self):
        return f"CommandsSubscription: channel={self._channel}, group={self._group}"