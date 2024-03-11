from typing import Callable
from kubemq.grpc import Subscribe
from kubemq.common.subscribe_type import SubscribeType
from kubemq.cq.command_message_received import CommandMessageReceived


class CommandsSubscription:
    """
    Class representing a subscription to receive commands.

    Attributes:
        channel (str): The channel to subscribe to.
        group (str): The group to subscribe to.
        on_receive_command_callback (Callable[[CommandMessageReceived], None]): Callback function to handle received commands.
        on_error_callback (Callable[[str], None]): Callback function to handle errors.

    Methods:
        raise_on_receive_message(received_command: CommandMessageReceived): Raises the on_receive_command_callback function with the received command.
        raise_on_error(msg: str): Raises the on_error_callback function with the error message.
        validate(): Validates the command subscription by checking if channel and on_receive_command_callback are set.
        decode(client_id: str = "") -> Subscribe: Decodes the subscription into a Subscribe object.
        __repr__(): Returns a string representation of the CommandsSubscription object.
    """
    def __init__(self, channel: str = None,
                 group: str = None,
                 on_receive_command_callback: Callable[[CommandMessageReceived], None] = None,
                 on_error_callback: Callable[[str], None] = None):
        self.channel: str = channel
        self.group: str = group
        self.on_receive_command_callback = on_receive_command_callback
        self.on_error_callback = on_error_callback

    def raise_on_receive_message(self, received_command: CommandMessageReceived):
        if self.on_receive_command_callback:
            self.on_receive_command_callback(received_command)

    def raise_on_error(self, msg: str):
        if self.on_error_callback:
            self.on_error_callback(msg)

    def validate(self):
        if not self.channel:
            raise ValueError("command subscription must have a channel.")
        if not self.on_receive_command_callback:
            raise ValueError("command subscription must have a on_receive_command_callback function.")

    def decode(self, client_id: str = "") -> Subscribe:
        request = Subscribe()
        request.Channel = self.channel
        request.Group = self.group
        request.ClientID = client_id
        request.SubscribeTypeData = SubscribeType.Commands.value
        return request

    def __repr__(self):
        return f"CommandsSubscription: channel={self.channel}, group={self.group}"