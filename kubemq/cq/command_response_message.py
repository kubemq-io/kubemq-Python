from datetime import datetime
from kubemq.cq.command_message_received import CommandMessageReceived
from kubemq.grpc import Response as pbResponse


class CommandResponseMessage:
    """

    Class representing a command response message.

    Attributes:
        command_received (CommandMessageReceived): The received command message this response is associated with.
        client_id (str): The client ID associated with this response.
        request_id (str): The request ID associated with this response.
        is_executed (bool): Indicates if the command associated with this response was executed successfully.
        timestamp (datetime): The timestamp of this response.
        error (str): The error message associated with this response.

    Methods:
        __init__(command_received=None, is_executed=False, error="", timestamp=None):
            Initializes a new CommandResponseMessage object with the given parameters.

        validate() -> CommandResponseMessage:
            Validates the command response message. Throws a ValueError if the command response is invalid.

        decode(pb_response) -> CommandResponseMessage:
            Decodes the protocol buffer response and populates the CommandResponseMessage attributes.

        encode(client_id) -> pbResponse:
            Encodes the CommandResponseMessage into a protocol buffer response object.

        __repr__() -> str:
            Returns a string representation of the CommandResponseMessage.

    """
    def __init__(self, command_received: CommandMessageReceived = None,
                 is_executed: bool = False,
                 error: str = "",
                 timestamp: datetime = None,
                 ):
        self.command_received: CommandMessageReceived = command_received
        self.client_id: str = ""
        self.request_id: str = ""
        self.is_executed: bool = is_executed
        self.timestamp: datetime = timestamp if timestamp else datetime.now()
        self.error: str = error
    def validate(self) -> 'CommandResponseMessage':
        if not self.command_received:
            raise ValueError("Command response must have a command request.")
        elif self.command_received.reply_channel == "":
            raise ValueError("Command response must have a reply channel.")
        return self

    def decode(self, pb_response: pbResponse) -> 'CommandResponseMessage':
        self.client_id = pb_response.ClientID
        self.request_id = pb_response.RequestID
        self.is_executed = pb_response.Executed
        self.error = pb_response.Error
        self.timestamp = datetime.fromtimestamp(pb_response.Timestamp / 1e9)
        return self

    def encode(self, client_id: str) -> pbResponse:
        pb_response = pbResponse()
        pb_response.ClientID = client_id
        pb_response.RequestID = self.command_received.id
        pb_response.ReplyChannel = self.command_received.reply_channel
        pb_response.Executed = self.is_executed
        pb_response.Error = self.error
        pb_response.Timestamp = int(self.timestamp.timestamp() * 1e9)
        return pb_response

    def __repr__(self):
        return f"CommandResponseMessage: client_id={self.client_id}, request_id={self.request_id}, is_executed={self.is_executed}, error={self.error}, timestamp={self.timestamp}"