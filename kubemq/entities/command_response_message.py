from datetime import datetime
from kubemq.entities.command_message_received import CommandMessageReceived
from kubemq.grpc import Response as pbResponse


class CommandResponseMessage:

    def __init__(self, command_received: CommandMessageReceived = None,
                 is_executed: bool = False,
                 error: str = "",
                 timestamp: datetime = None,
                 ):
        self._command_received: CommandMessageReceived = command_received
        self._client_id: str = ""
        self._request_id: str = ""
        self._is_executed: bool = is_executed
        self._timestamp: datetime = timestamp if timestamp else datetime.now()
        self._error: str = error

    @property
    def command_received(self) -> CommandMessageReceived:
        return self._command_received

    @property
    def is_executed(self) -> bool:
        return self._is_executed

    @property
    def client_id(self) -> str:
        return self._client_id

    @property
    def request_id(self) -> str:
        return self._request_id

    @property
    def error(self) -> str:
        return self._error

    @property
    def timestamp(self) -> datetime:
        return self._timestamp

    def validate(self) -> 'CommandResponseMessage':
        if not self._command_received:
            raise ValueError("Command response must have a command request.")
        elif self._command_received._reply_channel == "":
            raise ValueError("Command response must have a reply channel.")
        return self

    def from_kubemq_command_response(self, pb_response: pbResponse) -> 'CommandResponseMessage':
        self._client_id = pb_response.ClientID
        self._request_id = pb_response.RequestID
        self._is_executed = pb_response.Executed
        self._error = pb_response.Error
        self._timestamp = datetime.fromtimestamp(pb_response.Timestamp / 1e9)
        return self

    def to_kubemq_command_response(self, client_id: str) -> pbResponse:
        pb_response = pbResponse()
        pb_response.ClientID = client_id
        pb_response.RequestID = self._command_received.id
        pb_response.ReplyChannel = self._command_received._reply_channel
        pb_response.Executed = self._is_executed
        pb_response.Error = self._error
        pb_response.Timestamp = int(self._timestamp.timestamp() * 1e9)
        return pb_response
