from datetime import datetime, timedelta
from kubemq.commands_queries.commands.command_received import CommandReceived
from kubemq.grpc import Response as pbResponse


class CommandResponse:

    def __init__(self, command_received: CommandReceived = None,
                 is_executed: bool = False,
                 error: str = "",
                 timestamp: datetime = None,
                 ):
        self._command_received: CommandReceived = command_received
        self._client_id: str = ""
        self._request_id: str = ""
        self._is_executed: bool = is_executed
        self._timestamp: datetime = timestamp
        self._error: str = error

    @property
    def command_received(self) -> CommandReceived:
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

    def set_command_received(self, command_received: CommandReceived) -> 'CommandResponse':
        self._command_received = command_received
        return self

    def set_is_executed(self, is_executed: bool) -> 'CommandResponse':
        self._is_executed = is_executed
        return self

    def set_error(self, error: str) -> 'CommandResponse':
        self._error = error
        return self

    def set_timestamp(self, timestamp: datetime) -> 'CommandResponse':
        self._timestamp = timestamp
        return self

    def validate(self) -> 'CommandResponse':
        if not self._command_received:
            raise ValueError("Command response must have a command request.")
        elif self._command_received._reply_channel == "":
            raise ValueError("Command response must have a reply channel.")
        return self

    def from_kubemq_command_response(self, pb_response: pbResponse) -> 'CommandResponse':
        self._client_id = pb_response.ClientID
        self._request_id = pb_response.RequestID
        self._is_executed = pb_response.Executed
        self._error = pb_response.Error
        epoch_s, ns = divmod(self._timestamp.Timestamp, 1_000_000_000)
        ts = datetime.fromtimestamp(epoch_s)
        ts += timedelta(microseconds=ns // 1_000)
        self._timestamp = ts
        return self

    def to_kubemq_command_response(self, client_id: str) -> pbResponse:
        pb_response = pbResponse()
        pb_response.ClientID = client_id
        pb_response.RequestID = self._command_received.id
        pb_response.Executed = self._is_executed
        pb_response.Error = self._error
        pb_response.Timestamp = self._timestamp.timestamp()
        return pb_response
