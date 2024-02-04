import uuid
from typing import Dict
from kubemq.grpc import Request as pbCommand


class CommandMessage:

    def __init__(self, id: str = None,
                 channel: str = None,
                 metadata: str = None,
                 body: bytes = None,
                 tags: Dict[str, str] = None,
                 timeout_in_seconds: int = 0):
        self._id: str = id
        self._channel: str = channel
        self._metadata: str = metadata
        self._body: bytes = body
        self._tags: Dict[str, str] = tags if tags else {}
        self._timeout_in_seconds: int = timeout_in_seconds

    @property
    def id(self) -> str:
        return self._id

    @property
    def channel(self) -> str:
        return self._channel

    @property
    def metadata(self) -> str:
        return self._metadata

    @property
    def body(self) -> bytes:
        return self._body

    @property
    def tags(self) -> Dict[str, str]:
        return self._tags

    @property
    def timeout_in_seconds(self) -> int:
        return self._timeout_in_seconds

    def _validate(self) -> 'CommandMessage':
        if not self._channel:
            raise ValueError("Command message must have a channel.")

        if not self._metadata and not self._body and not self._tags:
            raise ValueError("Command message must have at least one of the following: metadata, body, or tags.")

        if self._timeout_in_seconds <= 0:
            raise ValueError("Command message timeout must be a positive integer.")
        return self

    def _to_kubemq_command(self, client_id: str) -> pbCommand:
        if not self._id:
            self._id = str(uuid.uuid4())
        self._tags["x-kubemq-client-id"] = client_id
        pb_command = pbCommand()  # Assuming pb is an imported module
        pb_command.RequestID = self._id
        pb_command.ClientID = client_id
        pb_command.Channel = self._channel
        pb_command.Metadata = self._metadata or ""
        pb_command.Body = self._body
        pb_command.Timeout = self._timeout_in_seconds * 1000
        pb_command.RequestTypeData = pbCommand.RequestType.Command
        for key, value in self._tags.items():
            pb_command.Tags[key] = value
        return pb_command
