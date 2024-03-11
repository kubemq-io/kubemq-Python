import uuid
from typing import Dict
from kubemq.grpc import Request as pbCommand


class CommandMessage:

    def __init__(self, id: str = None,
                 channel: str = None,
                 metadata: str = None,
                 body: bytes = b'',
                 tags: Dict[str, str] = None,
                 timeout_in_seconds: int = 0):
        self.id: str = id
        self.channel: str = channel
        self.metadata: str = metadata
        self.body: bytes = body
        self.tags: Dict[str, str] = tags if tags else {}
        self.timeout_in_seconds: int = timeout_in_seconds

    def validate(self) -> 'CommandMessage':
        if not self.channel:
            raise ValueError("Command message must have a channel.")

        if not self.metadata and not self.body and not self.tags:
            raise ValueError("Command message must have at least one of the following: metadata, body, or tags.")

        if self.timeout_in_seconds <= 0:
            raise ValueError("Command message timeout must be a positive integer.")
        return self

    def encode(self, client_id: str) -> pbCommand:
        pb_command = pbCommand()
        pb_command.RequestID = self.id or str(uuid.uuid4())
        pb_command.ClientID = client_id
        pb_command.Channel = self.channel
        pb_command.Metadata = self.metadata or ""
        pb_command.Body = self.body
        pb_command.Timeout = self.timeout_in_seconds * 1000
        pb_command.RequestTypeData = pbCommand.RequestType.Command
        for key, value in self.tags.items():
            pb_command.Tags[key] = value
        return pb_command

    def __repr__(self):
        return f"CommandMessage: id={self.id}, channel={self.channel}, metadata={self.metadata}, body={self.body}, tags={self.tags}, timeout_in_seconds={self.timeout_in_seconds}"