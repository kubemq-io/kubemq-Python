import uuid
from typing import Dict
from kubemq.grpc import Request as pbCommand


class CommandMessage:
    """

    The `CommandMessage` class represents a command message that can be sent over a communication channel. It contains information such as the message ID, channel, metadata, body, tags,
    * and timeout.

    Attributes:
    - `id` (str): The ID of the command message.
    - `channel` (str): The channel through which the command message will be sent.
    - `metadata` (str): Additional metadata associated with the command message.
    - `body` (bytes): The body of the command message as bytes.
    - `tags` (dict): A dictionary of key-value pairs representing tags associated with the command message.
    - `timeout_in_seconds` (int): The maximum time in seconds for which the command message is valid.

    Methods:
    - `validate() -> CommandMessage`: Validates the command message. Raises a `ValueError` if the message is invalid.
    - `encode(client_id: str) -> pbCommand`: Encodes the command message into a protocol buffer message. Returns the encoded message.
    - `__repr__() -> str`: Returns a string representation of the command message.

    Example usage:
    command = CommandMessage(id='123', channel='commands', metadata='Execute command', body=b'command body', tags={'tag1': 'value1'}, timeout_in_seconds=10)
    command.validate()
    encoded_message = command.encode('client1')
    print(encoded_message)

    """
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