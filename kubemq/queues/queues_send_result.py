import datetime
from kubemq.grpc import SendQueueMessageResult


class QueueSendResult:
    """

    QueueSendResult represents the result of sending a message to a queue.

    Attributes:
        id (str): The unique identifier of the message.
        sent_at (datetime): The timestamp when the message was sent.
        expired_at (datetime): The timestamp when the message will expire.
        delayed_to (datetime): The timestamp when the message will be delivered.
        is_error (bool): Indicates if there was an error while sending the message.
        error (str): The error message if `is_error` is True.

    Methods:
        decode(result: SendQueueMessageResult) -> QueueSendResult:
            Decodes the given SendQueueMessageResult and sets the attributes of the QueueSendResult instance accordingly.
            Returns the updated QueueSendResult instance.

        __repr__() -> str:
            Returns a string representation of the QueueSendResult object.

    """
    def __init__(self, id: str = None,
                 sent_at: datetime = None,
                 expired_at: datetime = None,
                 delayed_to: datetime = None,
                 is_error: bool = False,
                 error: str = None):
        self.id: str = id
        self.sent_at: datetime = sent_at
        self.expired_at: datetime = expired_at
        self.delayed_to: datetime = delayed_to
        self.is_error: bool = is_error
        self.error: str = error

    def decode(self, result:SendQueueMessageResult) -> 'QueueSendResult':
        self.id = result.MessageID if result.MessageID else ""
        self.sent_at = datetime.datetime.fromtimestamp(result.SentAt / 1e9) if result.SentAt > 0 else None
        self.expired_at = datetime.datetime.fromtimestamp(result.ExpirationAt /1e9) if result.ExpirationAt > 0 else None
        self.delayed_to = datetime.datetime.fromtimestamp(result.DelayedTo /1e9) if result.DelayedTo > 0 else None
        self.is_error = result.IsError if result.IsError else False
        self.error = result.Error if result.Error else ""
        return self

    def __repr__(self):
        return f"QueueSendResult: id={self.id}, sent_at={self.sent_at}, expired_at={self.expired_at}, delayed_to={self.delayed_to}, is_error={self.is_error}, error={self.error}"