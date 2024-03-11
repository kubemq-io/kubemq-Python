from kubemq.grpc import Result


class EventSendResult:
    """
    Initializes an EventSendResult instance.

    Args:
        id (str, optional): The ID of the event. Defaults to None.
        sent (bool, optional): Indicates whether the event was sent successfully. Defaults to False.
        error (str, optional): An error message related to the event send operation. Defaults to None.
    """
    def __init__(self, id: str = None,
                 sent: bool = False,
                 error: str = None):
        self.id: str = id
        self.sent: bool = sent
        self.error: str = error

    def decode(self, result: Result) -> 'EventSendResult':
        self.id = result.EventID
        self.sent = result.Sent
        self.error = result.Error
        return self

    def __repr__(self):
        return f"EventSendResult: id={self.id}, sent={self.sent}, error={self.error}"
