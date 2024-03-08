from kubemq.grpc import Result


class EventSendResult:
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
