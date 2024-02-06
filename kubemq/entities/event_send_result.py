from kubemq.grpc import Result


class EventSendResult:
    def __init__(self, id: str = None,
                 sent: bool = False,
                 error: str = None):
        self._id: str = id
        self._sent: bool = sent
        self._error: str = error

    @property
    def id(self) -> str:
        return self._id

    @property
    def sent(self) -> bool:
        return self._sent

    @property
    def error(self) -> str:
        return self._error

    def _from_kubemq_result(self, result:Result) -> 'EventSendResult':
        self._id = result.EventID
        self._sent = result.Sent
        self._error = result.Error
        return self

    def __repr__(self):
        return f"EventSendResult: id={self._id}, sent={self._sent}, error={self._error}"