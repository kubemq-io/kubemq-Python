import threading


class CancellationToken:
    """
    A class representing a cancellation token.

    This class provides a simple mechanism to cancel an operation or check if
    cancellation has been requested.

    Attributes:
        event (threading.Event): The event used to signal cancellation.

    Methods:
        cancel: Set the cancellation event.
        is_set: Check if cancellation has been requested.
    """

    def __init__(self):
        self.event = threading.Event()

    def cancel(self):
        self.event.set()

    def is_set(self):
        return self.event.is_set()
