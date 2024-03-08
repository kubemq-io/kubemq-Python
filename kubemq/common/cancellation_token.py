import threading


class CancellationToken:
    def __init__(self):
        self.event = threading.Event()

    def cancel(self):
        self.event.set()

    def is_set(self):
        return self.event.is_set()
