import threading


class CancellationToken:
    """A class representing a cancellation token for synchronous operations.

    This class provides a simple mechanism to cancel an operation or check if
    cancellation has been requested.

    Note:
        For async operations, use `AsyncCancellationToken` from
        `kubemq.common.async_cancellation_token` instead, which provides
        native asyncio support::

            from kubemq.common import AsyncCancellationToken

            token = AsyncCancellationToken()
            async for event in client.subscribe_to_events(sub, token):
                if should_stop:
                    token.cancel()

    Attributes:
        event (threading.Event): The event used to signal cancellation.

    Methods:
        cancel: Set the cancellation event.
        is_set: Check if cancellation has been requested (deprecated, use is_cancelled).
        is_cancelled: Check if cancellation has been requested.
    """

    def __init__(self):
        self.event = threading.Event()

    def cancel(self):
        """Signal cancellation. Thread-safe and idempotent."""
        self.event.set()

    def is_set(self):
        """Check if cancellation has been requested.

        Deprecated:
            Use `is_cancelled` property instead for consistency with AsyncCancellationToken.
        """
        return self.event.is_set()

    @property
    def is_cancelled(self) -> bool:
        """Check if cancellation has been requested.

        This property provides API consistency with AsyncCancellationToken.
        """
        return self.event.is_set()
