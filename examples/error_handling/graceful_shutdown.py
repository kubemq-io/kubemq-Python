"""Example: Graceful shutdown — properly close clients and cancel subscriptions."""

from __future__ import annotations

import signal
import time

from kubemq import (
    CancellationToken,
    EventMessage,
    EventMessageReceived,
    EventsSubscription,
    PubSubClient,
)


def main() -> None:
    client = PubSubClient(
        address="localhost:50000",
        client_id="python-error-handling-graceful-shutdown-client",
    )
    cancel = CancellationToken()
    shutdown_requested = False

    def on_receive(event: EventMessageReceived) -> None:
        print(f"Received: {event.body.decode('utf-8')}")

    def on_error(err: str) -> None:
        print(f"Error: {err}")

    def signal_handler(sig: int, frame: object) -> None:
        nonlocal shutdown_requested
        print("\nShutdown signal received...")
        shutdown_requested = True

    # Register signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)

    try:
        # Start subscription
        client.subscribe_to_events(
            subscription=EventsSubscription(
                channel="python-error-handling.graceful-shutdown",
                on_receive_event_callback=on_receive,
                on_error_callback=on_error,
            ),
            cancel=cancel,
        )
        print("Subscription active. Press Ctrl+C to shutdown gracefully.")
        time.sleep(1)

        # Send some messages
        for i in range(3):
            if shutdown_requested:
                break
            client.publish_event(
                EventMessage(
                    channel="python-error-handling.graceful-shutdown",
                    body=f"Message #{i + 1}".encode(),
                )
            )
            time.sleep(0.5)

        time.sleep(1)
    finally:
        # Graceful shutdown sequence:
        # 1. Cancel all subscriptions
        print("Step 1: Cancelling subscriptions...")
        cancel.cancel()

        # 2. Close the client (drains in-flight operations)
        print("Step 2: Closing client...")
        client.close()

        print("Shutdown complete")


if __name__ == "__main__":
    main()
