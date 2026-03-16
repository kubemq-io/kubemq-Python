"""Example: Multiple subscribers — demonstrates broadcast and group load balancing."""

from __future__ import annotations

import time

from kubemq import CancellationToken, EventMessage, EventsSubscription, PubSubClient


def make_handler(name: str):  # type: ignore[no-untyped-def]
    """Create a named event handler for identification."""

    def handler(event) -> None:  # type: ignore[no-untyped-def]
        print(f"[{name}] Received: {event.body.decode('utf-8')}")

    return handler


def on_error(error: str) -> None:
    """Handle subscription errors."""
    print(f"Subscription error: {error}")


def main() -> None:
    cancel = CancellationToken()

    with PubSubClient(
        address="localhost:50000",
        client_id="python-events-multiple-subscribers-client",
    ) as client:
        # Broadcast: all subscribers receive every message
        client.subscribe_to_events(
            subscription=EventsSubscription(
                channel="python-events.multiple-subscribers",
                on_receive_event_callback=make_handler("Subscriber-A"),
                on_error_callback=on_error,
            ),
            cancel=cancel,
        )
        client.subscribe_to_events(
            subscription=EventsSubscription(
                channel="python-events.multiple-subscribers",
                on_receive_event_callback=make_handler("Subscriber-B"),
                on_error_callback=on_error,
            ),
            cancel=cancel,
        )

        # Group subscription: only one subscriber in the group receives each message
        client.subscribe_to_events(
            subscription=EventsSubscription(
                channel="python-events.multiple-subscribers-tasks",
                group="workers",
                on_receive_event_callback=make_handler("Worker-1"),
                on_error_callback=on_error,
            ),
            cancel=cancel,
        )
        client.subscribe_to_events(
            subscription=EventsSubscription(
                channel="python-events.multiple-subscribers-tasks",
                group="workers",
                on_receive_event_callback=make_handler("Worker-2"),
                on_error_callback=on_error,
            ),
            cancel=cancel,
        )

        time.sleep(1)

        # Broadcast: both Subscriber-A and Subscriber-B receive this
        client.publish_event(
            EventMessage(
                channel="python-events.multiple-subscribers",
                body=b"System update available",
            )
        )

        # Group: only one of Worker-1 or Worker-2 receives each task
        for i in range(4):
            client.publish_event(
                EventMessage(
                    channel="python-events.multiple-subscribers-tasks",
                    body=f"Task #{i + 1}".encode(),
                )
            )

        time.sleep(3)
        cancel.cancel()


if __name__ == "__main__":
    main()
