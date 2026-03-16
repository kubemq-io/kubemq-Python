"""Quick start: Events (Pub/Sub) — send and receive a fire-and-forget event."""

from __future__ import annotations

import time

from kubemq import CancellationToken, EventMessage, EventsSubscription, PubSubClient


def on_event(event) -> None:  # type: ignore[no-untyped-def]
    """Handle received event."""
    print(f"Received: {event.body.decode('utf-8')}")


def main() -> None:
    cancel = CancellationToken()

    with PubSubClient(address="localhost:50000", client_id="python-events-quickstart-client") as client:
        # Subscribe first so the subscriber is ready
        client.subscribe_to_events(
            subscription=EventsSubscription(
                channel="python-quickstart",
                on_receive_event_callback=on_event,
                on_error_callback=lambda e: print(f"Error: {e}"),
            ),
            cancel=cancel,
        )

        # Give the subscription a moment to connect
        time.sleep(1)

        # Publish an event
        client.publish_event(EventMessage(channel="python-quickstart", body=b"Hello KubeMQ!"))
        print("Event sent!")

        # Wait for the event to arrive
        time.sleep(2)
        cancel.cancel()


if __name__ == "__main__":
    main()
