"""Example: Wildcard subscription — subscribe to events on channels matching a pattern."""

from __future__ import annotations

import time

from kubemq import CancellationToken, EventMessage, EventsSubscription
from kubemq.pubsub import Client as PubSubClient


def on_event(event) -> None:  # type: ignore[no-untyped-def]
    """Handle received event from any matching channel."""
    print(f"[{event.channel}] Received: {event.body.decode('utf-8')}")


def on_error(error: str) -> None:
    """Handle subscription errors."""
    print(f"Subscription error: {error}")


def main() -> None:
    cancel = CancellationToken()

    with PubSubClient(
        address="localhost:50000",
        client_id="python-events-wildcard-subscription-client",
    ) as client:
        # Subscribe using a wildcard pattern
        # This will receive events from all channels matching "python-events.wildcard.*"
        client.subscribe_to_events(
            subscription=EventsSubscription(
                channel="python-events.wildcard.*",
                on_receive_event_callback=on_event,
                on_error_callback=on_error,
            ),
            cancel=cancel,
        )

        time.sleep(1)

        # Publish events to different sub-channels
        client.send_event(
            EventMessage(channel="python-events.wildcard.created", body=b"Order #1001 created")
        )
        client.send_event(
            EventMessage(channel="python-events.wildcard.shipped", body=b"Order #1002 shipped")
        )
        client.send_event(
            EventMessage(channel="python-events.wildcard.delivered", body=b"Order #1003 delivered")
        )

        print("Events sent to wildcard sub-channels")
        time.sleep(3)
        cancel.cancel()


if __name__ == "__main__":
    main()
