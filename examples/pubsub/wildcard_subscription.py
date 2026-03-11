"""Example: Wildcard subscription — subscribe to events on channels matching a pattern."""

from __future__ import annotations

import time

from kubemq import CancellationToken, EventMessage, EventsSubscription, PubSubClient


def on_event(event) -> None:  # type: ignore[no-untyped-def]
    """Handle received event from any matching channel."""
    print(f"[{event.channel}] Received: {event.body.decode('utf-8')}")


def on_error(error: str) -> None:
    """Handle subscription errors."""
    print(f"Subscription error: {error}")


def main() -> None:
    cancel = CancellationToken()

    with PubSubClient(address="localhost:50000") as client:
        # Subscribe using a wildcard pattern
        # This will receive events from all channels matching "orders.*"
        client.subscribe_to_events(
            subscription=EventsSubscription(
                channel="orders.*",
                on_receive_event_callback=on_event,
                on_error_callback=on_error,
            ),
            cancel=cancel,
        )

        time.sleep(1)

        # Publish events to different sub-channels
        client.publish_event(
            EventMessage(channel="orders.created", body=b"Order #1001 created")
        )
        client.publish_event(
            EventMessage(channel="orders.shipped", body=b"Order #1002 shipped")
        )
        client.publish_event(
            EventMessage(channel="orders.delivered", body=b"Order #1003 delivered")
        )

        print("Events sent to orders.created, orders.shipped, orders.delivered")
        time.sleep(3)
        cancel.cancel()


if __name__ == "__main__":
    main()
