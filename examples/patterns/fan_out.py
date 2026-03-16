"""Example: Fan-out pattern — broadcast a message to multiple subscribers."""

from __future__ import annotations

import time

from kubemq import (
    CancellationToken,
    EventMessage,
    EventMessageReceived,
    EventsSubscription,
    PubSubClient,
)


def make_subscriber(name: str):  # type: ignore[no-untyped-def]
    """Create a named subscriber handler."""
    received: list[str] = []

    def handler(event: EventMessageReceived) -> None:
        body = event.body.decode("utf-8")
        received.append(body)
        print(f"  [{name}] Received: {body}")

    return handler, received


def on_error(err: str) -> None:
    print(f"Error: {err}")


def main() -> None:
    with PubSubClient(
        address="localhost:50000",
        client_id="python-patterns-fan-out-client",
    ) as client:
        cancel = CancellationToken()

        # Create 3 independent subscribers (no group — each gets every message)
        handler_a, received_a = make_subscriber("Service-A")
        handler_b, received_b = make_subscriber("Service-B")
        handler_c, received_c = make_subscriber("Service-C")

        for handler in [handler_a, handler_b, handler_c]:
            client.subscribe_to_events(
                subscription=EventsSubscription(
                    channel="python-patterns.fan-out",
                    on_receive_event_callback=handler,
                    on_error_callback=on_error,
                ),
                cancel=cancel,
            )

        time.sleep(1)

        # Publish a single message — all 3 subscribers should receive it
        print("Publishing message to fan-out channel...")
        client.publish_event(
            EventMessage(
                channel="python-patterns.fan-out",
                body=b"Order #1001 placed",
            )
        )

        time.sleep(3)
        print(
            f"\nEach subscriber received: A={len(received_a)}, "
            f"B={len(received_b)}, C={len(received_c)}"
        )
        cancel.cancel()


if __name__ == "__main__":
    main()
