"""Example: Consumer group — load-balance events across multiple subscribers."""

from __future__ import annotations

import time

from kubemq import (
    CancellationToken,
    EventMessage,
    EventMessageReceived,
    EventsSubscription,
    PubSubClient,
)


def make_handler(name: str):  # type: ignore[no-untyped-def]
    def handler(event: EventMessageReceived) -> None:
        print(f"[{name}] Received: {event.body.decode('utf-8')}")

    return handler


def on_error(err: str) -> None:
    print(f"Error: {err}")


def main() -> None:
    with PubSubClient(
        address="localhost:50000",
        client_id="python-events-consumer-group-client",
    ) as client:
        cancel = CancellationToken()

        client.subscribe_to_events(
            subscription=EventsSubscription(
                channel="python-events.consumer-group",
                group="workers",
                on_receive_event_callback=make_handler("Worker-1"),
                on_error_callback=on_error,
            ),
            cancel=cancel,
        )
        client.subscribe_to_events(
            subscription=EventsSubscription(
                channel="python-events.consumer-group",
                group="workers",
                on_receive_event_callback=make_handler("Worker-2"),
                on_error_callback=on_error,
            ),
            cancel=cancel,
        )

        time.sleep(1)
        for i in range(6):
            client.publish_event(
                EventMessage(
                    channel="python-events.consumer-group",
                    body=f"Task #{i + 1}".encode(),
                )
            )

        time.sleep(3)
        cancel.cancel()


if __name__ == "__main__":
    main()
