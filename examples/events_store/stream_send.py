"""Example: Stream send — send events store messages via the bidirectional stream path."""

from __future__ import annotations

import time

from kubemq import (
    CancellationToken,
    EventStoreMessage,
    EventStoreMessageReceived,
    EventsStoreSubscription,
    PubSubClient,
)
from kubemq.pubsub.events_store_subscription import EventsStoreType


def main() -> None:
    with PubSubClient(
        address="localhost:50000",
        client_id="python-events-store-stream-send-client",
    ) as client:
        cancel = CancellationToken()
        received: list[EventStoreMessageReceived] = []

        def on_receive(event: EventStoreMessageReceived) -> None:
            received.append(event)
            print(f"Received seq={event.sequence}: {event.body.decode('utf-8')}")

        def on_error(err: str) -> None:
            print(f"Error: {err}")

        client.subscribe_to_events_store(
            subscription=EventsStoreSubscription(
                channel="python-events-store.stream-send",
                on_receive_event_callback=on_receive,
                on_error_callback=on_error,
                events_store_type=EventsStoreType.StartNewOnly,
            ),
            cancel=cancel,
        )
        time.sleep(1)

        for i in range(10):
            result = client.publish_event_store(
                EventStoreMessage(
                    channel="python-events-store.stream-send",
                    body=f"StoreEvent-{i + 1}".encode(),
                )
            )
            print(f"Sent: id={result.id}, sent={result.sent}")

        time.sleep(3)
        print(f"Received {len(received)} events store messages")
        cancel.cancel()


if __name__ == "__main__":
    main()
