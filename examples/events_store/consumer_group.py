"""Example: Consumer group — load-balance events store messages across subscribers."""

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


def make_handler(name: str):  # type: ignore[no-untyped-def]
    def handler(event: EventStoreMessageReceived) -> None:
        print(f"[{name}] Seq:{event.sequence}, Body:{event.body.decode('utf-8')}")

    return handler


def on_error(err: str) -> None:
    print(f"Error: {err}")


def main() -> None:
    with PubSubClient(
        address="localhost:50000",
        client_id="python-events-store-consumer-group-client",
    ) as client:
        cancel = CancellationToken()

        client.subscribe_to_events_store(
            subscription=EventsStoreSubscription(
                channel="python-events-store.consumer-group",
                group="processors",
                on_receive_event_callback=make_handler("Processor-1"),
                on_error_callback=on_error,
                events_store_type=EventsStoreType.StartFromFirst,
            ),
            cancel=cancel,
        )
        client.subscribe_to_events_store(
            subscription=EventsStoreSubscription(
                channel="python-events-store.consumer-group",
                group="processors",
                on_receive_event_callback=make_handler("Processor-2"),
                on_error_callback=on_error,
                events_store_type=EventsStoreType.StartFromFirst,
            ),
            cancel=cancel,
        )

        time.sleep(1)
        for i in range(6):
            client.send_events_store_message(
                EventStoreMessage(
                    channel="python-events-store.consumer-group",
                    body=f"Event-{i + 1}".encode(),
                )
            )

        time.sleep(3)
        cancel.cancel()


if __name__ == "__main__":
    main()
