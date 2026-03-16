"""Example: StartNewOnly — subscribe only to new events published after subscription."""

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
        client_id="python-events-store-start-new-only-client",
    ) as client:
        # Send messages BEFORE subscribing — these will NOT be received
        for i in range(3):
            client.send_events_store_message(
                EventStoreMessage(
                    channel="python-events-store.start-new-only",
                    body=f"Old-Message-{i + 1}".encode(),
                )
            )
        print("Sent 3 old messages before subscribing")

        cancel = CancellationToken()

        def on_receive(event: EventStoreMessageReceived) -> None:
            print(f"Received — Seq:{event.sequence}, Body:{event.body.decode('utf-8')}")

        def on_error(err: str) -> None:
            print(f"Error: {err}")

        client.subscribe_to_events_store(
            subscription=EventsStoreSubscription(
                channel="python-events-store.start-new-only",
                on_receive_event_callback=on_receive,
                on_error_callback=on_error,
                events_store_type=EventsStoreType.StartNewOnly,
            ),
            cancel=cancel,
        )
        time.sleep(1)

        # Send messages AFTER subscribing — only these will be received
        client.send_events_store_message(
            EventStoreMessage(
                channel="python-events-store.start-new-only",
                body=b"New message after subscription",
            )
        )
        print("Sent 1 new message after subscribing")

        time.sleep(2)
        cancel.cancel()


if __name__ == "__main__":
    main()
