"""Example: StartFromFirst — replay all stored events from the beginning."""

from __future__ import annotations

import time

from kubemq import (
    CancellationToken,
    Client,
    EventStoreMessage,
    EventStoreReceived,
    EventsStoreSubscription,
)
from kubemq.pubsub.events_store_subscription import EventStoreStartPosition


def main() -> None:
    with Client(
        address="localhost:50000",
        client_id="python-events-store-start-from-first-client",
    ) as client:
        # Pre-populate with some messages
        for i in range(5):
            client.send_event_store(
                EventStoreMessage(
                    channel="python-events-store.start-from-first",
                    body=f"Message-{i + 1}".encode(),
                )
            )
        print("Sent 5 messages")
        time.sleep(1)

        cancel = CancellationToken()

        def on_receive(event: EventStoreReceived) -> None:
            print(f"[StartFromFirst] Seq:{event.sequence}, Body:{event.body.decode('utf-8')}")

        def on_error(err: str) -> None:
            print(f"Error: {err}")

        # Subscribe from the very first message — will replay all stored messages
        client.subscribe_to_events_store(
            subscription=EventsStoreSubscription(
                channel="python-events-store.start-from-first",
                on_receive_event_callback=on_receive,
                on_error_callback=on_error,
                events_store_type=EventStoreStartPosition.StartFromFirst,
            ),
            cancel=cancel,
        )

        time.sleep(3)
        cancel.cancel()


if __name__ == "__main__":
    main()
