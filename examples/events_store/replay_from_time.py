"""Example: Replay from time — subscribe starting from a specific timestamp."""

from __future__ import annotations

import time
from datetime import datetime

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
        client_id="python-events-store-replay-from-time-client",
    ) as client:
        # Pre-populate with some messages
        for i in range(5):
            client.send_events_store_message(
                EventStoreMessage(
                    channel="python-events-store.replay-from-time",
                    body=f"Message-{i + 1}".encode(),
                )
            )
        print("Sent 5 messages")
        time.sleep(1)

        cancel = CancellationToken()

        def on_receive(event: EventStoreMessageReceived) -> None:
            print(f"[StartAtTime] Seq:{event.sequence}, Body:{event.body.decode('utf-8')}")

        def on_error(err: str) -> None:
            print(f"Error: {err}")

        # Subscribe starting from the current time — will only get new messages
        start_time = datetime.now()
        client.subscribe_to_events_store(
            subscription=EventsStoreSubscription(
                channel="python-events-store.replay-from-time",
                on_receive_event_callback=on_receive,
                on_error_callback=on_error,
                events_store_type=EventsStoreType.StartAtTime,
                events_store_start_time=start_time,
            ),
            cancel=cancel,
        )

        time.sleep(1)
        # Send a new message after the start time
        client.send_events_store_message(
            EventStoreMessage(
                channel="python-events-store.replay-from-time",
                body=b"Message after start time",
            )
        )

        time.sleep(2)
        cancel.cancel()


if __name__ == "__main__":
    main()
