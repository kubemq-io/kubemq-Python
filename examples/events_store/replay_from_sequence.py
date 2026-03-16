"""Example: Replay from sequence — subscribe starting from a specific sequence number."""

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
        client_id="python-events-store-replay-from-sequence-client",
    ) as client:
        # Pre-populate with some messages
        for i in range(5):
            result = client.send_events_store_message(
                EventStoreMessage(
                    channel="python-events-store.replay-from-sequence",
                    body=f"Message-{i + 1}".encode(),
                )
            )
            print(f"Sent message {i + 1}, result: {result}")
        time.sleep(1)

        cancel = CancellationToken()

        def on_receive(event: EventStoreMessageReceived) -> None:
            print(
                f"[StartAtSequence(3)] Seq:{event.sequence}, "
                f"Body:{event.body.decode('utf-8')}"
            )

        def on_error(err: str) -> None:
            print(f"Error: {err}")

        # Subscribe starting from sequence 3 — will replay messages 3, 4, 5
        client.subscribe_to_events_store(
            subscription=EventsStoreSubscription(
                channel="python-events-store.replay-from-sequence",
                on_receive_event_callback=on_receive,
                on_error_callback=on_error,
                events_store_type=EventsStoreType.StartAtSequence,
                events_store_sequence_value=3,
            ),
            cancel=cancel,
        )

        time.sleep(3)
        cancel.cancel()


if __name__ == "__main__":
    main()
