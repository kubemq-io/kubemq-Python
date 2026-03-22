"""Example: Persistent pub/sub — publish and subscribe to events store with persistence."""

from __future__ import annotations

import time

from kubemq import (
    CancellationToken,
    Client,
    EventStoreMessage,
    EventStoreReceived,
    EventsStoreSubscription,
    KubeMQConnectionError,
    KubeMQError,
)
from kubemq.pubsub.events_store_subscription import EventStoreStartPosition


def main() -> None:
    try:
        with Client(
            address="localhost:50000",
            client_id="python-events-store-persistent-pubsub-client",
        ) as client:

            def on_receive_event(event: EventStoreReceived) -> None:
                print(
                    f"Received — Id:{event.id}, Seq:{event.sequence}, "
                    f"Body:{event.body.decode('utf-8')}"
                )

            def on_error(err: str) -> None:
                print(f"Error: {err}")

            cancel = CancellationToken()
            client.subscribe_to_events_store(
                subscription=EventsStoreSubscription(
                    channel="python-events-store.persistent-pubsub",
                    on_receive_event_callback=on_receive_event,
                    on_error_callback=on_error,
                    events_store_type=EventStoreStartPosition.StartFromNew,
                ),
                cancel=cancel,
            )
            time.sleep(1)

            result = client.send_event_store(
                EventStoreMessage(
                    channel="python-events-store.persistent-pubsub",
                    body=b"hello kubemq",
                )
            )
            print(f"Send result: {result}")

            time.sleep(2)
            cancel.cancel()
    except KubeMQConnectionError as e:
        print(f"Connection error: {e}")
    except KubeMQError as e:
        print(f"KubeMQ error: {e}")


if __name__ == "__main__":
    main()

# Expected output:
# Received — Id:<message-id>, Seq:<sequence>, Body:hello kubemq
# Send result: <result>
