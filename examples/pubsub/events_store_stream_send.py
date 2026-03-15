"""Send events store messages via the bidirectional stream path."""

import time
from kubemq.pubsub import *


def main():
    with Client(address="localhost:50000", client_id="store-stream-sender") as client:
        cancel = CancellationToken()
        received = []

        def on_receive(event: EventStoreMessageReceived):
            received.append(event)
            print(f"Received seq={event.sequence}: {event.body.decode('utf-8')}")

        def on_error(err: str):
            print(f"Error: {err}")

        client.subscribe_to_events_store(
            subscription=EventsStoreSubscription(
                channel="store-stream-demo",
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
                    channel="store-stream-demo",
                    body=f"StoreEvent-{i + 1}".encode(),
                )
            )
            print(f"Sent: id={result.id}, sent={result.sent}")

        time.sleep(3)
        print(f"Received {len(received)} events store messages")
        cancel.cancel()


if __name__ == "__main__":
    main()
