"""Example: Cancel subscription — demonstrate cancelling an events store subscription."""

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
        client_id="python-events-store-cancel-subscription-client",
    ) as client:
        cancel = CancellationToken()

        def on_receive(event: EventStoreReceived) -> None:
            print(f"Received: Seq={event.sequence}, Body:{event.body.decode('utf-8')}")

        def on_error(err: str) -> None:
            print(f"Error: {err}")

        client.subscribe_to_events_store(
            subscription=EventsStoreSubscription(
                channel="python-events-store.cancel-subscription",
                on_receive_event_callback=on_receive,
                on_error_callback=on_error,
                events_store_type=EventStoreStartPosition.StartFromNew,
            ),
            cancel=cancel,
        )
        time.sleep(1)

        client.send_event_store(
            EventStoreMessage(
                channel="python-events-store.cancel-subscription",
                body=b"before cancel",
            )
        )
        time.sleep(1)

        cancel.cancel()
        print("Events store subscription cancelled")

        client.send_event_store(
            EventStoreMessage(
                channel="python-events-store.cancel-subscription",
                body=b"after cancel",
            )
        )
        print("Message sent after cancel — subscriber will NOT receive it")
        time.sleep(1)


if __name__ == "__main__":
    main()
