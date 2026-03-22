"""Example: Cancel subscription — demonstrate cancelling an active subscription."""

from __future__ import annotations

import time

from kubemq import (
    CancellationToken,
    Client,
    EventMessage,
    EventReceived,
    EventsSubscription,
)


def main() -> None:
    with Client(
        address="localhost:50000",
        client_id="python-events-cancel-subscription-client",
    ) as client:
        cancel = CancellationToken()

        def on_receive(event: EventReceived) -> None:
            print(f"Received: {event.body.decode('utf-8')}")

        def on_error(err: str) -> None:
            print(f"Error: {err}")

        client.subscribe_to_events(
            subscription=EventsSubscription(
                channel="python-events.cancel-subscription",
                on_receive_event_callback=on_receive,
                on_error_callback=on_error,
            ),
            cancel=cancel,
        )
        time.sleep(1)

        client.send_event(
            EventMessage(channel="python-events.cancel-subscription", body=b"before cancel")
        )
        time.sleep(1)

        cancel.cancel()
        print("Subscription cancelled")

        time.sleep(1)
        client.send_event(
            EventMessage(channel="python-events.cancel-subscription", body=b"after cancel")
        )
        print("Message sent after cancel — subscriber will NOT receive it")
        time.sleep(1)


if __name__ == "__main__":
    main()
