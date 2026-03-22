"""Example: Basic pub/sub — publish and subscribe to events."""

from __future__ import annotations

import time

from kubemq import (
    CancellationToken,
    Client,
    EventMessage,
    EventReceived,
    EventsSubscription,
    KubeMQConnectionError,
    KubeMQError,
)


def main() -> None:
    try:
        with Client(
            address="localhost:50000",  # TODO: Replace with your KubeMQ server address
            client_id="python-events-basic-pubsub-client",
        ) as client:

            def on_receive_event(event: EventReceived) -> None:
                print(
                    f"Received — Id:{event.id}, Channel:{event.channel}, "
                    f"Body:{event.body.decode('utf-8')}"
                )

            def on_error(err: str) -> None:
                print(f"Error: {err}")

            cancel = CancellationToken()
            client.subscribe_to_events(
                subscription=EventsSubscription(
                    channel="python-events.basic-pubsub",
                    on_receive_event_callback=on_receive_event,
                    on_error_callback=on_error,
                ),
                cancel=cancel,
            )
            time.sleep(1)

            client.send_event(
                EventMessage(
                    channel="python-events.basic-pubsub",
                    body=b"hello kubemq",
                )
            )
            print("Event sent")

            time.sleep(2)
            cancel.cancel()
    except KubeMQConnectionError as e:
        print(f"Connection error: {e}")
    except KubeMQError as e:
        print(f"KubeMQ error: {e}")


if __name__ == "__main__":
    main()

# Expected output:
# Received — Id:<message-id>, Channel:python-events.basic-pubsub, Body:hello kubemq
# Event sent
