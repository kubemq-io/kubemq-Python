"""Example: Stream send — send many events efficiently via the streaming path."""

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
        client_id="python-events-stream-send-client",
    ) as client:
        cancel = CancellationToken()
        received: list[EventReceived] = []

        def on_receive(event: EventReceived) -> None:
            received.append(event)
            print(f"Received: {event.body.decode('utf-8')}")

        def on_error(err: str) -> None:
            print(f"Error: {err}")

        client.subscribe_to_events(
            subscription=EventsSubscription(
                channel="python-events.stream-send",
                on_receive_event_callback=on_receive,
                on_error_callback=on_error,
            ),
            cancel=cancel,
        )
        time.sleep(1)

        for i in range(100):
            client.send_event(
                EventMessage(
                    channel="python-events.stream-send",
                    body=f"Event-{i + 1}".encode(),
                )
            )

        print("Sent 100 events via stream")
        time.sleep(3)
        print(f"Received {len(received)} events")
        cancel.cancel()


if __name__ == "__main__":
    main()
