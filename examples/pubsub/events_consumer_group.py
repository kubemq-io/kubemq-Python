import time
from kubemq.pubsub import *


def make_handler(name: str):
    def handler(event: EventMessageReceived):
        print(f"[{name}] Received: {event.body.decode('utf-8')}")

    return handler


def on_error(err: str):
    print(f"Error: {err}")


def main():
    client = Client(address="localhost:50000")
    cancel = CancellationToken()

    client.subscribe_to_events(
        subscription=EventsSubscription(
            channel="tasks",
            group="workers",
            on_receive_event_callback=make_handler("Worker-1"),
            on_error_callback=on_error,
        ),
        cancel=cancel,
    )
    client.subscribe_to_events(
        subscription=EventsSubscription(
            channel="tasks",
            group="workers",
            on_receive_event_callback=make_handler("Worker-2"),
            on_error_callback=on_error,
        ),
        cancel=cancel,
    )

    time.sleep(1)
    for i in range(6):
        client.send_events_message(
            EventMessage(channel="tasks", body=f"Task #{i + 1}".encode())
        )

    time.sleep(3)
    cancel.cancel()
    client.close()


if __name__ == "__main__":
    main()
