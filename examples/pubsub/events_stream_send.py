import time
from kubemq.pubsub import *


def main():
    client = Client(address="localhost:50000")
    cancel = CancellationToken()

    received = []

    def on_receive(event: EventMessageReceived):
        received.append(event)
        print(f"Received: {event.body.decode('utf-8')}")

    def on_error(err: str):
        print(f"Error: {err}")

    client.subscribe_to_events(
        subscription=EventsSubscription(
            channel="stream-send-demo",
            on_receive_event_callback=on_receive,
            on_error_callback=on_error,
        ),
        cancel=cancel,
    )
    time.sleep(1)

    for i in range(100):
        client.send_events_message(
            EventMessage(channel="stream-send-demo", body=f"Event-{i + 1}".encode())
        )

    print(f"Sent 100 events via stream")
    time.sleep(3)
    print(f"Received {len(received)} events")
    cancel.cancel()
    client.close()


if __name__ == "__main__":
    main()
