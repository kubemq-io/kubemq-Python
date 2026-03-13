import time
from kubemq.pubsub import *


def main():
    client = Client(address="localhost:50000")
    cancel = CancellationToken()

    def on_receive(event: EventMessageReceived):
        print(f"Received: {event.body.decode('utf-8')}")

    def on_error(err: str):
        print(f"Error: {err}")

    client.subscribe_to_events(
        subscription=EventsSubscription(
            channel="cancel-demo",
            on_receive_event_callback=on_receive,
            on_error_callback=on_error,
        ),
        cancel=cancel,
    )
    time.sleep(1)
    client.send_events_message(EventMessage(channel="cancel-demo", body=b"before cancel"))
    time.sleep(1)

    cancel.cancel()
    print("Subscription cancelled")

    time.sleep(1)
    client.send_events_message(EventMessage(channel="cancel-demo", body=b"after cancel"))
    print("Message sent after cancel — subscriber will NOT receive it")
    time.sleep(1)
    client.close()


if __name__ == "__main__":
    main()
