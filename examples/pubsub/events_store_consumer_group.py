import time
from kubemq.pubsub import *


def make_handler(name: str):
    def handler(event: EventStoreMessageReceived):
        print(f"[{name}] Seq:{event.sequence}, Body:{event.body.decode('utf-8')}")

    return handler


def on_error(err: str):
    print(f"Error: {err}")


def main():
    client = Client(address="localhost:50000")
    cancel = CancellationToken()

    client.subscribe_to_events_store(
        subscription=EventsStoreSubscription(
            channel="es-group-demo",
            group="processors",
            on_receive_event_callback=make_handler("Processor-1"),
            on_error_callback=on_error,
            events_store_type=EventsStoreType.StartFromFirst,
        ),
        cancel=cancel,
    )
    client.subscribe_to_events_store(
        subscription=EventsStoreSubscription(
            channel="es-group-demo",
            group="processors",
            on_receive_event_callback=make_handler("Processor-2"),
            on_error_callback=on_error,
            events_store_type=EventsStoreType.StartFromFirst,
        ),
        cancel=cancel,
    )

    time.sleep(1)
    for i in range(6):
        client.send_events_store_message(
            EventStoreMessage(channel="es-group-demo", body=f"Event-{i + 1}".encode())
        )

    time.sleep(3)
    cancel.cancel()
    client.close()


if __name__ == "__main__":
    main()
