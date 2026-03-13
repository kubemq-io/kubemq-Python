import time
from datetime import datetime
from kubemq.pubsub import *


def on_error(err: str):
    print(f"Error: {err}")


def make_handler(label: str):
    def handler(event: EventStoreMessageReceived):
        print(f"[{label}] Seq:{event.sequence}, Body:{event.body.decode('utf-8')}")

    return handler


def main():
    client = Client(address="localhost:50000")

    for i in range(5):
        result = client.send_events_store_message(
            EventStoreMessage(channel="es-positions", body=f"Message-{i + 1}".encode())
        )
        print(f"Sent message {i + 1}, sequence: {result}")
    time.sleep(1)

    cancel1 = CancellationToken()
    client.subscribe_to_events_store(
        subscription=EventsStoreSubscription(
            channel="es-positions",
            on_receive_event_callback=make_handler("StartFromFirst"),
            on_error_callback=on_error,
            events_store_type=EventsStoreType.StartFromFirst,
        ),
        cancel=cancel1,
    )
    time.sleep(2)
    cancel1.cancel()

    cancel2 = CancellationToken()
    client.subscribe_to_events_store(
        subscription=EventsStoreSubscription(
            channel="es-positions",
            on_receive_event_callback=make_handler("StartFromLast"),
            on_error_callback=on_error,
            events_store_type=EventsStoreType.StartFromLast,
        ),
        cancel=cancel2,
    )
    time.sleep(2)
    cancel2.cancel()

    cancel3 = CancellationToken()
    client.subscribe_to_events_store(
        subscription=EventsStoreSubscription(
            channel="es-positions",
            on_receive_event_callback=make_handler("StartAtSequence(3)"),
            on_error_callback=on_error,
            events_store_type=EventsStoreType.StartAtSequence,
            events_store_sequence_value=3,
        ),
        cancel=cancel3,
    )
    time.sleep(2)
    cancel3.cancel()

    cancel4 = CancellationToken()
    client.subscribe_to_events_store(
        subscription=EventsStoreSubscription(
            channel="es-positions",
            on_receive_event_callback=make_handler("StartAtTime"),
            on_error_callback=on_error,
            events_store_type=EventsStoreType.StartAtTime,
            events_store_start_time=datetime.now(),
        ),
        cancel=cancel4,
    )
    time.sleep(2)
    cancel4.cancel()

    cancel5 = CancellationToken()
    client.subscribe_to_events_store(
        subscription=EventsStoreSubscription(
            channel="es-positions",
            on_receive_event_callback=make_handler("StartAtTimeDelta(30s)"),
            on_error_callback=on_error,
            events_store_type=EventsStoreType.StartAtTimeDelta,
            events_store_time_delta_seconds=30,
        ),
        cancel=cancel5,
    )
    time.sleep(2)
    cancel5.cancel()
    client.close()


if __name__ == "__main__":
    main()
