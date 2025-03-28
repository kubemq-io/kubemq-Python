import time
from kubemq.pubsub import *


def main():
    try:
        with Client(
            address="localhost:50000", client_id="events_store_example"
        ) as client:

            def on_receive_event(event: EventStoreMessageReceived):
                print(
                    f"Id:{event.id}, Timestamp:{event.timestamp} From: {event.from_client_id},  Body:{event.body.decode('utf-8')}"
                )

            def on_error_handler(err: str):
                print(f"{err}")

            client.subscribe_to_events_store(
                subscription=EventsStoreSubscription(
                    channel="es1",
                    group="",
                    on_receive_event_callback=on_receive_event,
                    on_error_callback=on_error_handler,
                    events_store_type=EventsStoreType.StartNewOnly,
                ),
                cancel=CancellationToken(),
            )

            time.sleep(1)
            result = client.send_events_store_message(
                EventStoreMessage(channel="es1", body=b"hello kubemq")
            )
            print(f"send result:{result}")
            time.sleep(1000)
    except Exception as e:
        print(e)
        return


if __name__ == "__main__":
    main()
