import time
from kubemq.pubsub import *


def main():
    try:
        client = Client(address="localhost:50000", client_id="events_example")

        def on_receive_event(event: EventMessageReceived):
            print(
                f"Id:{event.id}, Timestamp:{event.timestamp} From: {event.from_client_id},  Body:{event.body.decode('utf-8')}"
            )

        def on_error_handler(err: str):
            print(f"{err}")

        client.subscribe_to_events(
            subscription=EventsSubscription(
                channel="e1",
                group="",
                on_receive_event_callback=on_receive_event,
                on_error_callback=on_error_handler,
            ),
            cancel=CancellationToken(),
        )
        time.sleep(1)
        client.send_events_message(EventMessage(channel="e1", body=b"hello kubemq"))
        time.sleep(1)
    except Exception as e:
        print(e)
        return


if __name__ == "__main__":
    main()
