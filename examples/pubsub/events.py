import logging
import time
from kubemq.client import Client, CancellationToken
from kubemq.pubsub.events import Event, EventReceived, EventsSubscription


def main():
    def on_receive_event(event: EventReceived):
        print(
            f"Id:{event.id}, Timestamp:{event.timestamp} From: {event.from_client_id},  Body:{event.body.decode('utf-8')}")

    try:
        client = Client(address="localhost:50000", client_id="events_example", log_level=logging.DEBUG)
        client.subscribe_to_events(
            subscription=EventsSubscription(
                channel="e1",
                group="",
                on_receive_event_callback=on_receive_event,
            )
            , cancellation_token=CancellationToken())
        time.sleep(1)
        client.send_event(Event(
            channel="e1",
            body=b"hello kubemq"
        ))
        time.sleep(1)
    except Exception as e:
        print(e)
        return


if __name__ == "__main__":
    main()
