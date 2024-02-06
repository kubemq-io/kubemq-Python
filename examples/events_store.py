import logging
import time
from kubemq.client import Client
from kubemq.entities import *


def main():

    try:
        client = Client(address="localhost:50000", client_id="events_store_example", log_level=logging.DEBUG)

        def on_receive_event(event: EventStoreMessageMessage):
            print(
                f"Id:{event.id}, Timestamp:{event.timestamp} From: {event.from_client_id},  Body:{event.body.decode('utf-8')} Sequence:{event.sequence}")

        def on_error_handler(exception):
            print(f"Error: {exception}")

        client.subscribe(
            subscription=EventsStoreSubscription(
                channel="es1",
                group="",
                on_receive_event_callback=on_receive_event,
                on_error_callback=on_error_handler,
                events_store_type=EventsStoreType.StartNewOnly,
            )
            , cancellation_token=CancellationToken())
        time.sleep(1)
        result= client.send(EventStoreMessage(
            channel="es1",
            body=b"hello kubemq"
        ))
        print(f"send result:{result}")
    except Exception as e:
        print(e)
        return


if __name__ == "__main__":
    main()
