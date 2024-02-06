import logging
import time
from kubemq.client import Client
from kubemq.entities import EventMessage, EventMessageReceived, EventsSubscription,CancellationToken

def main():

    try:
        client = Client(address="localhost:50000", client_id="events_example", log_level=logging.DEBUG)
        # client._run_events_upstream_sender(CancellationToken())

        # def on_receive_event(event: EventMessageReceived):
        #     print(
        #         f"Id:{event.id}, Timestamp:{event.timestamp} From: {event.from_client_id},  Body:{event.body.decode('utf-8')}")
        #
        # def on_error_handler(err: str):
        #     print(f"Error: {err}")
        # client.subscribe(
        #     subscription=EventsSubscription(
        #         channel="e1",
        #         group="",
        #         on_receive_event_callback=on_receive_event,
        #         on_error_callback=on_error_handler,
        #     )
        #     , cancellation_token=CancellationToken())
        # time.sleep(1)
        # client.send(EventMessage(
        #     channel="e1",
        #     body=b"hello kubemq"
        # ))
        time.sleep(10)
    except Exception as e:
        print(e)
        return


if __name__ == "__main__":
    main()