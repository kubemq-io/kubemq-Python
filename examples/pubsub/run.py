import time
import concurrent.futures
from kubemq.pubsub import *
import asyncio


def events_run(address, channelName, itr, one_mb_message):
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        with Client(address=address, client_id="events_example") as client:
            print(
                f"Starting events run with address: {address}, channel: {channelName}, itr: {itr}"
            )
            received = 0
            errors = 0

            def on_receive_event(event: EventMessageReceived):
                nonlocal received
                received += 1

            def on_error_handler(err: str):
                nonlocal errors
                errors += 1

            client.subscribe_to_events(
                subscription=EventsSubscription(
                    channel=channelName,
                    group="",
                    on_receive_event_callback=on_receive_event,
                    on_error_callback=on_error_handler,
                ),
                cancel=CancellationToken(),
            )
            time.sleep(1)

            for _ in range(itr):
                result = client.send_events_message(
                    EventMessage(channel=channelName, body=one_mb_message)
                )
            time.sleep(1)
            print(
                f"Completed events_run with address: {address}, channel: {channelName}, itr: {itr}, received: {received}, errors: {errors}"
            )
        loop.close()
    except Exception as e:
        print(f"Events Run: {e}")
        os.exit(1)


def events_store_run(address, channelName, itr, one_mb_message):
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        with Client(address=address, client_id="events_store_example") as client:
            print(
                f"Starting events store run with address: {address}, channel: {channelName}, itr: {itr}"
            )
            received = 0
            errors = 0

            def on_receive_event(event: EventStoreMessageReceived):
                nonlocal received
                received += 1

            def on_error_handler(err: str):
                nonlocal errors
                errors += 1

            client.subscribe_to_events_store(
                subscription=EventsStoreSubscription(
                    channel=channelName,
                    group="",
                    on_receive_event_callback=on_receive_event,
                    on_error_callback=on_error_handler,
                    events_store_type=EventsStoreType.StartNewOnly,
                ),
                cancel=CancellationToken(),
            )
            time.sleep(1)

            for _ in range(itr):
                result = client.send_events_store_message(
                    EventStoreMessage(channel=channelName, body=one_mb_message)
                )
            time.sleep(1)
            print(
                f"Completed events_store_run with address: {address}, channel: {channelName}, itr: {itr}, received: {received}, errors: {errors}"
            )
        loop.close()
    except Exception as e:
        print(f"Events Store Run: {e}")
        os.exit(1)


def main():
    one_mb_message = b"x" * int(1e4)
    itr = 200
    repeat = 1
    channels_events_store_ports = [
        ("localhost:50000", "es1"),
        ("localhost:50000", "es2"),
        ("localhost:50000", "es3"),
    ]
    channels_events_ports = [
        ("localhost:50000", "e1"),
        ("localhost:50000", "e2"),
        ("localhost:50000", "e3"),
    ]
    print(f"Starting events_store_run and events_run with itr: {itr}, repeat: {repeat}")
    for _ in range(repeat):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures_es = [
                executor.submit(
                    events_store_run, address, channelName, itr, one_mb_message
                )
                for address, channelName in channels_events_store_ports
            ]
            futures_e = [
                executor.submit(events_run, address, channelName, itr, one_mb_message)
                for address, channelName in channels_events_ports
            ]
            concurrent.futures.wait(futures_es)
            concurrent.futures.wait(futures_e)
    print(
        f"Completed events_store_run and events_run with itr: {itr}, repeat: {repeat}"
    )


if __name__ == "__main__":
    main()
