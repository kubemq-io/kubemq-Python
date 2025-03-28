import uuid
from datetime import datetime

import grpc
import threading
import time
import queue
from kubemq_pb2_grpc import kubemqStub
from kubemq_pb2 import Event, Result


def sender_thread(sender, thread_id):
    for _ in range(100000):
        event = Event(
            EventID=str(uuid.uuid4()),
            Channel="es4",
            Metadata=f"Thread {thread_id} - Message {_}",
            ClientID="python-sdk",
            Store=True,
        )
        response = sender.send(event=event)
        # Uncomment the next line if you need to print the response or handle it.
        # print(f"Response from Thread {thread_id}: {response}")
        # time.sleep(0.1)  # 0.1 second interval


def generate_and_send_messages(sender):
    threads = []
    for i in range(50):  # 50 threads
        thread = threading.Thread(target=sender_thread, args=(sender, i))
        thread.start()
        threads.append(thread)

    # Wait for all threads to complete
    for thread in threads:
        thread.join()


def run():
    with grpc.insecure_channel("localhost:50000") as channel:
        stub = kubemqStub(channel)
        shutdown_event = threading.Event()
        sender = EventSender(stub, shutdown_event)
        # Start the send_message function in a separate thread

        time.sleep(2)
        # Example of sending a single message and waiting for the response
        print("Starting to send messages", datetime.now())
        generate_and_send_messages(sender)
        print("All messages sent", datetime.now())
        try:
            # Keep the main thread running, unless interrupted
            while not shutdown_event.is_set():
                time.sleep(0.1)
        except KeyboardInterrupt:
            print("Received shutdown signal")
            shutdown_event.set()


class EventSender:
    def __init__(self, client_stub: kubemqStub, shutdown_event: threading.Event):
        self._clientStub = client_stub
        self._shutdown_event = shutdown_event
        self._lock = threading.Lock()
        self._response_tracking = {}
        self._sending_queue = queue.Queue()
        self._allow_new_messages = True
        threading.Thread(target=self._send_events_stream, args=(), daemon=True).start()

    def send(self, event: Event) -> [Result, None]:
        if not event.Store:
            self._sending_queue.put(event)
            return None
        response_event = threading.Event()
        response_container = {}

        with self._lock:
            self._response_tracking[event.EventID] = (
                response_container,
                response_event,
            )
        self._sending_queue.put(event)
        response_event.wait()
        response = response_container.get("response")
        with self._lock:
            del self._response_tracking[event.EventID]
        return response

    def _send_events_stream(self):
        def send_requests():
            while not self._shutdown_event.is_set():
                try:
                    msg = self._sending_queue.get(
                        timeout=1
                    )  # timeout to check for shutdown event periodically
                    yield msg
                except queue.Empty:
                    continue

        while not self._shutdown_event.is_set():
            try:
                responses = self._clientStub.SendEventsStream(send_requests())
                print("Connecting to the server...")
                for response in responses:
                    if self._shutdown_event.is_set():
                        break
                    response_event_id = response.EventID
                    with self._lock:
                        if response_event_id in self._response_tracking:
                            response_container, response_event = (
                                self._response_tracking[response_event_id]
                            )
                            response_container["response"] = response
                            response_event.set()
            except grpc.RpcError as e:
                print(f"GRPC Error: {e}. Retrying in 5 seconds...")
                time.sleep(5)
                continue


if __name__ == "__main__":
    run()
