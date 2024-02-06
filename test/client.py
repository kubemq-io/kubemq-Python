from __future__ import print_function
import grpc
import threading
import time
import queue
from bidirecctional_pb2_grpc import BidirectionalStub
from bidirecctional_pb2 import Message


def make_message(message):
    return Message(message=message)


def generate_messages(msg_queue):
    messages = [
        "First message",
        "Second message",
        "Third message",
        "Fourth message",
        "Fifth message",
    ]
    for msg in messages:
        print(f"Hello Server Sending you the {msg}")
        msg_queue.put(make_message(msg))
        time.sleep(1)  # wait for 1 second before sending the next message


def send_message(stub, msg_queue):
    def send_requests():
        while True:
            msg = msg_queue.get()  # This will block until an item is available
            yield msg

    responses = stub.GetServerResponse(send_requests())
    for response in responses:
        print(f"Hello from the server received your {response.message}")


def run():
    msg_queue = queue.Queue()

    with grpc.insecure_channel('localhost:50051') as channel:
        stub = BidirectionalStub(channel)

        # Start the generate_messages function in a separate thread
        threading.Thread(target=generate_messages, args=(msg_queue,)).start()

        # Start the send_message function in the main thread
        send_message(stub, msg_queue)


if __name__ == '__main__':
    run()
