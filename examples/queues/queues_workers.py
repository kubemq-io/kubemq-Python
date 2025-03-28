import time
import threading
from random import random

from kubemq.queues import *


def worker(message: QueueMessageReceived):
    print(f"Worker received message: {message.body.decode('utf-8')}\n")
    time.sleep(random())  # Simulate processing time
    message.re_queue("q2")


def main():
    with Client(address="localhost:50000") as client:
        # Send 10 messages
        for i in range(10):
            send_result = client.send_queues_message(
                QueueMessage(
                    channel="q1",
                    body=f"Message {i + 1}".encode("utf-8"),
                    metadata="some-metadata",
                )
            )
            print(f"Queue Message Sent: {send_result}")

        # Wait for 1 second
        time.sleep(1)

        # Receive 10 messages
        result = client.receive_queues_messages(
            channel="q1",
            max_messages=10,
            wait_timeout_in_seconds=10,
            auto_ack=False,
        )
        if result.is_error:
            print(f"{result.error}")
            return

        # Create 10 threads, each processing one message
        threads = []
        for message in result.messages:
            thread = threading.Thread(target=worker, args=(message,))
            threads.append(thread)

        # Start all threads simultaneously
        for thread in threads:
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        time.sleep(1)


if __name__ == "__main__":
    main()
