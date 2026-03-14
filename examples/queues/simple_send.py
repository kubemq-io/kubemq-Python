"""Send a single queue message using the simple synchronous API."""

from kubemq.queues import *


def main():
    with Client(address="localhost:50000") as client:
        result = client.send_queue_message(
            QueueMessage(
                channel="simple-demo",
                body=b"Hello from simple send!",
                metadata="example-metadata",
                tags={"source": "simple_send_example"},
            )
        )
        print(f"Sent: id={result.id}, sent_at={result.sent_at}, error={result.error}")


if __name__ == "__main__":
    main()
