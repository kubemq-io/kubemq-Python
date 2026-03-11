"""Quick start: Queues — send and receive a message with acknowledgment."""

from __future__ import annotations

from kubemq import QueueMessage, QueuesClient


def main() -> None:
    with QueuesClient(address="localhost:50000") as client:
        # Send a message to the queue
        result = client.send_queue_message(
            QueueMessage(channel="quickstart-queue", body=b"Task #1")
        )
        print(f"Sent: ID={result.id}")

        # Receive and acknowledge the message
        response = client.receive_queue_messages(
            channel="quickstart-queue",
            max_messages=1,
            wait_timeout_in_seconds=10,
        )
        for msg in response.messages:
            print(f"Received: {msg.body.decode('utf-8')}")
            msg.ack()


if __name__ == "__main__":
    main()
