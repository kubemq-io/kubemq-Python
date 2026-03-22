"""Example: Purge queue — remove all messages from a queue."""

from __future__ import annotations

from kubemq import Client, QueueMessage


def main() -> None:
    with Client(
        address="localhost:50000",
        client_id="python-management-purge-queue-client",
    ) as client:
        # Send some messages to purge
        for i in range(5):
            client.send_queue_message(
                QueueMessage(
                    channel="python-management.purge-queue",
                    body=f"Msg-{i + 1}".encode(),
                )
            )
        print("Sent 5 messages")

        # Purge all messages from the queue
        acked = client.ack_all_queue_messages(
            "python-management.purge-queue", wait_time_seconds=5
        )
        print(f"Purged {acked} messages from 'python-management.purge-queue'")


if __name__ == "__main__":
    main()
