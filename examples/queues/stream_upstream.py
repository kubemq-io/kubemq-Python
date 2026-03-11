"""Example: Queue stream upstream — send multiple messages via the streaming interface."""

from __future__ import annotations

from kubemq import QueueMessage, QueuesClient


def main() -> None:
    with QueuesClient(address="localhost:50000") as client:
        # Send multiple messages using the queue client
        for i in range(5):
            result = client.send_queue_message(
                QueueMessage(
                    channel="stream-demo",
                    body=f"Stream message #{i + 1}".encode(),
                    tags={"batch": "upstream-demo", "index": str(i)},
                )
            )
            print(f"Sent message #{i + 1}: ID={result.id}, Error={result.is_error}")

        print("All messages sent via upstream stream")


if __name__ == "__main__":
    main()
