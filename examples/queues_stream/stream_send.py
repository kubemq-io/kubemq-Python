"""Example: Stream send — send multiple messages via the streaming interface."""

from __future__ import annotations

from kubemq import Client, QueueMessage


def main() -> None:
    with Client(
        address="localhost:50000",
        client_id="python-queues-stream-stream-send-client",
    ) as client:
        for i in range(5):
            result = client.send_queue_message(
                QueueMessage(
                    channel="python-queues-stream.stream-send",
                    body=f"Stream message #{i + 1}".encode(),
                    tags={"batch": "upstream-demo", "index": str(i)},
                )
            )
            print(f"Sent message #{i + 1}: ID={result.id}, Error={result.is_error}")

        print("All messages sent via upstream stream")


if __name__ == "__main__":
    main()
