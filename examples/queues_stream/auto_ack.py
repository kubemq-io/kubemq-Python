"""Example: Auto ack — receive messages with automatic acknowledgment."""

from __future__ import annotations

from kubemq import QueueMessage, QueuesClient


def main() -> None:
    with QueuesClient(
        address="localhost:50000",
        client_id="python-queues-stream-auto-ack-client",
    ) as client:
        client.send_queue_message(
            QueueMessage(
                channel="python-queues-stream.auto-ack",
                body=b"auto-acked-message",
            )
        )

        response = client.receive_queue_messages(
            channel="python-queues-stream.auto-ack",
            max_messages=10,
            wait_timeout_in_seconds=5,
            auto_ack=True,
        )
        for msg in response.messages:
            print(
                f"Received (auto-acked): id={msg.id}, "
                f"body={msg.body.decode('utf-8')}"
            )
        print("Messages were automatically acknowledged upon receipt")


if __name__ == "__main__":
    main()
