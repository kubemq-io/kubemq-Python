"""Example: Peek messages — inspect queue messages without consuming them."""

from __future__ import annotations

from kubemq import Client, QueueMessage


def main() -> None:
    with Client(
        address="localhost:50000",
        client_id="python-queues-peek-messages-client",
    ) as client:
        # Send a message to peek at
        client.send_queue_message(
            QueueMessage(
                channel="python-queues.peek-messages",
                body=b"peek-me",
                metadata="test",
            )
        )

        # Peek at messages (they remain in the queue)
        waiting_result = client.peek_queue_messages("python-queues.peek-messages", 5, 10)
        if waiting_result.is_error:
            print(f"Error: {waiting_result.error}")
            return

        print(f"Messages waiting: {len(waiting_result.messages)}")
        for msg in waiting_result.messages:
            print(f"  Peek: id={msg.id}, body={msg.body.decode('utf-8')}")

        # Messages are still available after peeking
        pull_result = client.pull("python-queues.peek-messages", 5, 10)
        print(f"Messages pulled after peek: {len(pull_result.messages)}")


if __name__ == "__main__":
    main()
