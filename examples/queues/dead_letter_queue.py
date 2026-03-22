"""Example: Dead letter queue — messages move to DLQ after max receive attempts."""

from __future__ import annotations

from kubemq import Client, QueueMessage


def main() -> None:
    with Client(
        address="localhost:50000",
        client_id="python-queues-dead-letter-queue-client",
    ) as client:
        # Send a message with DLQ configuration
        client.send_queue_message(
            QueueMessage(
                channel="python-queues.dead-letter-queue",
                body=b"message with DLQ policy",
                metadata="dlq-test",
                max_receive_count=3,
                max_receive_queue="python-queues.dead-letter-queue-dlq",
            )
        )
        print("Sent message with max 3 attempts before DLQ")

        # Simulate failed processing by rejecting the message multiple times
        for attempt in range(3):
            response = client.receive_queue_messages(
                channel="python-queues.dead-letter-queue",
                max_messages=1,
                wait_timeout_in_seconds=5,
            )
            if not response.messages:
                print(f"  Attempt {attempt + 1}: No message (already moved to DLQ)")
                break
            for msg in response.messages:
                print(
                    f"  Attempt {attempt + 1}: Received (receive_count={msg.receive_count}), "
                    f"rejecting..."
                )
                msg.nack()

        # Check the DLQ for the message
        dlq_response = client.receive_queue_messages(
            channel="python-queues.dead-letter-queue-dlq",
            max_messages=1,
            wait_timeout_in_seconds=5,
            auto_ack=True,
        )
        print(f"DLQ messages: {len(dlq_response.messages)}")
        for msg in dlq_response.messages:
            print(f"  DLQ message: {msg.body.decode('utf-8')}")


if __name__ == "__main__":
    main()
