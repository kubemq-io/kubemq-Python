"""Example: Dead letter policy — messages move to DLQ after max attempts via stream."""

from __future__ import annotations

from kubemq.queues import Client as QueuesClient
from kubemq import QueueMessage


def main() -> None:
    with QueuesClient(
        address="localhost:50000",
        client_id="python-queues-stream-dead-letter-policy-client",
    ) as client:
        # Send a message with combined policies: expiration + DLQ
        result = client.send_queue_message(
            QueueMessage(
                channel="python-queues-stream.dead-letter-policy",
                body=b"message with policies",
                metadata="policy-test",
                expiration_in_seconds=60,
                delay_in_seconds=0,
                max_receive_count=3,
                max_receive_queue="python-queues-stream.dead-letter-policy-dlq",
            )
        )
        print(f"Sent with policies: {result}")
        print("  Expiration: 60s, Max attempts: 3, DLQ: dead-letter-policy-dlq")

        # Reject the message repeatedly to trigger DLQ
        for attempt in range(3):
            response = client.receive_queue_messages(
                channel="python-queues-stream.dead-letter-policy",
                max_messages=1,
                wait_timeout_in_seconds=5,
            )
            if not response.messages:
                print(f"  Attempt {attempt + 1}: No message available")
                break
            for msg in response.messages:
                print(
                    f"  Attempt {attempt + 1}: receive_count={msg.receive_count}, rejecting..."
                )
                msg.nack()

        # Check the DLQ
        dlq = client.receive_queue_messages(
            channel="python-queues-stream.dead-letter-policy-dlq",
            max_messages=1,
            wait_timeout_in_seconds=5,
            auto_ack=True,
        )
        if dlq.messages:
            print(f"DLQ received: {dlq.messages[0].body.decode('utf-8')}")
        else:
            print("DLQ: no messages yet")


if __name__ == "__main__":
    main()
