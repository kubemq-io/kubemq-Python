"""Example: Expiration policy — send a message that expires after a timeout."""

from __future__ import annotations

import time

from kubemq import Client, QueueMessage


def main() -> None:
    with Client(
        address="localhost:50000",
        client_id="python-queues-stream-expiration-policy-client",
    ) as client:
        # Send a message that expires in 5 seconds
        result = client.send_queue_message(
            QueueMessage(
                channel="python-queues-stream.expiration-policy",
                body=b"message with expiration",
                expiration_in_seconds=5,
            )
        )
        print(f"Sent message with 5s expiration: {result}")

        # Wait for the message to expire
        print("Waiting 6 seconds for expiration...")
        time.sleep(6)

        # Try to receive — message should have expired
        response = client.receive_queue_messages(
            channel="python-queues-stream.expiration-policy",
            max_messages=1,
            wait_timeout_in_seconds=2,
            auto_ack=True,
        )
        print(f"Received {len(response.messages)} messages (expected 0 — message expired)")


if __name__ == "__main__":
    main()
