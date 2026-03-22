"""Example: Delayed messages — send a message that becomes available after a delay."""

from __future__ import annotations

import time

from kubemq import Client, QueueMessage


def main() -> None:
    with Client(
        address="localhost:50000",
        client_id="python-queues-delayed-messages-client",
    ) as client:
        # Send a message with a 5-second delay
        result = client.send_queue_message(
            QueueMessage(
                channel="python-queues.delayed-messages",
                body=b"message with delay",
                delay_in_seconds=5,
            )
        )
        print(f"Sent delayed message: {result}")

        # Try to receive immediately — should get nothing
        response = client.receive_queue_messages(
            channel="python-queues.delayed-messages",
            max_messages=1,
            wait_timeout_in_seconds=1,
            auto_ack=True,
        )
        print(f"Immediate poll: {len(response.messages)} messages (expected 0)")

        # Wait for the delay to expire
        print("Waiting 6 seconds for delay to expire...")
        time.sleep(6)

        # Now the message should be available
        response = client.receive_queue_messages(
            channel="python-queues.delayed-messages",
            max_messages=1,
            wait_timeout_in_seconds=5,
            auto_ack=True,
        )
        print(f"After delay: {len(response.messages)} messages (expected 1)")
        for msg in response.messages:
            print(f"  Received: {msg.body.decode('utf-8')}")


if __name__ == "__main__":
    main()
