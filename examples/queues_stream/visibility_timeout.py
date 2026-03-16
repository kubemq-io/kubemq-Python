"""Example: Visibility timeout — extend processing time with visibility timer."""

from __future__ import annotations

import time

from kubemq import QueueMessage, QueuesClient


def main() -> None:
    with QueuesClient(
        address="localhost:50000",
        client_id="python-queues-stream-visibility-timeout-client",
    ) as client:
        client.send_queue_message(
            QueueMessage(
                channel="python-queues-stream.visibility-timeout",
                body=b"message with visibility",
            )
        )

        # Receive with a 5-second visibility timeout
        response = client.receive_queue_messages(
            channel="python-queues-stream.visibility-timeout",
            max_messages=1,
            wait_timeout_in_seconds=10,
            auto_ack=False,
            visibility_seconds=5,
        )

        for msg in response.messages:
            print(f"Received: {msg.body.decode('utf-8')}")
            print(f"  Visibility: {msg.visibility_seconds}s")

            # Simulate processing that takes a while — extend visibility
            time.sleep(2)
            msg.extend_visibility_timer(10)
            print("  Extended visibility by 10 seconds")

            # Continue processing
            time.sleep(3)
            msg.ack()
            print("  Processing complete, message acknowledged")


if __name__ == "__main__":
    main()
