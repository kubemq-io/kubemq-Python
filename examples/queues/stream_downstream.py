"""Example: Queue stream downstream — receive messages with ack/reject/requeue."""

from __future__ import annotations

from kubemq import QueueMessage, QueuesClient


def main() -> None:
    with QueuesClient(address="localhost:50000") as client:
        # First, send some test messages
        for i in range(3):
            client.send_queue_message(
                QueueMessage(
                    channel="stream-downstream-demo",
                    body=f"Message #{i + 1}".encode(),
                )
            )
        print("Sent 3 test messages")

        # Receive messages with visibility timeout
        response = client.receive_queue_messages(
            channel="stream-downstream-demo",
            max_messages=10,
            wait_timeout_in_seconds=5,
            visibility_seconds=30,
        )

        for msg in response.messages:
            body = msg.body.decode("utf-8")
            print(f"Received: {body}")

            # Demonstrate different acknowledgment strategies
            if "1" in body:
                msg.ack()
                print(f"  -> Acknowledged: {body}")
            elif "2" in body:
                msg.reject()
                print(f"  -> Rejected (sent to DLQ): {body}")
            else:
                msg.ack()
                print(f"  -> Acknowledged: {body}")


if __name__ == "__main__":
    main()
