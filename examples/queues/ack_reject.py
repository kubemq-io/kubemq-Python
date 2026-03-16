"""Example: Ack and reject — demonstrate per-message ack and reject decisions."""

from __future__ import annotations

from kubemq import QueueMessage, QueuesClient


def main() -> None:
    with QueuesClient(
        address="localhost:50000",
        client_id="python-queues-ack-reject-client",
    ) as client:
        # Send test messages
        for i in range(4):
            client.send_queue_message(
                QueueMessage(
                    channel="python-queues.ack-reject",
                    body=f"Order-{i + 1}".encode(),
                )
            )
        print("Sent 4 messages")

        # Receive and selectively ack or reject
        response = client.receive_queue_messages(
            channel="python-queues.ack-reject",
            max_messages=4,
            wait_timeout_in_seconds=10,
        )
        for msg in response.messages:
            body = msg.body.decode("utf-8")
            # Simulate processing: ack even-numbered, reject odd-numbered
            if msg.sequence % 2 == 0:
                msg.ack()
                print(f"  Acked: {body} (seq={msg.sequence})")
            else:
                msg.reject()
                print(f"  Rejected: {body} (seq={msg.sequence})")


if __name__ == "__main__":
    main()
