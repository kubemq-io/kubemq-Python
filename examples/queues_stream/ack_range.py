"""Example: Ack range — selectively acknowledge messages by sequence."""

from __future__ import annotations

from kubemq import Client, QueueMessage


def main() -> None:
    with Client(
        address="localhost:50000",
        client_id="python-queues-stream-ack-range-client",
    ) as client:
        for i in range(3):
            client.send_queue_message(
                QueueMessage(
                    channel="python-queues-stream.ack-range",
                    body=f"msg-{i}".encode(),
                )
            )

        response = client.receive_queue_messages(
            channel="python-queues-stream.ack-range",
            max_messages=3,
            wait_timeout_in_seconds=10,
        )
        print(f"Received {len(response.messages)} messages")
        for msg in response.messages:
            if msg.sequence % 2 == 0:
                msg.ack()
                print(f"  Acked: seq={msg.sequence}")
            else:
                msg.nack()
                print(f"  Rejected: seq={msg.sequence}")


if __name__ == "__main__":
    main()
