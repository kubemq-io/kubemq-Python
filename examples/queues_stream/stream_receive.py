"""Example: Stream receive — receive messages with ack/reject/requeue options."""

from __future__ import annotations

from kubemq import QueueMessage, QueuesClient


def main() -> None:
    with QueuesClient(
        address="localhost:50000",
        client_id="python-queues-stream-stream-receive-client",
    ) as client:
        # Send some test messages
        for i in range(3):
            client.send_queue_message(
                QueueMessage(
                    channel="python-queues-stream.stream-receive",
                    body=f"Message #{i + 1}".encode(),
                )
            )
        print("Sent 3 test messages")

        # Receive messages via streaming
        response = client.receive_queue_messages(
            channel="python-queues-stream.stream-receive",
            max_messages=10,
            wait_timeout_in_seconds=5,
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
                print(f"  -> Rejected: {body}")
            else:
                msg.ack()
                print(f"  -> Acknowledged: {body}")


if __name__ == "__main__":
    main()
