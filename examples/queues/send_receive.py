"""Example: Send and receive — basic queue message send and receive with manual ack."""

from __future__ import annotations

from kubemq.queues import Client as QueuesClient
from kubemq import KubeMQConnectionError, KubeMQError, QueueMessage


def main() -> None:
    try:
        with QueuesClient(
            address="localhost:50000",  # TODO: Replace with your KubeMQ server address
            client_id="python-queues-send-receive-client",
        ) as client:
            # Send a message
            result = client.send_queue_message(
                QueueMessage(
                    channel="python-queues.send-receive",
                    body=b"Hello from queue!",
                    metadata="example-metadata",
                    tags={"source": "send_receive_example"},
                )
            )
            print(f"Sent: id={result.id}, sent_at={result.sent_at}, error={result.error}")

            # Receive and acknowledge the message
            response = client.receive_queue_messages(
                channel="python-queues.send-receive",
                max_messages=1,
                wait_timeout_in_seconds=10,
            )
            for msg in response.messages:
                print(f"Received: id={msg.id}, body={msg.body.decode('utf-8')}")
                msg.ack()
                print("  Message acknowledged")
    except KubeMQConnectionError as e:
        print(f"Connection error: {e}")
    except KubeMQError as e:
        print(f"KubeMQ error: {e}")


if __name__ == "__main__":
    main()

# Expected output:
# Sent: id=<message-id>, sent_at=<timestamp>, error=
# Received: id=<message-id>, body=Hello from queue!
#   Message acknowledged
