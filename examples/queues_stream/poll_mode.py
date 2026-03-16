"""Example: Poll mode — single-shot polling pattern for queue messages."""

from __future__ import annotations

from kubemq import QueueMessage, QueuesClient


def main() -> None:
    with QueuesClient(
        address="localhost:50000",
        client_id="python-queues-stream-poll-mode-client",
    ) as client:
        client.send_queue_message(
            QueueMessage(
                channel="python-queues-stream.poll-mode",
                body=b"message for polling",
            )
        )
        print("Sent 1 message")

        response = client.receive_queue_messages(
            channel="python-queues-stream.poll-mode",
            max_messages=10,
            wait_timeout_in_seconds=5,
            auto_ack=True,
        )

        if response.is_error:
            print(f"Error: {response.error}")
            return

        if not response.messages:
            print("No messages available")
            return

        print(f"Polled {len(response.messages)} messages:")
        for msg in response.messages:
            print(f"  id={msg.id}, body={msg.body.decode('utf-8')}")


if __name__ == "__main__":
    main()
