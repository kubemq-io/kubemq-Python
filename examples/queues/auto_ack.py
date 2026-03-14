"""Receive messages with automatic acknowledgment (no manual ack needed)."""

from kubemq.queues import *


def main():
    with Client(address="localhost:50000") as client:
        client.send_queue_message(
            QueueMessage(channel="auto-ack-demo", body=b"auto-acked-message")
        )

        response = client.receive_queue_messages(
            channel="auto-ack-demo",
            max_messages=10,
            wait_timeout_in_seconds=5,
            auto_ack=True,
        )
        for msg in response.messages:
            print(f"Received (auto-acked): id={msg.id}, body={msg.body.decode('utf-8')}")


if __name__ == "__main__":
    main()
