"""Single-shot poll pattern for queue messages."""

from kubemq.queues import *


def main():
    with Client(address="localhost:50000") as client:
        client.send_queue_message(
            QueueMessage(channel="poll-demo", body=b"message for polling")
        )
        print("Sent 1 message")

        response = client.receive_queue_messages(
            channel="poll-demo",
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
