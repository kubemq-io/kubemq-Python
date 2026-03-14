"""Selective acknowledgment of messages by sequence using per-message ack."""

from kubemq.queues import *


def main():
    with Client(address="localhost:50000") as client:
        for i in range(3):
            client.send_queue_message(
                QueueMessage(channel="ack-range-demo", body=f"msg-{i}".encode())
            )

        response = client.receive_queue_messages(
            channel="ack-range-demo",
            max_messages=3,
            wait_timeout_in_seconds=10,
        )
        print(f"Received {len(response.messages)} messages")
        for msg in response.messages:
            if msg.sequence % 2 == 0:
                msg.ack()
                print(f"  Acked: seq={msg.sequence}")
            else:
                msg.reject()
                print(f"  Rejected: seq={msg.sequence}")


if __name__ == "__main__":
    main()
