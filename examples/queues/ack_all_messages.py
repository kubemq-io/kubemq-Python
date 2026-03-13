"""Ack all pending messages in a queue using ack_all_queue_messages()."""

from kubemq.queues import *


def main():
    with Client(address="localhost:50000") as client:
        for i in range(10):
            client.send_queues_message(
                QueueMessage(channel="ack-all-demo", body=f"Msg-{i + 1}".encode())
            )
        print("Sent 10 messages")

        acked = client.ack_all_queue_messages("ack-all-demo")
        print(f"Acknowledged {acked} messages")


if __name__ == "__main__":
    main()
