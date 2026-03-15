"""Purge all messages from a queue using ack_all_queue_messages()."""

from kubemq.queues import *


def main():
    with Client(address="localhost:50000") as client:
        for i in range(5):
            client.send_queue_message(
                QueueMessage(channel="purge-demo", body=f"Msg-{i + 1}".encode())
            )
        print("Sent 5 messages")

        acked = client.ack_all_queue_messages("purge-demo")
        print(f"Purged {acked} messages from 'purge-demo'")


if __name__ == "__main__":
    main()
