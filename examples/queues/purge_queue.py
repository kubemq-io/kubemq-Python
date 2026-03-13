"""Purge all messages from a queue channel using delete_queues_channel()."""

from kubemq.queues import *


def main():
    with Client(address="localhost:50000") as client:
        for i in range(5):
            client.send_queues_message(
                QueueMessage(channel="purge-demo", body=f"Msg-{i + 1}".encode())
            )
        print("Sent 5 messages")

        client.delete_queues_channel("purge-demo")
        print("Queue 'purge-demo' purged/deleted")


if __name__ == "__main__":
    main()
