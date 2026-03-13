"""Peek at queue messages without consuming them (using the simple API waiting() method)."""

from kubemq.queues import *


def main():
    with Client(address="localhost:50000") as client:
        client.send_queues_message(
            QueueMessage(channel="peek-demo", body=b"peek-me", metadata="test")
        )

        waiting_result = client.waiting("peek-demo", 5, 10)
        if waiting_result.is_error:
            print(f"Error: {waiting_result.error}")
            return

        print(f"Messages waiting: {len(waiting_result.messages)}")
        for msg in waiting_result.messages:
            print(f"  Peek: id={msg.id}, body={msg.body.decode('utf-8')}")

        pull_result = client.pull("peek-demo", 5, 10)
        print(f"Messages pulled: {len(pull_result.messages)}")


if __name__ == "__main__":
    main()
