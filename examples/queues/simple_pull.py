"""Pull messages from a queue using the simple synchronous API."""

from kubemq.queues import *


def main():
    with Client(address="localhost:50000") as client:
        client.send_queue_message(
            QueueMessage(channel="pull-demo", body=b"message to pull")
        )

        result = client.pull("pull-demo", max_messages=5, wait_timeout_in_seconds=10)
        if result.is_error:
            print(f"Error: {result.error}")
            return

        print(f"Pulled {len(result.messages)} messages:")
        for msg in result.messages:
            print(f"  id={msg.id}, body={msg.body.decode('utf-8')}")


if __name__ == "__main__":
    main()
