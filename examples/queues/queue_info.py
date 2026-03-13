"""Get queue information and statistics using queues_info()."""

from kubemq.queues import *


def main():
    with Client(address="localhost:50000") as client:
        client.send_queues_message(
            QueueMessage(channel="info-demo", body=b"test message")
        )

        info = client.queues_info()
        print(f"All queues info: {info}")

        info_specific = client.queues_info("info-demo")
        print(f"Queue info for 'info-demo': {info_specific}")


if __name__ == "__main__":
    main()
