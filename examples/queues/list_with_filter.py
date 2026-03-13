"""List queue channels with a search filter pattern."""

from kubemq.queues import *


def main():
    with Client(
        address="localhost:50000", client_id="list_filter_example"
    ) as client:
        all_channels = client.list_queues_channels("")
        print(f"All queue channels: {all_channels}")

        filtered = client.list_queues_channels("demo")
        print(f"Channels matching 'demo': {filtered}")


if __name__ == "__main__":
    main()
