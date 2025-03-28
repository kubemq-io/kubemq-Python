from kubemq.queues import *


def list_queues_channels(search_pattern=""):
    try:
        with Client(address="localhost:50000", client_id="list_example") as client:
            list = client.list_queues_channels(search_pattern)
            print(f"Queues channels list: {list}")
    except Exception as e:
        print(f"Error listing Queues Channels: {e}")


if __name__ == "__main__":
    list_queues_channels()
