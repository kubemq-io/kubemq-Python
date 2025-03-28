from kubemq.pubsub import *


def list_events_channels(search_pattern=""):
    try:
        with Client(address="localhost:50000", client_id="list_example") as client:
            list = client.list_events_channels(search_pattern)
            print(f"Events channels list: {list}")
    except Exception as e:
        print(f"Error listing Events Channels: {e}")


def list_events_store_channels(search_pattern=""):
    try:
        with Client(address="localhost:50000", client_id="list_example") as client:
            list = client.list_events_store_channels(search_pattern)
            print(f"Events Store channels list: {list}")
    except Exception as e:
        print(f"Error listing Events Store Channels: {e}")


if __name__ == "__main__":
    list_events_channels()
    list_events_store_channels()
