from kubemq.cq import *


def list_commands_channels(search_pattern=""):
    try:
        with Client(address="localhost:50000", client_id="list_example") as client:
            list = client.list_commands_channels(search_pattern)
            print(f"Commands channels list: {list}")
    except Exception as e:
        print(f"Error listing Commands Channels: {e}")


def list_queries_channels(search_pattern=""):
    try:
        with Client(address="localhost:50000", client_id="list_example") as client:
            list = client.list_queries_channels(search_pattern)
            print(f"Queries channels list: {list}")
    except Exception as e:
        print(f"Error listing Queries Channels: {e}")


if __name__ == "__main__":
    list_commands_channels()
    list_queries_channels()
