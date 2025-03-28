from kubemq.cq import *


def create_commands_channel(channelName):
    try:
        with Client(address="localhost:50000") as client:
            client.create_commands_channel(channelName)
            print(f"Commands channel: {channelName} created successfully.")
    except Exception as e:
        print(f"Error creating Commands Channel: {channelName},  {e}")


def create_queries_channel(channelName):
    try:
        with Client(address="localhost:50000") as client:
            client.create_queries_channel(channelName)
            print(f"Queries channel: {channelName} created successfully.")
    except Exception as e:
        print(f"Error creating Queries Channel: {channelName},  {e}")


if __name__ == "__main__":
    create_commands_channel("new_commands_channel")
    create_queries_channel("new_queries_channel")
