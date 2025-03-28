from kubemq.queues import *


def create_queues_channel(channelName):
    try:
        with Client(address="localhost:50000") as client:
            client.create_queues_channel(channelName)
            print(f"Queues channel: {channelName} created successfully.")
    except Exception as e:
        print(f"Error creating Queues Channel: {channelName},  {e}")


if __name__ == "__main__":
    create_queues_channel("new_queues_channel")
