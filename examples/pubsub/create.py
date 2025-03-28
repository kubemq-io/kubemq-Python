from kubemq.pubsub import *


def create_events_channel(channelName):
    try:
        with Client(address="localhost:50000") as client:
            client.create_events_channel(channelName)
            print(f"Events channel: {channelName} created successfully.")
    except Exception as e:
        print(f"Error creating Events Channel: {channelName},  {e}")


def create_events_store_channel(channelName):
    try:
        with Client(address="localhost:50000") as client:
            client.create_events_store_channel(channelName)
            print(f"Events Store channel: {channelName} created successfully.")
    except Exception as e:
        print(f"Error creating Events Store Channel: {channelName},  {e}")


if __name__ == "__main__":
    create_events_channel("new_events_channel")
    create_events_store_channel("new_events_store_channel")
