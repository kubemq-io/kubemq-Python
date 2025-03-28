from kubemq.pubsub import *


def delete_events_channel(channelName):
    try:
        with Client(address="localhost:50000", client_id="delete_example") as client:
            client.delete_events_channel(channelName)
            print(f"Events channel: {channelName} deleted successfully.")
    except Exception as e:
        print(f"Error deleting Events Channel: {channelName},  {e}")


def delete_events_store_channel(channelName):
    try:
        with Client(address="localhost:50000", client_id="delete_example") as client:
            client.delete_events_store_channel(channelName)
            print(f"Events Store channel: {channelName} deleted successfully.")
    except Exception as e:
        print(f"Error creating Events Store Channel: {channelName},  {e}")


if __name__ == "__main__":
    delete_events_channel("new_events_channel")
    delete_events_store_channel("new_events_store_channel")
