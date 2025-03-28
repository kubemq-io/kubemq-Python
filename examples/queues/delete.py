from kubemq.queues import *


def delete_queues_channel(channelName):
    try:
        with Client(address="localhost:50000", client_id="delete_example") as client:
            client.delete_queues_channel(channelName)
            print(f"Queues channel: {channelName} deleted successfully.")
    except Exception as e:
        print(f"Error deleting Queues Channel: {channelName},  {e}")


if __name__ == "__main__":
    delete_queues_channel("new_queues_channel")
