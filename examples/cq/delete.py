from kubemq.cq import *


def delete_commands_channel(channelName):
    try:
        with Client(address="localhost:50000", client_id="delete_example") as client:
            client.delete_commands_channel(channelName)
            print(f"Commands channel: {channelName} deleted successfully.")
    except Exception as e:
        print(f"Error deleting Commands Channel: {channelName},  {e}")


def delete_queries_channel(channelName):
    try:
        with Client(address="localhost:50000", client_id="delete_example") as client:
            client.delete_queries_channel(channelName)
            print(f"Queries channel: {channelName} deleted successfully.")
    except Exception as e:
        print(f"Error creating Queries Channel: {channelName},  {e}")


if __name__ == "__main__":
    delete_commands_channel("new_commands_channel")
    delete_queries_channel("new_queries_channel")
