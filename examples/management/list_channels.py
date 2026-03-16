"""Example: List channels — list all channels by type."""

from __future__ import annotations

from kubemq import CQClient, PubSubClient, QueuesClient


def main() -> None:
    # List events and events store channels
    with PubSubClient(
        address="localhost:50000",
        client_id="python-management-list-channels-client",
    ) as client:
        try:
            events_channels = client.list_events_channels()
            print(f"Events channels: {events_channels}")
        except Exception as e:
            print(f"Error listing events channels: {e}")

        try:
            events_store_channels = client.list_events_store_channels()
            print(f"Events store channels: {events_store_channels}")
        except Exception as e:
            print(f"Error listing events store channels: {e}")

    # List queues channels
    with QueuesClient(
        address="localhost:50000",
        client_id="python-management-list-channels-client",
    ) as client:
        try:
            queues_channels = client.list_queues_channels()
            print(f"Queues channels: {queues_channels}")
        except Exception as e:
            print(f"Error listing queues channels: {e}")

    # List CQ channels
    with CQClient(
        address="localhost:50000",
        client_id="python-management-list-channels-client",
    ) as client:
        try:
            commands_channels = client.list_commands_channels()
            print(f"Commands channels: {commands_channels}")
        except Exception as e:
            print(f"Error listing commands channels: {e}")

        try:
            queries_channels = client.list_queries_channels()
            print(f"Queries channels: {queries_channels}")
        except Exception as e:
            print(f"Error listing queries channels: {e}")


if __name__ == "__main__":
    main()
