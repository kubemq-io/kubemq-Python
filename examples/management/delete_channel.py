"""Example: Delete channel — delete channels for events, events store, queues, and CQ."""

from __future__ import annotations

from kubemq import Client


def main() -> None:
    # Delete events channels
    with Client(
        address="localhost:50000",
        client_id="python-management-delete-channel-client",
    ) as client:
        try:
            client.delete_events_channel("python-management.create-events")
            print("Events channel deleted: python-management.create-events")
        except Exception as e:
            print(f"Error deleting events channel: {e}")

        try:
            client.delete_events_store_channel("python-management.create-events-store")
            print("Events store channel deleted: python-management.create-events-store")
        except Exception as e:
            print(f"Error deleting events store channel: {e}")

    # Delete queues channel
    with Client(
        address="localhost:50000",
        client_id="python-management-delete-channel-client",
    ) as client:
        try:
            client.delete_queues_channel("python-management.create-queues")
            print("Queues channel deleted: python-management.create-queues")
        except Exception as e:
            print(f"Error deleting queues channel: {e}")

    # Delete CQ channels
    with Client(
        address="localhost:50000",
        client_id="python-management-delete-channel-client",
    ) as client:
        try:
            client.delete_commands_channel("python-management.create-commands")
            print("Commands channel deleted: python-management.create-commands")
        except Exception as e:
            print(f"Error deleting commands channel: {e}")

        try:
            client.delete_queries_channel("python-management.create-queries")
            print("Queries channel deleted: python-management.create-queries")
        except Exception as e:
            print(f"Error deleting queries channel: {e}")


if __name__ == "__main__":
    main()
