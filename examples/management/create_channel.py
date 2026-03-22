"""Example: Create channel — create channels for events, events store, queues, and CQ."""

from __future__ import annotations

from kubemq import Client


def main() -> None:
    # Create events channel
    with Client(
        address="localhost:50000",
        client_id="python-management-create-channel-client",
    ) as client:
        try:
            client.create_events_channel("python-management.create-events")
            print("Events channel created: python-management.create-events")
        except Exception as e:
            print(f"Error creating events channel: {e}")

        try:
            client.create_events_store_channel("python-management.create-events-store")
            print("Events store channel created: python-management.create-events-store")
        except Exception as e:
            print(f"Error creating events store channel: {e}")

    # Create queues channel
    with Client(
        address="localhost:50000",
        client_id="python-management-create-channel-client",
    ) as client:
        try:
            client.create_queues_channel("python-management.create-queues")
            print("Queues channel created: python-management.create-queues")
        except Exception as e:
            print(f"Error creating queues channel: {e}")

    # Create CQ channels
    with Client(
        address="localhost:50000",
        client_id="python-management-create-channel-client",
    ) as client:
        try:
            client.create_commands_channel("python-management.create-commands")
            print("Commands channel created: python-management.create-commands")
        except Exception as e:
            print(f"Error creating commands channel: {e}")

        try:
            client.create_queries_channel("python-management.create-queries")
            print("Queries channel created: python-management.create-queries")
        except Exception as e:
            print(f"Error creating queries channel: {e}")


if __name__ == "__main__":
    main()
