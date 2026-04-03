"""Example: Delete channel — delete channels for events, events store, queues, and CQ."""

from __future__ import annotations

import asyncio

from kubemq.cq import Client as CQClient
from kubemq.pubsub import Client as PubSubClient
from kubemq.queues import Client as QueuesClient


async def main() -> None:
    async with PubSubClient(
        address="localhost:50000",
        client_id="python-management-delete-channel-client",
    ) as client:
        try:
            await client.delete_events_channel_async("python-management.create-events")
            print("Events channel deleted: python-management.create-events")
        except Exception as e:
            print(f"Error deleting events channel: {e}")

        try:
            await client.delete_events_store_channel_async("python-management.create-events-store")
            print("Events store channel deleted: python-management.create-events-store")
        except Exception as e:
            print(f"Error deleting events store channel: {e}")

    async with QueuesClient(
        address="localhost:50000",
        client_id="python-management-delete-channel-client",
    ) as client:
        try:
            await client.delete_queues_channel_async("python-management.create-queues")
            print("Queues channel deleted: python-management.create-queues")
        except Exception as e:
            print(f"Error deleting queues channel: {e}")

    async with CQClient(
        address="localhost:50000",
        client_id="python-management-delete-channel-client",
    ) as client:
        try:
            await client.delete_commands_channel_async("python-management.create-commands")
            print("Commands channel deleted: python-management.create-commands")
        except Exception as e:
            print(f"Error deleting commands channel: {e}")

        try:
            await client.delete_queries_channel_async("python-management.create-queries")
            print("Queries channel deleted: python-management.create-queries")
        except Exception as e:
            print(f"Error deleting queries channel: {e}")


if __name__ == "__main__":
    asyncio.run(main())
