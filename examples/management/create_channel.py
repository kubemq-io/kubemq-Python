"""Example: Create channel — create channels for events, events store, queues, and CQ."""

from __future__ import annotations

import asyncio

from kubemq.cq import Client as CQClient
from kubemq.pubsub import Client as PubSubClient
from kubemq.queues import Client as QueuesClient


async def main() -> None:
    async with PubSubClient(
        address="localhost:50000",
        client_id="python-management-create-channel-client",
    ) as client:
        try:
            await client.create_events_channel_async("python-management.create-events")
            print("Events channel created: python-management.create-events")
        except Exception as e:
            print(f"Error creating events channel: {e}")

        try:
            await client.create_events_store_channel_async("python-management.create-events-store")
            print("Events store channel created: python-management.create-events-store")
        except Exception as e:
            print(f"Error creating events store channel: {e}")

    async with QueuesClient(
        address="localhost:50000",
        client_id="python-management-create-channel-client",
    ) as client:
        try:
            await client.create_queues_channel_async("python-management.create-queues")
            print("Queues channel created: python-management.create-queues")
        except Exception as e:
            print(f"Error creating queues channel: {e}")

    async with CQClient(
        address="localhost:50000",
        client_id="python-management-create-channel-client",
    ) as client:
        try:
            await client.create_commands_channel_async("python-management.create-commands")
            print("Commands channel created: python-management.create-commands")
        except Exception as e:
            print(f"Error creating commands channel: {e}")

        try:
            await client.create_queries_channel_async("python-management.create-queries")
            print("Queries channel created: python-management.create-queries")
        except Exception as e:
            print(f"Error creating queries channel: {e}")


if __name__ == "__main__":
    asyncio.run(main())
