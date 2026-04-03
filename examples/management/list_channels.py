"""Example: List channels — list all channels by type."""

from __future__ import annotations

import asyncio

from kubemq.cq import Client as CQClient
from kubemq.pubsub import Client as PubSubClient
from kubemq.queues import Client as QueuesClient


async def main() -> None:
    async with PubSubClient(
        address="localhost:50000",
        client_id="python-management-list-channels-client",
    ) as client:
        try:
            events_channels = await client.list_events_channels_async()
            print(f"Events channels: {events_channels}")
        except Exception as e:
            print(f"Error listing events channels: {e}")

        try:
            events_store_channels = await client.list_events_store_channels_async()
            print(f"Events store channels: {events_store_channels}")
        except Exception as e:
            print(f"Error listing events store channels: {e}")

    async with QueuesClient(
        address="localhost:50000",
        client_id="python-management-list-channels-client",
    ) as client:
        try:
            queues_channels = await client.list_queues_channels_async()
            print(f"Queues channels: {queues_channels}")
        except Exception as e:
            print(f"Error listing queues channels: {e}")

    async with CQClient(
        address="localhost:50000",
        client_id="python-management-list-channels-client",
    ) as client:
        try:
            commands_channels = await client.list_commands_channels_async()
            print(f"Commands channels: {commands_channels}")
        except Exception as e:
            print(f"Error listing commands channels: {e}")

        try:
            queries_channels = await client.list_queries_channels_async()
            print(f"Queries channels: {queries_channels}")
        except Exception as e:
            print(f"Error listing queries channels: {e}")


if __name__ == "__main__":
    asyncio.run(main())
