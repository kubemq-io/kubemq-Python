"""Quick start: Async Events — publish an event using the native async client."""

from __future__ import annotations

import asyncio

from kubemq import AsyncPubSubClient, EventMessage


async def main() -> None:
    async with AsyncPubSubClient(address="localhost:50000") as client:
        await client.publish_event(
            EventMessage(channel="quickstart", body=b"Hello async KubeMQ!")
        )
        print("Async event sent!")


if __name__ == "__main__":
    asyncio.run(main())
