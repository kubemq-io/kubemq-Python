"""Quick start: Async Events — publish an event using the native async client."""

from __future__ import annotations

import asyncio

from kubemq import EventMessage
from kubemq.pubsub import AsyncClient as AsyncPubSubClient


async def main() -> None:
    async with AsyncPubSubClient(address="localhost:50000", client_id="python-events-quickstart-async-client") as client:
        await client.send_event(EventMessage(channel="python-quickstart", body=b"Hello async KubeMQ!"))
        print("Async event sent!")


if __name__ == "__main__":
    asyncio.run(main())
