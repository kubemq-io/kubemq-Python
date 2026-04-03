"""Example: Close — demonstrate graceful client close."""

from __future__ import annotations

import asyncio

from kubemq import AsyncPubSubClient, EventMessage


async def main() -> None:
    client = AsyncPubSubClient(
        address="localhost:50000",
        client_id="python-connection-close-client",
    )
    try:
        await client.connect()
        await client.publish_event(
            EventMessage(channel="python-connection.close", body=b"Hello before close")
        )
        print("Event sent successfully")
    finally:
        await client.close()
        print("Client closed gracefully")


if __name__ == "__main__":
    asyncio.run(main())
