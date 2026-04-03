"""Example: Basic connection — connect to KubeMQ and verify with ping."""

from __future__ import annotations

import asyncio

from kubemq import AsyncQueuesClient


async def main() -> None:
    async with AsyncQueuesClient(
        address="localhost:50000",
        client_id="python-connection-connect-client",
    ) as client:
        server_info = await client.ping()
        print(f"Connected to KubeMQ server: {server_info}")


if __name__ == "__main__":
    asyncio.run(main())

# Expected output:
# Connected to KubeMQ server: <server-info>
