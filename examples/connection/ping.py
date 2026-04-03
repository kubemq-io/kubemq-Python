"""Example: Ping — check server health and retrieve server information."""

from __future__ import annotations

import asyncio

from kubemq import AsyncQueuesClient


async def main() -> None:
    async with AsyncQueuesClient(
        address="localhost:50000",
        client_id="python-connection-ping-client",
    ) as client:
        server_info = await client.ping()
        print(f"Server host: {server_info.host}")
        print(f"Server version: {server_info.version}")
        print(f"Server uptime: {server_info.server_up_time_seconds}s")
        print(f"Server is reachable: True")


if __name__ == "__main__":
    asyncio.run(main())
