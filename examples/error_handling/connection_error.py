"""Example: Connection error — handle connection failures gracefully."""

from __future__ import annotations

import asyncio

from kubemq import AsyncQueuesClient, KubeMQConnectionError


async def main() -> None:
    try:
        client = AsyncQueuesClient(
            address="localhost:59999",
            client_id="python-error-handling-connection-error-client",
        )
        await client.connect()
        server_info = await client.ping()
        print(f"Connected: {server_info}")
    except KubeMQConnectionError as e:
        print(f"Connection error (expected): {e}")
        print("  Tip: Verify the KubeMQ server is running at the specified address")
    except Exception as e:
        print(f"Unexpected error: {type(e).__name__}: {e}")


if __name__ == "__main__":
    asyncio.run(main())
