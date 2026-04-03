"""Example: Custom timeouts — configure operation and reconnection timeouts."""

from __future__ import annotations

import asyncio

from kubemq import AsyncCQClient, ClientConfig, CommandMessage, KeepAliveConfig


async def main() -> None:
    config = ClientConfig(
        address="localhost:50000",
        client_id="python-connection-custom-timeouts-client",
        auto_reconnect=True,
        reconnect_interval_seconds=2,
        keep_alive=KeepAliveConfig(
            enabled=True,
            ping_interval_in_seconds=15,
            ping_timeout_in_seconds=5,
        ),
    )

    async with AsyncCQClient(config=config) as client:
        info = await client.ping()
        print(f"Connected to {info.host} with custom timeouts")

        try:
            response = await client.send_command(
                CommandMessage(
                    channel="python-connection.custom-timeouts",
                    body=b"test operation",
                    timeout_in_seconds=5,
                )
            )
            print(f"Command executed: {response.is_executed}")
        except Exception as e:
            print(f"Operation failed: {e}")


if __name__ == "__main__":
    asyncio.run(main())
