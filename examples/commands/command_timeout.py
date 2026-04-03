"""Example: Command timeout — demonstrate command timeout when no responder exists."""

from __future__ import annotations

import asyncio

from kubemq import AsyncCQClient, CommandMessage


async def main() -> None:
    async with AsyncCQClient(
        address="localhost:50000",
        client_id="python-commands-command-timeout-client",
    ) as client:
        try:
            response = await client.send_command(
                CommandMessage(
                    channel="python-commands.command-timeout",
                    body=b"this will time out",
                    timeout_in_seconds=3,
                )
            )
            print(f"Executed: {response.is_executed}, Error: {response.error}")
        except Exception as e:
            print(f"Command timed out as expected: {e}")


if __name__ == "__main__":
    asyncio.run(main())
