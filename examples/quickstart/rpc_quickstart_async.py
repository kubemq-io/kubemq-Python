"""Quick start: Async RPC (Commands) — send a command using the native async client."""

from __future__ import annotations

import asyncio

from kubemq import (
    AsyncCQClient,
    CommandMessage,
    CommandResponseMessage,
    CommandsSubscription,
    AsyncCancellationToken,
)


async def main() -> None:
    async with AsyncCQClient(address="localhost:50000") as client:

        async def on_command(cmd) -> None:  # type: ignore[no-untyped-def]
            print(f"Received command: {cmd.body.decode('utf-8')}")
            await client.send_response_message(
                CommandResponseMessage(command_received=cmd, is_executed=True)
            )

        await client.subscribe_to_commands(
            subscription=CommandsSubscription(
                channel="quickstart-commands",
                on_receive_command_callback=on_command,
                on_error_callback=lambda e: print(f"Error: {e}"),
            ),
            cancel=AsyncCancellationToken(),
        )

        await asyncio.sleep(1)

        response = await client.send_command(
            CommandMessage(
                channel="quickstart-commands",
                body=b"Turn on lights",
                timeout_in_seconds=10,
            )
        )
        print(f"Command executed: {response.is_executed}")


if __name__ == "__main__":
    asyncio.run(main())
