"""Quick start: Async RPC (Commands) — send a command using the native async client."""

from __future__ import annotations

import asyncio

from kubemq import (
    AsyncCancellationToken,
    AsyncCQClient,
    CommandMessage,
    CommandResponseMessage,
    CommandsSubscription,
)


async def main() -> None:
    async with AsyncCQClient(address="localhost:50000", client_id="python-rpc-quickstart-async-client") as client:
        token = AsyncCancellationToken()

        async def command_handler() -> None:
            async for cmd in client.subscribe_to_commands(
                subscription=CommandsSubscription(
                    channel="python-quickstart-commands-async",
                    on_receive_command_callback=lambda c: None,
                    on_error_callback=lambda e: print(f"Error: {e}"),
                ),
                cancellation_token=token,
            ):
                print(f"Received command: {cmd.body.decode('utf-8')}")
                await client.send_response(
                    CommandResponseMessage(command_received=cmd, is_executed=True)
                )

        # Start the command handler in the background
        handler_task = asyncio.create_task(command_handler())
        await asyncio.sleep(1)

        response = await client.send_command(
            CommandMessage(
                channel="python-quickstart-commands-async",
                body=b"Turn on lights",
                timeout_in_seconds=10,
            )
        )
        print(f"Command executed: {response.is_executed}")

        # Clean up
        token.cancel()
        handler_task.cancel()
        try:
            await handler_task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    asyncio.run(main())
