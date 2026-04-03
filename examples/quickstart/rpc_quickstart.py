"""Quick start: RPC (Commands) — send a command and handle the response."""

from __future__ import annotations

import asyncio

from kubemq import (
    AsyncCancellationToken,
    AsyncCQClient,
    CommandMessage,
    CommandResponse,
    CommandsSubscription,
)


async def main() -> None:
    async with AsyncCQClient(address="localhost:50000", client_id="python-rpc-quickstart-client") as client:
        token = AsyncCancellationToken()

        async def command_handler() -> None:
            async for cmd in client.subscribe_to_commands(
                subscription=CommandsSubscription(
                    channel="python-quickstart-commands",
                    on_receive_command_callback=lambda c: None,
                    on_error_callback=lambda e: print(f"Error: {e}"),
                ),
                cancellation_token=token,
            ):
                print(f"Received command: {cmd.body.decode('utf-8')}")
                await client.send_response(
                    CommandResponse(command_received=cmd, is_executed=True)
                )

        handler_task = asyncio.create_task(command_handler())
        await asyncio.sleep(1)

        response = await client.send_command(
            CommandMessage(
                channel="python-quickstart-commands",
                body=b"Turn on lights",
                timeout_in_seconds=10,
            )
        )
        print(f"Command executed: {response.is_executed}")

        token.cancel()
        handler_task.cancel()
        try:
            await handler_task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    asyncio.run(main())
