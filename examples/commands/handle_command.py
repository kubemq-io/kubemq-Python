"""Example: Handle command — subscribe and respond to incoming commands."""

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
    async with AsyncCQClient(
        address="localhost:50000",
        client_id="python-commands-handle-command-client",
    ) as client:
        token = AsyncCancellationToken()

        async def command_handler() -> None:
            async for cmd in client.subscribe_to_commands(
                subscription=CommandsSubscription(
                    channel="python-commands.handle-command",
                    on_receive_command_callback=lambda c: None,
                    on_error_callback=lambda e: print(f"Subscription error: {e}"),
                ),
                cancellation_token=token,
            ):
                body = cmd.body.decode("utf-8")
                print(f"Handling command: Id={cmd.id}, Body={body}")
                await client.send_response(
                    CommandResponse(command_received=cmd, is_executed=True)
                )
                print(f"  Response sent: executed=True")

        handler_task = asyncio.create_task(command_handler())
        print("Listening for commands on 'python-commands.handle-command'...")
        await asyncio.sleep(1)

        response = await client.send_command(
            CommandMessage(
                channel="python-commands.handle-command",
                body=b"process this task",
                timeout_in_seconds=10,
            )
        )
        print(f"Command result: executed={response.is_executed}")

        await asyncio.sleep(1)
        token.cancel()
        handler_task.cancel()
        try:
            await handler_task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    asyncio.run(main())
