"""Example: Consumer group — load-balance commands across multiple handlers."""

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
        client_id="python-commands-consumer-group-client",
    ) as client:
        token = AsyncCancellationToken()

        async def make_handler(name: str) -> None:
            async for cmd in client.subscribe_to_commands(
                subscription=CommandsSubscription(
                    channel="python-commands.consumer-group",
                    group="handlers",
                    on_receive_command_callback=lambda c: None,
                    on_error_callback=lambda e: print(f"Error: {e}"),
                ),
                cancellation_token=token,
            ):
                print(f"[{name}] Received command: {cmd.body.decode('utf-8')}")
                await client.send_response(
                    CommandResponse(command_received=cmd, is_executed=True)
                )

        task1 = asyncio.create_task(make_handler("Handler-1"))
        task2 = asyncio.create_task(make_handler("Handler-2"))
        await asyncio.sleep(1)

        for i in range(4):
            response = await client.send_command(
                CommandMessage(
                    channel="python-commands.consumer-group",
                    body=f"Command #{i + 1}".encode(),
                    timeout_in_seconds=10,
                )
            )
            print(f"Command #{i + 1} executed: {response.is_executed}")

        token.cancel()
        for t in [task1, task2]:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass


if __name__ == "__main__":
    asyncio.run(main())
