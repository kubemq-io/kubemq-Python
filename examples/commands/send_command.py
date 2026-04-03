"""Example: Send command — send a command and receive a response."""

from __future__ import annotations

import asyncio

from kubemq import (
    AsyncCancellationToken,
    AsyncCQClient,
    CommandMessage,
    CommandResponse,
    CommandsSubscription,
    KubeMQConnectionError,
    KubeMQError,
    KubeMQTimeoutError,
)


async def main() -> None:
    try:
        async with AsyncCQClient(
            address="localhost:50000",
            client_id="python-commands-send-command-client",
        ) as client:
            token = AsyncCancellationToken()

            async def command_handler() -> None:
                async for cmd in client.subscribe_to_commands(
                    subscription=CommandsSubscription(
                        channel="python-commands.send-command",
                        on_receive_command_callback=lambda c: None,
                        on_error_callback=lambda e: print(f"Error: {e}"),
                    ),
                    cancellation_token=token,
                ):
                    print(f"Responder received: {cmd.body.decode('utf-8')}")
                    await client.send_response(
                        CommandResponse(command_received=cmd, is_executed=True)
                    )

            handler_task = asyncio.create_task(command_handler())
            await asyncio.sleep(1)

            response = await client.send_command(
                CommandMessage(
                    channel="python-commands.send-command",
                    body=b"hello kubemq, please reply!",
                    timeout_in_seconds=10,
                )
            )
            print(
                f"Response: executed={response.is_executed}, "
                f"timestamp={response.timestamp}, error={response.error}"
            )

            token.cancel()
            handler_task.cancel()
            try:
                await handler_task
            except asyncio.CancelledError:
                pass
    except KubeMQConnectionError as e:
        print(f"Connection error: {e}")
    except KubeMQTimeoutError as e:
        print(f"Timeout error: {e}")
    except KubeMQError as e:
        print(f"KubeMQ error: {e}")


if __name__ == "__main__":
    asyncio.run(main())

# Expected output:
# Responder received: hello kubemq, please reply!
# Response: executed=True, timestamp=<timestamp>, error=
