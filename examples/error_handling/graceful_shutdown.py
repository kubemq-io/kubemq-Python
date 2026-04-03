"""Example: Graceful shutdown — properly close clients and cancel subscriptions."""

from __future__ import annotations

import asyncio
import signal

from kubemq import AsyncCancellationToken, AsyncPubSubClient, EventMessage, EventsSubscription


async def main() -> None:
    client = AsyncPubSubClient(
        address="localhost:50000",
        client_id="python-error-handling-graceful-shutdown-client",
    )
    token = AsyncCancellationToken()
    shutdown_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: shutdown_event.set())

    try:
        await client.connect()

        async def subscriber() -> None:
            async for event in client.subscribe_to_events(
                subscription=EventsSubscription(
                    channel="python-error-handling.graceful-shutdown",
                    on_receive_event_callback=lambda e: None,
                    on_error_callback=lambda e: print(f"Error: {e}"),
                ),
                cancellation_token=token,
            ):
                print(f"Received: {event.body.decode('utf-8')}")

        sub_task = asyncio.create_task(subscriber())
        print("Subscription active. Press Ctrl+C to shutdown gracefully.")
        await asyncio.sleep(1)

        for i in range(3):
            if shutdown_event.is_set():
                break
            await client.publish_event(
                EventMessage(
                    channel="python-error-handling.graceful-shutdown",
                    body=f"Message #{i + 1}".encode(),
                )
            )
            await asyncio.sleep(0.5)

        await asyncio.sleep(1)
    finally:
        print("Step 1: Cancelling subscriptions...")
        token.cancel()
        sub_task.cancel()
        try:
            await sub_task
        except asyncio.CancelledError:
            pass

        print("Step 2: Closing client...")
        await client.close()
        print("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
