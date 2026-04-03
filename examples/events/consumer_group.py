"""Example: Consumer group — load-balance events across multiple subscribers."""

from __future__ import annotations

import asyncio

from kubemq import AsyncCancellationToken, AsyncPubSubClient, EventMessage, EventsSubscription


async def main() -> None:
    async with AsyncPubSubClient(
        address="localhost:50000",
        client_id="python-events-consumer-group-client",
    ) as client:
        token = AsyncCancellationToken()

        async def make_worker(name: str) -> None:
            async for event in client.subscribe_to_events(
                subscription=EventsSubscription(
                    channel="python-events.consumer-group",
                    group="workers",
                    on_receive_event_callback=lambda e: None,
                    on_error_callback=lambda e: print(f"Error: {e}"),
                ),
                cancellation_token=token,
            ):
                print(f"[{name}] Received: {event.body.decode('utf-8')}")

        task1 = asyncio.create_task(make_worker("Worker-1"))
        task2 = asyncio.create_task(make_worker("Worker-2"))
        await asyncio.sleep(1)

        for i in range(6):
            await client.publish_event(
                EventMessage(
                    channel="python-events.consumer-group",
                    body=f"Task #{i + 1}".encode(),
                )
            )

        await asyncio.sleep(3)
        token.cancel()
        for t in [task1, task2]:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass


if __name__ == "__main__":
    asyncio.run(main())
