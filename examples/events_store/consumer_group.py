"""Example: Consumer group — load-balance events store messages across subscribers."""

from __future__ import annotations

import asyncio

from kubemq import AsyncCancellationToken, AsyncPubSubClient, EventStoreMessage, EventsStoreSubscription
from kubemq.pubsub import EventStoreStartPosition


async def main() -> None:
    async with AsyncPubSubClient(
        address="localhost:50000",
        client_id="python-events-store-consumer-group-client",
    ) as client:
        token = AsyncCancellationToken()

        async def make_processor(name: str) -> None:
            async for event in client.subscribe_to_events_store(
                subscription=EventsStoreSubscription(
                    channel="python-events-store.consumer-group",
                    group="processors",
                    on_receive_event_callback=lambda e: None,
                    on_error_callback=lambda e: print(f"Error: {e}"),
                    events_store_type=EventStoreStartPosition.StartFromFirst,
                ),
                cancellation_token=token,
            ):
                print(f"[{name}] Seq:{event.sequence}, Body:{event.body.decode('utf-8')}")

        task1 = asyncio.create_task(make_processor("Processor-1"))
        task2 = asyncio.create_task(make_processor("Processor-2"))
        await asyncio.sleep(1)

        for i in range(6):
            await client.send_event_store(
                EventStoreMessage(
                    channel="python-events-store.consumer-group",
                    body=f"Event-{i + 1}".encode(),
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
