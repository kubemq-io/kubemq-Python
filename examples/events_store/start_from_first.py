"""Example: StartFromFirst — replay all stored events from the beginning."""

from __future__ import annotations

import asyncio

from kubemq import AsyncCancellationToken, AsyncPubSubClient, EventStoreMessage, EventsStoreSubscription
from kubemq.pubsub import EventStoreStartPosition


async def main() -> None:
    async with AsyncPubSubClient(
        address="localhost:50000",
        client_id="python-events-store-start-from-first-client",
    ) as client:
        for i in range(5):
            await client.send_event_store(
                EventStoreMessage(
                    channel="python-events-store.start-from-first",
                    body=f"Message-{i + 1}".encode(),
                )
            )
        print("Sent 5 messages")
        await asyncio.sleep(1)

        token = AsyncCancellationToken()

        async def subscriber() -> None:
            async for event in client.subscribe_to_events_store(
                subscription=EventsStoreSubscription(
                    channel="python-events-store.start-from-first",
                    on_receive_event_callback=lambda e: None,
                    on_error_callback=lambda e: print(f"Error: {e}"),
                    events_store_type=EventStoreStartPosition.StartFromFirst,
                ),
                cancellation_token=token,
            ):
                print(f"[StartFromFirst] Seq:{event.sequence}, Body:{event.body.decode('utf-8')}")

        task = asyncio.create_task(subscriber())
        await asyncio.sleep(3)
        token.cancel()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    asyncio.run(main())
