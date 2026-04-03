"""Example: Cancel subscription — demonstrate cancelling an events store subscription."""

from __future__ import annotations

import asyncio

from kubemq import AsyncCancellationToken, AsyncPubSubClient, EventStoreMessage, EventsStoreSubscription
from kubemq.pubsub import EventStoreStartPosition


async def main() -> None:
    async with AsyncPubSubClient(
        address="localhost:50000",
        client_id="python-events-store-cancel-subscription-client",
    ) as client:
        token = AsyncCancellationToken()

        async def subscriber() -> None:
            async for event in client.subscribe_to_events_store(
                subscription=EventsStoreSubscription(
                    channel="python-events-store.cancel-subscription",
                    on_receive_event_callback=lambda e: None,
                    on_error_callback=lambda e: print(f"Error: {e}"),
                    events_store_type=EventStoreStartPosition.StartFromNew,
                ),
                cancellation_token=token,
            ):
                print(f"Received: Seq={event.sequence}, Body:{event.body.decode('utf-8')}")

        task = asyncio.create_task(subscriber())
        await asyncio.sleep(1)

        await client.send_event_store(
            EventStoreMessage(
                channel="python-events-store.cancel-subscription",
                body=b"before cancel",
            )
        )
        await asyncio.sleep(1)

        token.cancel()
        print("Events store subscription cancelled")

        await client.send_event_store(
            EventStoreMessage(
                channel="python-events-store.cancel-subscription",
                body=b"after cancel",
            )
        )
        print("Message sent after cancel — subscriber will NOT receive it")

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    asyncio.run(main())
