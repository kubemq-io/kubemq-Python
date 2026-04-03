"""Example: Cancel subscription — demonstrate cancelling an active subscription."""

from __future__ import annotations

import asyncio

from kubemq import AsyncCancellationToken, AsyncPubSubClient, EventMessage, EventsSubscription


async def main() -> None:
    async with AsyncPubSubClient(
        address="localhost:50000",
        client_id="python-events-cancel-subscription-client",
    ) as client:
        token = AsyncCancellationToken()

        async def subscriber() -> None:
            async for event in client.subscribe_to_events(
                subscription=EventsSubscription(
                    channel="python-events.cancel-subscription",
                    on_receive_event_callback=lambda e: None,
                    on_error_callback=lambda e: print(f"Error: {e}"),
                ),
                cancellation_token=token,
            ):
                print(f"Received: {event.body.decode('utf-8')}")

        task = asyncio.create_task(subscriber())
        await asyncio.sleep(1)

        await client.publish_event(
            EventMessage(channel="python-events.cancel-subscription", body=b"before cancel")
        )
        await asyncio.sleep(1)

        token.cancel()
        print("Subscription cancelled")

        await asyncio.sleep(1)
        await client.publish_event(
            EventMessage(channel="python-events.cancel-subscription", body=b"after cancel")
        )
        print("Message sent after cancel — subscriber will NOT receive it")

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    asyncio.run(main())
