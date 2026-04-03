"""Example: Wildcard subscription — subscribe to events on channels matching a pattern."""

from __future__ import annotations

import asyncio

from kubemq import AsyncCancellationToken, AsyncPubSubClient, EventMessage, EventsSubscription


async def main() -> None:
    async with AsyncPubSubClient(
        address="localhost:50000",
        client_id="python-events-wildcard-subscription-client",
    ) as client:
        token = AsyncCancellationToken()

        async def subscriber() -> None:
            async for event in client.subscribe_to_events(
                subscription=EventsSubscription(
                    channel="python-events.wildcard.*",
                    on_receive_event_callback=lambda e: None,
                    on_error_callback=lambda e: print(f"Error: {e}"),
                ),
                cancellation_token=token,
            ):
                print(f"[{event.channel}] Received: {event.body.decode('utf-8')}")

        task = asyncio.create_task(subscriber())
        await asyncio.sleep(1)

        await client.publish_event(
            EventMessage(channel="python-events.wildcard.created", body=b"Order #1001 created")
        )
        await client.publish_event(
            EventMessage(channel="python-events.wildcard.shipped", body=b"Order #1002 shipped")
        )
        await client.publish_event(
            EventMessage(channel="python-events.wildcard.delivered", body=b"Order #1003 delivered")
        )

        print("Events sent to wildcard sub-channels")
        await asyncio.sleep(3)
        token.cancel()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    asyncio.run(main())
