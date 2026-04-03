"""Quick start: Events (Pub/Sub) — send and receive a fire-and-forget event."""

from __future__ import annotations

import asyncio

from kubemq import AsyncCancellationToken, AsyncPubSubClient, EventMessage, EventsSubscription


async def main() -> None:
    async with AsyncPubSubClient(address="localhost:50000", client_id="python-events-quickstart-client") as client:
        token = AsyncCancellationToken()

        async def subscriber() -> None:
            async for event in client.subscribe_to_events(
                subscription=EventsSubscription(
                    channel="python-quickstart",
                    on_receive_event_callback=lambda e: None,
                    on_error_callback=lambda e: print(f"Error: {e}"),
                ),
                cancellation_token=token,
            ):
                print(f"Received: {event.body.decode('utf-8')}")

        task = asyncio.create_task(subscriber())
        await asyncio.sleep(1)

        await client.publish_event(EventMessage(channel="python-quickstart", body=b"Hello KubeMQ!"))
        print("Event sent!")

        await asyncio.sleep(2)
        token.cancel()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    asyncio.run(main())
