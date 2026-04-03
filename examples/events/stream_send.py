"""Example: Stream send — send many events efficiently via the streaming path."""

from __future__ import annotations

import asyncio

from kubemq import AsyncCancellationToken, AsyncPubSubClient, EventMessage, EventsSubscription


async def main() -> None:
    async with AsyncPubSubClient(
        address="localhost:50000",
        client_id="python-events-stream-send-client",
    ) as client:
        token = AsyncCancellationToken()
        received: list[str] = []

        async def subscriber() -> None:
            async for event in client.subscribe_to_events(
                subscription=EventsSubscription(
                    channel="python-events.stream-send",
                    on_receive_event_callback=lambda e: None,
                    on_error_callback=lambda e: print(f"Error: {e}"),
                ),
                cancellation_token=token,
            ):
                received.append(event.body.decode("utf-8"))
                print(f"Received: {event.body.decode('utf-8')}")

        task = asyncio.create_task(subscriber())
        await asyncio.sleep(1)

        for i in range(100):
            await client.publish_event(
                EventMessage(
                    channel="python-events.stream-send",
                    body=f"Event-{i + 1}".encode(),
                )
            )

        print("Sent 100 events via stream")
        await asyncio.sleep(3)
        print(f"Received {len(received)} events")
        token.cancel()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    asyncio.run(main())
