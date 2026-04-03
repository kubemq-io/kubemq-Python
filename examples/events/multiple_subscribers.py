"""Example: Multiple subscribers — demonstrates broadcast and group load balancing."""

from __future__ import annotations

import asyncio

from kubemq import AsyncCancellationToken, AsyncPubSubClient, EventMessage, EventsSubscription


async def main() -> None:
    async with AsyncPubSubClient(
        address="localhost:50000",
        client_id="python-events-multiple-subscribers-client",
    ) as client:
        token = AsyncCancellationToken()
        tasks: list[asyncio.Task[None]] = []

        async def make_subscriber(channel: str, name: str, group: str = "") -> None:
            async for event in client.subscribe_to_events(
                subscription=EventsSubscription(
                    channel=channel,
                    group=group,
                    on_receive_event_callback=lambda e: None,
                    on_error_callback=lambda e: print(f"Error: {e}"),
                ),
                cancellation_token=token,
            ):
                print(f"[{name}] Received: {event.body.decode('utf-8')}")

        tasks.append(asyncio.create_task(
            make_subscriber("python-events.multiple-subscribers", "Subscriber-A")
        ))
        tasks.append(asyncio.create_task(
            make_subscriber("python-events.multiple-subscribers", "Subscriber-B")
        ))
        tasks.append(asyncio.create_task(
            make_subscriber("python-events.multiple-subscribers-tasks", "Worker-1", "workers")
        ))
        tasks.append(asyncio.create_task(
            make_subscriber("python-events.multiple-subscribers-tasks", "Worker-2", "workers")
        ))

        await asyncio.sleep(1)

        await client.publish_event(
            EventMessage(
                channel="python-events.multiple-subscribers",
                body=b"System update available",
            )
        )

        for i in range(4):
            await client.publish_event(
                EventMessage(
                    channel="python-events.multiple-subscribers-tasks",
                    body=f"Task #{i + 1}".encode(),
                )
            )

        await asyncio.sleep(3)
        token.cancel()
        for t in tasks:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass


if __name__ == "__main__":
    asyncio.run(main())
