"""Example: Fan-out pattern — broadcast a message to multiple subscribers."""

from __future__ import annotations

import asyncio

from kubemq import AsyncCancellationToken, AsyncPubSubClient, EventMessage, EventsSubscription


async def main() -> None:
    async with AsyncPubSubClient(
        address="localhost:50000",
        client_id="python-patterns-fan-out-client",
    ) as client:
        token = AsyncCancellationToken()
        received_counts: dict[str, int] = {"Service-A": 0, "Service-B": 0, "Service-C": 0}

        async def make_subscriber(name: str) -> None:
            async for event in client.subscribe_to_events(
                subscription=EventsSubscription(
                    channel="python-patterns.fan-out",
                    on_receive_event_callback=lambda e: None,
                    on_error_callback=lambda e: print(f"Error: {e}"),
                ),
                cancellation_token=token,
            ):
                received_counts[name] += 1
                print(f"  [{name}] Received: {event.body.decode('utf-8')}")

        tasks = [
            asyncio.create_task(make_subscriber("Service-A")),
            asyncio.create_task(make_subscriber("Service-B")),
            asyncio.create_task(make_subscriber("Service-C")),
        ]
        await asyncio.sleep(1)

        print("Publishing message to fan-out channel...")
        await client.publish_event(
            EventMessage(
                channel="python-patterns.fan-out",
                body=b"Order #1001 placed",
            )
        )

        await asyncio.sleep(3)
        print(
            f"\nEach subscriber received: A={received_counts['Service-A']}, "
            f"B={received_counts['Service-B']}, C={received_counts['Service-C']}"
        )
        token.cancel()
        for t in tasks:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass


if __name__ == "__main__":
    asyncio.run(main())
