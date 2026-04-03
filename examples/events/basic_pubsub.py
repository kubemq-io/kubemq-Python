"""Example: Basic pub/sub — publish and subscribe to events."""

from __future__ import annotations

import asyncio

from kubemq import (
    AsyncCancellationToken,
    AsyncPubSubClient,
    EventMessage,
    EventsSubscription,
    KubeMQConnectionError,
    KubeMQError,
)


async def main() -> None:
    try:
        async with AsyncPubSubClient(
            address="localhost:50000",
            client_id="python-events-basic-pubsub-client",
        ) as client:
            token = AsyncCancellationToken()

            async def subscriber() -> None:
                async for event in client.subscribe_to_events(
                    subscription=EventsSubscription(
                        channel="python-events.basic-pubsub",
                        on_receive_event_callback=lambda e: None,
                        on_error_callback=lambda e: print(f"Error: {e}"),
                    ),
                    cancellation_token=token,
                ):
                    print(
                        f"Received — Id:{event.id}, Channel:{event.channel}, "
                        f"Body:{event.body.decode('utf-8')}"
                    )

            task = asyncio.create_task(subscriber())
            await asyncio.sleep(1)

            await client.publish_event(
                EventMessage(
                    channel="python-events.basic-pubsub",
                    body=b"hello kubemq",
                )
            )
            print("Event sent")

            await asyncio.sleep(2)
            token.cancel()
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
    except KubeMQConnectionError as e:
        print(f"Connection error: {e}")
    except KubeMQError as e:
        print(f"KubeMQ error: {e}")


if __name__ == "__main__":
    asyncio.run(main())

# Expected output:
# Received — Id:<message-id>, Channel:python-events.basic-pubsub, Body:hello kubemq
# Event sent
