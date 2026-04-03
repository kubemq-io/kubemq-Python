"""Example: Persistent pub/sub — publish and subscribe to events store with persistence."""

from __future__ import annotations

import asyncio

from kubemq import (
    AsyncCancellationToken,
    AsyncPubSubClient,
    EventStoreMessage,
    EventsStoreSubscription,
    KubeMQConnectionError,
    KubeMQError,
)
from kubemq.pubsub import EventStoreStartPosition


async def main() -> None:
    try:
        async with AsyncPubSubClient(
            address="localhost:50000",
            client_id="python-events-store-persistent-pubsub-client",
        ) as client:
            token = AsyncCancellationToken()

            async def subscriber() -> None:
                async for event in client.subscribe_to_events_store(
                    subscription=EventsStoreSubscription(
                        channel="python-events-store.persistent-pubsub",
                        on_receive_event_callback=lambda e: None,
                        on_error_callback=lambda e: print(f"Error: {e}"),
                        events_store_type=EventStoreStartPosition.StartFromNew,
                    ),
                    cancellation_token=token,
                ):
                    print(
                        f"Received — Id:{event.id}, Seq:{event.sequence}, "
                        f"Body:{event.body.decode('utf-8')}"
                    )

            task = asyncio.create_task(subscriber())
            await asyncio.sleep(1)

            result = await client.send_event_store(
                EventStoreMessage(
                    channel="python-events-store.persistent-pubsub",
                    body=b"hello kubemq",
                )
            )
            print(f"Send result: {result}")

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
# Received — Id:<message-id>, Seq:<sequence>, Body:hello kubemq
# Send result: <result>
