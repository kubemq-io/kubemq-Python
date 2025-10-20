import asyncio
from kubemq.pubsub import *


async def example_events():
    """Async events example with subscription and sending."""
    try:
        async with Client(address="localhost:50000", client_id="async_events_example") as client:
            # Define async callback (you can also use sync callbacks for backward compatibility)
            async def on_receive_event(event: EventMessageReceived):
                print(f"[Event] Id:{event.id}, From:{event.from_client_id}, Body:{event.body.decode('utf-8')}")
                # Async callbacks can use await for async operations
                await asyncio.sleep(0.01)  # Simulate async processing

            async def on_error(error: str):
                print(f"[Error] {error}")

            # Subscribe to events
            subscription = EventsSubscription(
                channel="async_events",
                on_receive_event_callback=on_receive_event,
                on_error_callback=on_error,
            )
            task = client.subscribe_to_events_async(subscription)

            # Wait for subscription to be ready
            await asyncio.sleep(0.5)

            # Send events asynchronously
            for i in range(5):
                await client.send_events_message_async(
                    EventMessage(channel="async_events", body=f"Async event {i}".encode())
                )
                await asyncio.sleep(0.2)

            # Wait for events to be processed
            await asyncio.sleep(1)

    except Exception as e:
        print(f"Error: {e}")


async def example_events_store():
    """Async events store example with subscription and sending."""
    try:
        async with Client(address="localhost:50000", client_id="async_events_store_example") as client:
            async def on_receive_event(event: EventStoreMessageReceived):
                print(f"[EventStore] Id:{event.id}, Sequence:{event.sequence}, Body:{event.body.decode('utf-8')}")

            async def on_error(error: str):
                print(f"[Error] {error}")

            # Subscribe to events store
            subscription = EventsStoreSubscription(
                channel="async_events_store",
                on_receive_event_callback=on_receive_event,
                on_error_callback=on_error,
                events_store_type=EventsStoreType.StartNewOnly,
            )
            task = client.subscribe_to_events_store_async(subscription)

            await asyncio.sleep(0.5)

            # Send events store messages and get results
            for i in range(3):
                result = await client.send_events_store_message_async(
                    EventStoreMessage(channel="async_events_store", body=f"Stored event {i}".encode())
                )
                print(f"[Sent] Id:{result.id}, Sent:{result.sent}")
                await asyncio.sleep(0.2)

            await asyncio.sleep(1)

    except Exception as e:
        print(f"Error: {e}")


async def example_concurrent_operations():
    """Demonstrate concurrent async operations using asyncio.gather()."""
    try:
        async with Client(address="localhost:50000", client_id="concurrent_pubsub") as client:
            # Send to multiple channels concurrently
            channels = ["channel_1", "channel_2", "channel_3"]

            # Send messages to all channels in parallel
            await asyncio.gather(*[
                client.send_events_message_async(
                    EventMessage(channel=ch, body=f"Message to {ch}".encode())
                )
                for ch in channels
            ])
            print(f"Sent messages to {len(channels)} channels concurrently")

            # Ping and list channels concurrently
            server_info, channel_list = await asyncio.gather(
                client.ping_async(),
                client.list_events_channels_async("")
            )
            print(f"Server version: {server_info.version}")
            print(f"Found {len(channel_list)} channels")

    except Exception as e:
        print(f"Error: {e}")


async def main():
    """Run all async examples."""
    print("=== Async PubSub Examples ===\n")

    print("1. Events Example:")
    await example_events()

    print("\n2. Events Store Example:")
    await example_events_store()

    print("\n3. Concurrent Operations Example:")
    await example_concurrent_operations()

    print("\n=== Done ===")


if __name__ == "__main__":
    asyncio.run(main())
