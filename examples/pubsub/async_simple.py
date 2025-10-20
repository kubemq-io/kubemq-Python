import asyncio
from kubemq.pubsub import *


async def simple_events_example():
    """Simple async events example."""
    print("=== Simple Async Events Example ===\n")

    client = Client(address="localhost:50000", client_id="async_simple_example")

    try:
        # Test ping
        print("1. Testing async ping...")
        result = await client.ping_async()
        print(f"   Server version: {result.version}, Host: {result.host}\n")

        # Send an event
        print("2. Sending async event...")
        await client.send_events_message_async(
            EventMessage(
                channel="test_channel",
                body=b"Hello from async!"
            )
        )
        print("   Event sent successfully!\n")

        # Send event store message
        print("3. Sending async event store message...")
        result = await client.send_events_store_message_async(
            EventStoreMessage(
                channel="test_store",
                body=b"Stored event from async!"
            )
        )
        print(f"   Event stored! ID: {result.id}, Sent: {result.sent}\n")

        # Test concurrent operations
        print("4. Testing concurrent operations...")
        channels = ["ch1", "ch2", "ch3"]
        results = await asyncio.gather(*[
            client.send_events_message_async(
                EventMessage(channel=ch, body=f"Message to {ch}".encode())
            )
            for ch in channels
        ])
        print(f"   Sent {len(results)} messages concurrently!\n")

        print("=== All tests passed! ===")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Manual cleanup to avoid event loop issues
        try:
            client.shutdown_event.set()
        except:
            pass


if __name__ == "__main__":
    asyncio.run(simple_events_example())
