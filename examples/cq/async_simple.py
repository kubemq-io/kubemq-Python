import asyncio
from kubemq.cq import *


async def simple_cq_example():
    """Simple async CQ example."""
    print("=== Simple Async CQ Example ===\n")

    client = Client(address="localhost:50000", client_id="async_cq_simple")

    try:
        # Test ping
        print("1. Testing async ping...")
        result = await client.ping_async()
        print(f"   Server version: {result.version}, Host: {result.host}\n")

        # List channels concurrently
        print("2. Listing channels concurrently...")
        cmd_channels, query_channels = await asyncio.gather(
            client.list_commands_channels_async(""),
            client.list_queries_channels_async("")
        )
        print(f"   Found {len(cmd_channels)} command channels")
        print(f"   Found {len(query_channels)} query channels\n")

        # Create channels
        print("3. Creating channels...")
        await client.create_commands_channel_async("async_test_cmd")
        await client.create_queries_channel_async("async_test_query")
        print("   Channels created!\n")

        # Note: Actual send/receive would require subscribers
        print("=== All tests passed! ===")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Manual cleanup
        try:
            client.shutdown_event.set()
        except:
            pass


if __name__ == "__main__":
    asyncio.run(simple_cq_example())
