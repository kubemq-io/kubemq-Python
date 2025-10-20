import asyncio
from kubemq.cq import *


async def example_commands():
    """Async commands example with subscription and request/response."""
    try:
        async with Client(address="localhost:50000", client_id="async_commands_example") as client:
            # Define async callback (you can also use sync callbacks for backward compatibility)
            async def on_receive_command(command: CommandMessageReceived):
                print(f"[Command Received] Id:{command.id}, Body:{command.body.decode('utf-8')}")

                # Async processing
                await asyncio.sleep(0.01)

                # Send response asynchronously
                response = CommandResponseMessage(
                    command_received=command,
                    is_executed=True,
                )
                await client.send_response_message_async(response)
                print(f"[Command Response] Sent for Id:{command.id}")

            async def on_error(error: str):
                print(f"[Error] {error}")

            # Subscribe to commands
            subscription = CommandsSubscription(
                channel="async_commands",
                on_receive_command_callback=on_receive_command,
                on_error_callback=on_error,
            )
            task = client.subscribe_to_commands_async(subscription)

            # Wait for subscription to be ready
            await asyncio.sleep(0.5)

            # Send command requests asynchronously
            for i in range(3):
                response = await client.send_command_request_async(
                    CommandMessage(
                        channel="async_commands",
                        body=f"Async command {i}".encode(),
                        timeout_in_seconds=10,
                    )
                )
                print(f"[Command Request] Response - Executed:{response.is_executed}, Timestamp:{response.timestamp}")
                await asyncio.sleep(0.3)

            await asyncio.sleep(1)

    except Exception as e:
        print(f"Error: {e}")


async def example_queries():
    """Async queries example with subscription and request/response."""
    try:
        async with Client(address="localhost:50000", client_id="async_queries_example") as client:
            async def on_receive_query(query: QueryMessageReceived):
                print(f"[Query Received] Id:{query.id}, Body:{query.body.decode('utf-8')}")

                # Async processing
                await asyncio.sleep(0.01)

                # Send response with data
                response = QueryResponseMessage(
                    query_received=query,
                    is_executed=True,
                    body=f"Response to query {query.id}".encode(),
                )
                await client.send_response_message_async(response)
                print(f"[Query Response] Sent for Id:{query.id}")

            async def on_error(error: str):
                print(f"[Error] {error}")

            # Subscribe to queries
            subscription = QueriesSubscription(
                channel="async_queries",
                on_receive_query_callback=on_receive_query,
                on_error_callback=on_error,
            )
            task = client.subscribe_to_queries_async(subscription)

            await asyncio.sleep(0.5)

            # Send query requests asynchronously
            for i in range(3):
                response = await client.send_query_request_async(
                    QueryMessage(
                        channel="async_queries",
                        body=f"Async query {i}".encode(),
                        timeout_in_seconds=10,
                    )
                )
                body_text = response.body.decode('utf-8') if response.body else "No response"
                print(f"[Query Request] Response - Executed:{response.is_executed}, Body:{body_text}")
                await asyncio.sleep(0.3)

            await asyncio.sleep(1)

    except Exception as e:
        print(f"Error: {e}")


async def example_concurrent_operations():
    """Demonstrate concurrent async operations using asyncio.gather()."""
    try:
        async with Client(address="localhost:50000", client_id="concurrent_cq") as client:
            # Ping server and list channels concurrently
            server_info, cmd_channels, query_channels = await asyncio.gather(
                client.ping_async(),
                client.list_commands_channels_async(""),
                client.list_queries_channels_async("")
            )
            print(f"Server version: {server_info.version}")
            print(f"Commands channels: {len(cmd_channels)}, Queries channels: {len(query_channels)}")

            # Send multiple command requests concurrently
            # Note: This requires subscribers to be running on those channels
            channels = ["cmd_1", "cmd_2"]

            async def send_command(channel: str, msg: str):
                try:
                    return await client.send_command_request_async(
                        CommandMessage(
                            channel=channel,
                            body=msg.encode(),
                            timeout_in_seconds=2,
                        )
                    )
                except Exception as e:
                    print(f"Command to {channel} failed: {e}")
                    return None

            # Send to multiple channels in parallel
            results = await asyncio.gather(*[
                send_command(ch, f"Concurrent command to {ch}")
                for ch in channels
            ])
            successful = sum(1 for r in results if r and r.is_executed)
            print(f"Sent {len(channels)} concurrent commands, {successful} successful")

    except Exception as e:
        print(f"Error: {e}")


async def main():
    """Run all async examples."""
    print("=== Async CQ Examples ===\n")

    print("1. Commands Example:")
    await example_commands()

    print("\n2. Queries Example:")
    await example_queries()

    print("\n3. Concurrent Operations Example:")
    await example_concurrent_operations()

    print("\n=== Done ===")


if __name__ == "__main__":
    asyncio.run(main())
