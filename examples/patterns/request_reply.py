"""Example: Request-reply pattern — synchronous request with response using queries."""

from __future__ import annotations

import asyncio

from kubemq import (
    AsyncCancellationToken,
    AsyncCQClient,
    QueriesSubscription,
    QueryMessage,
    QueryResponse,
)


async def main() -> None:
    async with AsyncCQClient(
        address="localhost:50000",
        client_id="python-patterns-request-reply-client",
    ) as client:
        token = AsyncCancellationToken()

        async def server() -> None:
            async for query in client.subscribe_to_queries(
                subscription=QueriesSubscription(
                    channel="python-patterns.request-reply",
                    on_receive_query_callback=lambda q: None,
                    on_error_callback=lambda e: print(f"Error: {e}"),
                ),
                cancellation_token=token,
            ):
                query_body = query.body.decode("utf-8")
                print(f"[Server] Received request: {query_body}")
                reply_data = f"Processed: {query_body}"
                await client.send_response(
                    QueryResponse(
                        query_received=query,
                        is_executed=True,
                        body=reply_data.encode(),
                    )
                )

        server_task = asyncio.create_task(server())
        await asyncio.sleep(1)

        for i in range(3):
            response = await client.send_query(
                QueryMessage(
                    channel="python-patterns.request-reply",
                    body=f"Request #{i + 1}".encode(),
                    timeout_in_seconds=10,
                )
            )
            print(f"[Client] Reply: {response.body}")

        token.cancel()
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    asyncio.run(main())
