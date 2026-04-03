"""Example: Cached query — use server-side caching to avoid redundant processing."""

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
        client_id="python-queries-cached-query-client",
    ) as client:
        token = AsyncCancellationToken()

        async def query_handler() -> None:
            async for query in client.subscribe_to_queries(
                subscription=QueriesSubscription(
                    channel="python-queries.cached-query",
                    on_receive_query_callback=lambda q: None,
                    on_error_callback=lambda e: print(f"Error: {e}"),
                ),
                cancellation_token=token,
            ):
                print(f"Responder received query: {query.body.decode('utf-8')}")
                await client.send_response(
                    QueryResponse(
                        query_received=query,
                        is_executed=True,
                        body=b"cached response data",
                    )
                )

        handler_task = asyncio.create_task(query_handler())
        await asyncio.sleep(1)

        response1 = await client.send_query(
            QueryMessage(
                channel="python-queries.cached-query",
                body=b"fetch data",
                timeout_in_seconds=10,
                cache_key="my-cache-key",
                cache_ttl_in_seconds=30,
            )
        )
        print(f"First query  — cache_hit: {response1.cache_hit}, body: {response1.body}")

        response2 = await client.send_query(
            QueryMessage(
                channel="python-queries.cached-query",
                body=b"fetch data",
                timeout_in_seconds=10,
                cache_key="my-cache-key",
                cache_ttl_in_seconds=30,
            )
        )
        print(f"Second query — cache_hit: {response2.cache_hit}, body: {response2.body}")

        token.cancel()
        handler_task.cancel()
        try:
            await handler_task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    asyncio.run(main())
