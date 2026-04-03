"""Example: Handle query — subscribe and respond to incoming queries with data."""

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
        client_id="python-queries-handle-query-client",
    ) as client:
        token = AsyncCancellationToken()

        async def query_handler() -> None:
            async for query in client.subscribe_to_queries(
                subscription=QueriesSubscription(
                    channel="python-queries.handle-query",
                    on_receive_query_callback=lambda q: None,
                    on_error_callback=lambda e: print(f"Subscription error: {e}"),
                ),
                cancellation_token=token,
            ):
                body = query.body.decode("utf-8")
                print(f"Handling query: Id={query.id}, Body={body}")
                result_data = f"Result for: {body}"
                await client.send_response(
                    QueryResponse(
                        query_received=query,
                        is_executed=True,
                        body=result_data.encode(),
                    )
                )
                print(f"  Response sent with data: {result_data}")

        handler_task = asyncio.create_task(query_handler())
        print("Listening for queries on 'python-queries.handle-query'...")
        await asyncio.sleep(1)

        response = await client.send_query(
            QueryMessage(
                channel="python-queries.handle-query",
                body=b"fetch user profile",
                timeout_in_seconds=10,
            )
        )
        print(f"Query result: executed={response.is_executed}, body={response.body}")

        await asyncio.sleep(1)
        token.cancel()
        handler_task.cancel()
        try:
            await handler_task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    asyncio.run(main())
