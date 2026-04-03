"""Example: Consumer group — load-balance queries across multiple responders."""

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
        client_id="python-queries-consumer-group-client",
    ) as client:
        token = AsyncCancellationToken()

        async def make_responder(name: str) -> None:
            async for query in client.subscribe_to_queries(
                subscription=QueriesSubscription(
                    channel="python-queries.consumer-group",
                    group="responders",
                    on_receive_query_callback=lambda q: None,
                    on_error_callback=lambda e: print(f"Error: {e}"),
                ),
                cancellation_token=token,
            ):
                print(f"[{name}] Received query: {query.body.decode('utf-8')}")
                await client.send_response(
                    QueryResponse(
                        query_received=query,
                        is_executed=True,
                        body=f"Response from {name}".encode(),
                    )
                )

        task1 = asyncio.create_task(make_responder("Responder-1"))
        task2 = asyncio.create_task(make_responder("Responder-2"))
        await asyncio.sleep(1)

        for i in range(4):
            response = await client.send_query(
                QueryMessage(
                    channel="python-queries.consumer-group",
                    body=f"Query #{i + 1}".encode(),
                    timeout_in_seconds=10,
                )
            )
            print(f"Query #{i + 1} response: {response.body}")

        token.cancel()
        for t in [task1, task2]:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass


if __name__ == "__main__":
    asyncio.run(main())
