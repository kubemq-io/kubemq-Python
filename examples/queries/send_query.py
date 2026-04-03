"""Example: Send query — send a query and receive a response with data."""

from __future__ import annotations

import asyncio

from kubemq import (
    AsyncCancellationToken,
    AsyncCQClient,
    KubeMQConnectionError,
    KubeMQError,
    KubeMQTimeoutError,
    QueriesSubscription,
    QueryMessage,
    QueryResponse,
)


async def main() -> None:
    try:
        async with AsyncCQClient(
            address="localhost:50000",
            client_id="python-queries-send-query-client",
        ) as client:
            token = AsyncCancellationToken()

            async def query_handler() -> None:
                async for query in client.subscribe_to_queries(
                    subscription=QueriesSubscription(
                        channel="python-queries.send-query",
                        on_receive_query_callback=lambda q: None,
                        on_error_callback=lambda e: print(f"Error: {e}"),
                    ),
                    cancellation_token=token,
                ):
                    print(f"Responder received: {query.body.decode('utf-8')}")
                    await client.send_response(
                        QueryResponse(
                            query_received=query,
                            is_executed=True,
                            body=b"response data payload",
                        )
                    )

            handler_task = asyncio.create_task(query_handler())
            await asyncio.sleep(1)

            response = await client.send_query(
                QueryMessage(
                    channel="python-queries.send-query",
                    body=b"hello kubemq, please reply!",
                    timeout_in_seconds=10,
                )
            )
            print(
                f"Response: executed={response.is_executed}, "
                f"body={response.body}, "
                f"timestamp={response.timestamp}"
            )

            token.cancel()
            handler_task.cancel()
            try:
                await handler_task
            except asyncio.CancelledError:
                pass
    except KubeMQConnectionError as e:
        print(f"Connection error: {e}")
    except KubeMQTimeoutError as e:
        print(f"Timeout error: {e}")
    except KubeMQError as e:
        print(f"KubeMQ error: {e}")


if __name__ == "__main__":
    asyncio.run(main())

# Expected output:
# Responder received: hello kubemq, please reply!
# Response: executed=True, body=b'response data payload', timestamp=<timestamp>
