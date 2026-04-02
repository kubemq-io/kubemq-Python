"""Example: Handle query — subscribe and respond to incoming queries with data."""

from __future__ import annotations

import time

from kubemq import (
    CancellationToken,
    Client,
    QueriesSubscription,
    QueryMessage,
    QueryReceived,
    QueryResponse,
)


def main() -> None:
    with Client(
        address="localhost:50000",
        client_id="python-queries-handle-query-client",
    ) as client:
        cancel = CancellationToken()

        def on_receive_query(request: QueryReceived) -> None:
            """Handle incoming query and send response with data."""
            try:
                body = request.body.decode("utf-8")
                print(f"Handling query: Id={request.id}, Body={body}")

                # Process the query (simulate data lookup)
                result_data = f"Result for: {body}"

                # Send response back with data
                client.send_response_message(
                    QueryResponse(
                        query_received=request,
                        is_executed=True,
                        body=result_data.encode(),
                    )
                )
                print(f"  Response sent with data: {result_data}")
            except Exception as e:
                print(f"  Error handling query: {e}")

        def on_error(err: str) -> None:
            print(f"Subscription error: {err}")

        # Subscribe to queries
        client.subscribe_to_queries(
            subscription=QueriesSubscription(
                channel="python-queries.handle-query",
                on_receive_query_callback=on_receive_query,
                on_error_callback=on_error,
            ),
            cancel=cancel,
        )
        print("Listening for queries on 'python-queries.handle-query'...")
        time.sleep(1)

        # Send a test query
        response = client.send_query(
            QueryMessage(
                channel="python-queries.handle-query",
                body=b"fetch user profile",
                timeout_in_seconds=10,
            )
        )
        print(f"Query result: executed={response.is_executed}, body={response.body}")

        time.sleep(1)
        cancel.cancel()


if __name__ == "__main__":
    main()
