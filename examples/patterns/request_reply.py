"""Example: Request-reply pattern — synchronous request with response using queries."""

from __future__ import annotations

import time

from kubemq import (
    CancellationToken,
    CQClient,
    QueriesSubscription,
    QueryMessage,
    QueryMessageReceived,
    QueryResponseMessage,
)


def main() -> None:
    with CQClient(
        address="localhost:50000",
        client_id="python-patterns-request-reply-client",
    ) as client:
        cancel = CancellationToken()

        def on_query(request: QueryMessageReceived) -> None:
            """Process the request and return a reply."""
            query_body = request.body.decode("utf-8")
            print(f"[Server] Received request: {query_body}")

            # Simulate processing
            reply_data = f"Processed: {query_body}"

            client.send_response_message(
                QueryResponseMessage(
                    query_received=request,
                    is_executed=True,
                    body=reply_data.encode(),
                )
            )

        def on_error(err: str) -> None:
            print(f"Error: {err}")

        # Set up the "server" side (responder)
        client.subscribe_to_queries(
            subscription=QueriesSubscription(
                channel="python-patterns.request-reply",
                on_receive_query_callback=on_query,
                on_error_callback=on_error,
            ),
            cancel=cancel,
        )
        time.sleep(1)

        # "Client" side — send requests and get replies
        for i in range(3):
            response = client.send_query_request(
                QueryMessage(
                    channel="python-patterns.request-reply",
                    body=f"Request #{i + 1}".encode(),
                    timeout_in_seconds=10,
                )
            )
            print(f"[Client] Reply: {response.body}")

        cancel.cancel()


if __name__ == "__main__":
    main()
