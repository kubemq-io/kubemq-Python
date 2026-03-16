"""Example: Consumer group — load-balance queries across multiple responders."""

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


def make_handler(name: str, client: CQClient):  # type: ignore[no-untyped-def]
    def handler(request: QueryMessageReceived) -> None:
        print(f"[{name}] Received query: {request.body.decode('utf-8')}")
        client.send_response_message(
            QueryResponseMessage(
                query_received=request,
                is_executed=True,
                body=f"Response from {name}".encode(),
            )
        )

    return handler


def on_error(err: str) -> None:
    print(f"Error: {err}")


def main() -> None:
    with CQClient(
        address="localhost:50000",
        client_id="python-queries-consumer-group-client",
    ) as client:
        cancel = CancellationToken()

        client.subscribe_to_queries(
            subscription=QueriesSubscription(
                channel="python-queries.consumer-group",
                group="responders",
                on_receive_query_callback=make_handler("Responder-1", client),
                on_error_callback=on_error,
            ),
            cancel=cancel,
        )
        client.subscribe_to_queries(
            subscription=QueriesSubscription(
                channel="python-queries.consumer-group",
                group="responders",
                on_receive_query_callback=make_handler("Responder-2", client),
                on_error_callback=on_error,
            ),
            cancel=cancel,
        )

        time.sleep(1)
        for i in range(4):
            response = client.send_query_request(
                QueryMessage(
                    channel="python-queries.consumer-group",
                    body=f"Query #{i + 1}".encode(),
                    timeout_in_seconds=10,
                )
            )
            print(f"Query #{i + 1} response: {response.body}")

        cancel.cancel()


if __name__ == "__main__":
    main()
