"""Example: Send query — send a query and receive a response with data."""

from __future__ import annotations

import time

from kubemq import (
    CancellationToken,
    CQClient,
    KubeMQConnectionError,
    KubeMQError,
    KubeMQTimeoutError,
    QueriesSubscription,
    QueryMessage,
    QueryMessageReceived,
    QueryResponseMessage,
)


def main() -> None:
    try:
        with CQClient(
            address="localhost:50000",
            client_id="python-queries-send-query-client",
        ) as client:
            cancel = CancellationToken()

            def on_receive_query(request: QueryMessageReceived) -> None:
                print(f"Responder received: {request.body.decode('utf-8')}")
                client.send_response_message(
                    QueryResponseMessage(
                        query_received=request,
                        is_executed=True,
                        body=b"response data payload",
                    )
                )

            def on_error(err: str) -> None:
                print(f"Error: {err}")

            # Set up a query responder
            client.subscribe_to_queries(
                subscription=QueriesSubscription(
                    channel="python-queries.send-query",
                    on_receive_query_callback=on_receive_query,
                    on_error_callback=on_error,
                ),
                cancel=cancel,
            )
            time.sleep(1)

            # Send a query and get the response
            response = client.send_query_request(
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

            cancel.cancel()
    except KubeMQConnectionError as e:
        print(f"Connection error: {e}")
    except KubeMQTimeoutError as e:
        print(f"Timeout error: {e}")
    except KubeMQError as e:
        print(f"KubeMQ error: {e}")


if __name__ == "__main__":
    main()

# Expected output:
# Responder received: hello kubemq, please reply!
# Response: executed=True, body=b'response data payload', timestamp=<timestamp>
