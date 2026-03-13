import time
from kubemq.cq import *


def main():
    client = Client(address="localhost:50000")
    cancel = CancellationToken()

    def on_receive_query(request: QueryMessageReceived):
        print(f"Responder received query: {request.body.decode('utf-8')}")
        client.send_response_message(
            QueryResponseMessage(
                query_received=request,
                is_executed=True,
                body=b"cached response data",
            )
        )

    def on_error(err: str):
        print(f"Error: {err}")

    client.subscribe_to_queries(
        subscription=QueriesSubscription(
            channel="cached-query",
            on_receive_query_callback=on_receive_query,
            on_error_callback=on_error,
        ),
        cancel=cancel,
    )
    time.sleep(1)

    response1 = client.send_query_request(
        QueryMessage(
            channel="cached-query",
            body=b"fetch data",
            timeout_in_seconds=10,
            cache_key="my-cache-key",
            cache_ttl_int_seconds=30,
        )
    )
    print(f"First query  - cache_hit: {response1.cache_hit}, body: {response1.body}")

    response2 = client.send_query_request(
        QueryMessage(
            channel="cached-query",
            body=b"fetch data",
            timeout_in_seconds=10,
            cache_key="my-cache-key",
            cache_ttl_int_seconds=30,
        )
    )
    print(f"Second query - cache_hit: {response2.cache_hit}, body: {response2.body}")

    cancel.cancel()
    client.close()


if __name__ == "__main__":
    main()
