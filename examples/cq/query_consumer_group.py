import time
from kubemq.cq import *


def make_handler(name: str, client):
    def handler(request: QueryMessageReceived):
        print(f"[{name}] Received query: {request.body.decode('utf-8')}")
        client.send_response_message(
            QueryResponseMessage(
                query_received=request,
                is_executed=True,
                body=f"Response from {name}".encode(),
            )
        )
    return handler


def on_error(err: str):
    print(f"Error: {err}")


def main():
    client = Client(address="localhost:50000")
    cancel = CancellationToken()

    client.subscribe_to_queries(
        subscription=QueriesSubscription(
            channel="query-group",
            group="responders",
            on_receive_query_callback=make_handler("Responder-1", client),
            on_error_callback=on_error,
        ),
        cancel=cancel,
    )
    client.subscribe_to_queries(
        subscription=QueriesSubscription(
            channel="query-group",
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
                channel="query-group",
                body=f"Query #{i + 1}".encode(),
                timeout_in_seconds=10,
            )
        )
        print(f"Query #{i + 1} response: {response.body}")

    cancel.cancel()
    client.close()


if __name__ == "__main__":
    main()
