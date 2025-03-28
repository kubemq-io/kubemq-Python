import time
from kubemq.cq import *


def main():
    # try:
    client = Client(address="localhost:50000")

    def on_receive_query(request: QueryMessageReceived):
        try:
            print(f"Id:{request.id}, Body:{request.body.decode('utf-8')}")
            response = QueryResponseMessage(
                query_received=request,
                is_executed=True,
                body=b"hello kubemq, I'm replying to you!",
            )
            client.send_response_message(response)
        except Exception as e:
            print(e)

    def on_error_handler(err: str):
        print(f"Error: {err}")

    client.subscribe_to_queries(
        subscription=QueriesSubscription(
            channel="q1",
            group="",
            on_receive_query_callback=on_receive_query,
            on_error_callback=on_error_handler,
        ),
        cancel=CancellationToken(),
    )
    time.sleep(1)
    response: QueryResponseMessage = client.send_query_request(
        QueryMessage(
            channel="q1",
            body=b"hello kubemq, please reply to me!",
            timeout_in_seconds=10,
        )
    )
    print(
        f"Request Execution: {response.is_executed}, Body: {response.body} Executed at: {response.timestamp}, Error: {response.error}"
    )


# except Exception as e:
#         print(e)
#         return


if __name__ == "__main__":
    main()
