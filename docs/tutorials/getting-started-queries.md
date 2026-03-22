# Getting Started with Queries

This tutorial walks you through building a request-reply data retrieval system with KubeMQ queries in Python. By the end, you'll have a query handler that responds to incoming requests with data — and a client that sends queries and receives responses.

## What You'll Build

A simple request-reply system where a client sends a query (e.g., "fetch user profile") and a handler processes it and returns a response with data. Queries in KubeMQ are **synchronous request-reply**: the sender blocks until a response arrives or times out. This makes them ideal for data lookups, read operations, and any scenario where you need a direct answer to a question.

## Prerequisites

- **Python 3.9 or later** ([download](https://www.python.org/downloads/))
- **KubeMQ server** running on `localhost:50000`. The quickest way:
  ```bash
  docker run -d -p 50000:50000 -p 9090:9090 kubemq/kubemq
  ```
- A virtual environment for your project:
  ```bash
  mkdir kubemq-queries-tutorial && cd kubemq-queries-tutorial
  python -m venv .venv
  source .venv/bin/activate  # On Windows: .venv\Scripts\activate
  ```

## Step 1: Install the SDK

Install the KubeMQ Python SDK:

```bash
pip install kubemq
```

## Step 2: Create a Query Handler

Create a file called `queries_tutorial.py`. The handler subscribes to a channel and responds to incoming queries using `CQClient` (Commands & Queries client).

```python
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
        client_id="queries-tutorial-client",
    ) as client:

        def on_receive_query(request: QueryMessageReceived) -> None:
            body = request.body.decode("utf-8")
            print(f"[Handler] Received query: Id={request.id}, Body={body}")

            # Process the query (e.g., lookup data)
            result_data = f"Result for: {body}"

            client.send_response_message(
                QueryResponseMessage(
                    query_received=request,
                    is_executed=True,
                    body=result_data.encode(),
                )
            )
            print(f"[Handler] Response sent: {result_data}")

        def on_error(err: str) -> None:
            print(f"[Handler] Error: {err}")

        cancel = CancellationToken()

        client.subscribe_to_queries(
            subscription=QueriesSubscription(
                channel="tutorial.queries",
                on_receive_query_callback=on_receive_query,
                on_error_callback=on_error,
            ),
            cancel=cancel,
        )
        print("[Handler] Listening for queries on channel: tutorial.queries")

        # ... query sender code will go here (Step 3) ...


if __name__ == "__main__":
    main()
```

Key points:

- **`CQClient`** is used for both commands and queries. Use it as a context manager for proper connection cleanup.
- **`QueriesSubscription`** defines the channel and an `on_receive_query_callback`. When a query arrives, you process it and call `send_response_message` with a `QueryResponseMessage`.
- **`QueryResponseMessage`** requires `query_received` (the incoming request), `is_executed` (True for success), and `body` (the response payload as bytes).

## Step 3: Send a Query and Get the Response

Add the code to send a query and receive the response. The handler must be listening before you send:

```python
        time.sleep(1)

        response = client.send_query_request(
            QueryMessage(
                channel="tutorial.queries",
                body=b"fetch user profile",
                timeout_in_seconds=10,
            )
        )
        print(
            f"[Client] Response: executed={response.is_executed}, "
            f"body={response.body.decode('utf-8')}"
        )

        time.sleep(1)
        cancel.cancel()
        print("\nDone!")
```

- **`QueryMessage`** takes a channel, body (as `bytes`), and `timeout_in_seconds`. The sender blocks until a response arrives or the timeout expires.
- **`send_query_request`** returns a `QueryResponseMessage` with `is_executed`, `body`, and `timestamp`.

## Complete Program

Here's the full program assembled:

```python
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
        client_id="queries-tutorial-client",
    ) as client:

        def on_receive_query(request: QueryMessageReceived) -> None:
            body = request.body.decode("utf-8")
            print(f"[Handler] Received query: Id={request.id}, Body={body}")

            result_data = f"Result for: {body}"

            client.send_response_message(
                QueryResponseMessage(
                    query_received=request,
                    is_executed=True,
                    body=result_data.encode(),
                )
            )
            print(f"[Handler] Response sent: {result_data}")

        def on_error(err: str) -> None:
            print(f"[Handler] Error: {err}")

        cancel = CancellationToken()

        client.subscribe_to_queries(
            subscription=QueriesSubscription(
                channel="tutorial.queries",
                on_receive_query_callback=on_receive_query,
                on_error_callback=on_error,
            ),
            cancel=cancel,
        )
        print("[Handler] Listening for queries on channel: tutorial.queries")

        time.sleep(1)

        response = client.send_query_request(
            QueryMessage(
                channel="tutorial.queries",
                body=b"fetch user profile",
                timeout_in_seconds=10,
            )
        )
        print(
            f"[Client] Response: executed={response.is_executed}, "
            f"body={response.body.decode('utf-8')}"
        )

        time.sleep(1)
        cancel.cancel()
        print("\nDone!")


if __name__ == "__main__":
    main()
```

Run it:

```bash
python queries_tutorial.py
```

### Expected Output

```
[Handler] Listening for queries on channel: tutorial.queries
[Handler] Received query: Id=<id>, Body=fetch user profile
[Handler] Response sent: Result for: fetch user profile
[Client] Response: executed=True, body=Result for: fetch user profile

Done!
```

## Key Concepts

| Concept | What It Means |
|---|---|
| **Request-reply** | The sender blocks until a response arrives. Use `send_query_request` — it returns the response or raises `KubeMQTimeoutError` on timeout. |
| **CQClient** | Commands & Queries client. Use `subscribe_to_queries` for handlers and `send_query_request` for senders. |
| **QueryResponseMessage** | Must include `query_received` (the request), `is_executed` (True/False), and `body`. The handler links the response to the request via `query_received`. |
| **Timeout** | Set `timeout_in_seconds` on `QueryMessage`. If no response arrives in time, `send_query_request` raises `KubeMQTimeoutError`. |

## Next Steps

- **Commands**: For request-reply with side effects (e.g., create, update, delete), see [Request-Reply with Commands](request-reply-with-commands.md). Commands are similar to queries but imply an action.
- **Error handling**: Catch `KubeMQConnectionError`, `KubeMQTimeoutError`, and `KubeMQError`. See [Handle Errors and Retries](../how-to/handle-errors-and-retries.md).
- **Separate handler process**: In production, run the query handler in a separate process or service. See [`examples/queries/handle_query.py`](../../examples/queries/handle_query.py).
- **API Reference**: Full CQ API details in [API Reference: CQ (Commands & Queries)](../api/cq.md).
