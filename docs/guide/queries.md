# Queries (RPC)

Request/reply where the sender expects a data response.

## Overview

- **Delivery:** At-most-once request/reply with payload
- **Pattern:** Query — the responder returns data
- **Use cases:** Data lookups, service-to-service reads, aggregation

## Send a query

```python
from kubemq import CQClient, QueryMessage

with CQClient(address="localhost:50000") as client:
    response = client.send_query(
        QueryMessage(
            channel="data-service",
            body=b'{"user_id": "123"}',
            timeout_in_seconds=10,
        )
    )
    print(f"Executed: {response.is_executed}")
    print(f"Response: {response.body.decode('utf-8')}")
```

## Handle queries

```python
import time
from kubemq import (
    CQClient, QueriesSubscription,
    QueryResponseMessage, CancellationToken,
)

client = CQClient(address="localhost:50000")

def on_query(query):
    print(f"Query: {query.body.decode('utf-8')}")
    client.send_response_message(
        QueryResponseMessage(
            query_received=query,
            is_executed=True,
            body=b'{"name": "Alice", "email": "alice@example.com"}',
        )
    )

client.subscribe_to_queries(
    subscription=QueriesSubscription(
        channel="data-service",
        on_receive_query_callback=on_query,
        on_error_callback=lambda e: print(f"Error: {e}"),
    ),
    cancel=CancellationToken(),
)
time.sleep(60)
client.close()
```

## See Also

- [Commands](commands.md) — request/reply without data
- [API Reference](../api/cq.md)
