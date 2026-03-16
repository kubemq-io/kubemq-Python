# Implementing CQRS with KubeMQ

This guide demonstrates a Command Query Responsibility Segregation (CQRS) architecture using KubeMQ's native messaging primitives: commands for writes, queries for reads, and events for state synchronization.

## Architecture

```
┌────────┐  command   ┌──────────────┐  event   ┌──────────────┐
│ Client │───────────▶│ Write Service │────────▶│ Read Service │
│        │            │ (cmd handler) │          │ (projection) │
│        │◀───────────│              │          │              │
│        │  query     └──────────────┘          └──────────────┘
│        │────────────────────────────────────▶│              │
│        │◀───────────────────────────────────│              │
└────────┘                                     └──────────────┘
```

1. **Commands** carry write intent — "create order", "update status". The write service processes commands and emits domain events.
2. **Events** propagate state changes to read-side projections asynchronously.
3. **Queries** retrieve data from the read-optimized projection, independent of the write model.

## Prerequisites

- KubeMQ server on `localhost:50000`
- `pip install kubemq`

## Write Service — Command Handler

The write service subscribes to commands, validates them, applies business logic, and publishes domain events.

```python
import json
from kubemq import (
    CancellationToken,
    CQClient,
    CommandMessageReceived,
    CommandResponseMessage,
    CommandsSubscription,
    EventMessage,
    PubSubClient,
)

orders_store: dict[str, str] = {}

def start_write_service():
    cq = CQClient(address="localhost:50000", client_id="cqrs-write-svc")
    pubsub = PubSubClient(address="localhost:50000", client_id="cqrs-write-events")
    cancel = CancellationToken()

    def on_command(cmd: CommandMessageReceived) -> None:
        body = cmd.body.decode("utf-8")
        order_id = cmd.tags.get("order_id", "unknown")
        print(f"[Write] Command received: {body}")

        orders_store[order_id] = body

        cq.send_response_message(
            CommandResponseMessage(
                command_received=cmd,
                is_executed=True,
            )
        )

        pubsub.publish_event(
            EventMessage(
                channel="cqrs.events",
                body=cmd.body,
                metadata=order_id,
            )
        )
        print(f"[Write] Order {order_id} persisted and event emitted")

    def on_error(err: str) -> None:
        print(f"[Write] Error: {err}")

    cq.subscribe_to_commands(
        subscription=CommandsSubscription(
            channel="cqrs.commands",
            on_receive_command_callback=on_command,
            on_error_callback=on_error,
        ),
        cancel=cancel,
    )
    return cancel, cq, pubsub
```

## Read Service — Query Handler with Event Projection

The read service maintains a denormalized projection updated by domain events, and serves queries against it.

```python
from kubemq import (
    CancellationToken,
    CQClient,
    EventMessageReceived,
    EventsSubscription,
    PubSubClient,
    QueriesSubscription,
    QueryMessageReceived,
    QueryResponseMessage,
)

projection: dict[str, str] = {}

def start_read_service():
    cq = CQClient(address="localhost:50000", client_id="cqrs-read-svc")
    pubsub = PubSubClient(address="localhost:50000", client_id="cqrs-read-events")
    cancel = CancellationToken()

    def on_event(event: EventMessageReceived) -> None:
        key = event.metadata
        projection[key] = event.body.decode("utf-8")
        print(f"[Read] Projection updated: key={key}")

    pubsub.subscribe_to_events(
        subscription=EventsSubscription(
            channel="cqrs.events",
            on_receive_event_callback=on_event,
            on_error_callback=lambda e: print(f"[Read] Event error: {e}"),
        ),
        cancel=cancel,
    )

    def on_query(q: QueryMessageReceived) -> None:
        key = q.body.decode("utf-8")
        data = projection.get(key, "")
        found = key in projection
        result = json.dumps({"found": found, "data": data})

        cq.send_response_message(
            QueryResponseMessage(
                query_received=q,
                is_executed=True,
                body=result.encode(),
            )
        )
        print(f"[Read] Query served: key={key} found={found}")

    cq.subscribe_to_queries(
        subscription=QueriesSubscription(
            channel="cqrs.queries",
            on_receive_query_callback=on_query,
            on_error_callback=lambda e: print(f"[Read] Query error: {e}"),
        ),
        cancel=cancel,
    )
    return cancel, cq, pubsub
```

## Client — Sending Commands and Queries

```python
import time
from kubemq import CQClient, CommandMessage, QueryMessage

def main():
    w_cancel, w_cq, w_pubsub = start_write_service()
    r_cancel, r_cq, r_pubsub = start_read_service()
    time.sleep(1)

    with CQClient(address="localhost:50000", client_id="cqrs-client") as client:
        # Write via command
        client.send_command_request(
            CommandMessage(
                channel="cqrs.commands",
                body=b'{"item":"widget","qty":5}',
                tags={"order_id": "ORD-001"},
                timeout_in_seconds=10,
            )
        )

        time.sleep(0.5)

        # Read via query
        resp = client.send_query_request(
            QueryMessage(
                channel="cqrs.queries",
                body=b"ORD-001",
                timeout_in_seconds=10,
            )
        )
        print(f"[Client] Query result: {resp.body.decode('utf-8')}")

    w_cancel.cancel()
    r_cancel.cancel()
    for c in (w_cq, w_pubsub, r_cq, r_pubsub):
        c.close()

if __name__ == "__main__":
    main()
```

## Design Considerations

| Concern | Approach |
|---|---|
| **Consistency** | Eventually consistent — events propagate asynchronously to the read model |
| **Ordering** | Use events-store with sequence replay if strict ordering matters |
| **Durability** | Commands are request-reply; the write service persists before acking |
| **Scaling** | Read and write services scale independently via consumer groups |
| **Failure** | If the read service misses events, replay from events-store |

## When to Use This Pattern

- Systems where read and write workloads have different scaling requirements
- Domain models that benefit from separate write validation and read optimization
- Microservices that need event-driven state synchronization across bounded contexts
