# How to Use Consumer Groups

Distribute message processing across multiple subscribers using consumer groups for load-balanced delivery.

## What Consumer Groups Do

When multiple subscribers on the same channel share a **group name**, each message is delivered to exactly **one** member of the group. This provides:

- **Load balancing** — work is distributed across group members
- **Horizontal scaling** — add members to increase throughput
- **Fault tolerance** — if a member goes down, others continue receiving

Without a group, every subscriber receives every message (fan-out). With a group, each message goes to one subscriber (competing consumers).

## Events — Load-Balanced Pub/Sub

Set the `group` field on `EventsSubscription`:

```python
import time
from kubemq import (
    CancellationToken,
    EventMessage,
    EventMessageReceived,
    EventsSubscription,
    PubSubClient,
)


def make_handler(name: str):
    def handler(event: EventMessageReceived) -> None:
        print(f"[{name}] {event.body.decode('utf-8')}")
    return handler


def on_error(err: str) -> None:
    print(f"Error: {err}")


with PubSubClient(
    address="localhost:50000",
    client_id="events-group-demo",
) as client:
    cancel = CancellationToken()

    # Worker 1
    client.subscribe_to_events(
        subscription=EventsSubscription(
            channel="orders.process",
            group="order-workers",
            on_receive_event_callback=make_handler("Worker-1"),
            on_error_callback=on_error,
        ),
        cancel=cancel,
    )

    # Worker 2
    client.subscribe_to_events(
        subscription=EventsSubscription(
            channel="orders.process",
            group="order-workers",
            on_receive_event_callback=make_handler("Worker-2"),
            on_error_callback=on_error,
        ),
        cancel=cancel,
    )

    time.sleep(1)

    for i in range(6):
        client.publish_event(
            EventMessage(channel="orders.process", body=f"order-{i+1}".encode())
        )

    time.sleep(3)
    cancel.cancel()
```

Each of the 6 messages is delivered to exactly one worker.

## Events Store — Persistent Consumer Groups

Consumer groups also work with `EventsStoreSubscription`. Provide a `group` alongside an `events_store_type`:

```python
import time
from kubemq import (
    CancellationToken,
    EventStoreMessage,
    EventStoreMessageReceived,
    EventsStoreSubscription,
    PubSubClient,
)
from kubemq.pubsub.events_store_subscription import EventsStoreType


def make_handler(name: str):
    def handler(event: EventStoreMessageReceived) -> None:
        print(f"[{name}] seq={event.sequence} body={event.body.decode('utf-8')}")
    return handler


with PubSubClient(
    address="localhost:50000",
    client_id="store-group-demo",
) as client:
    cancel = CancellationToken()

    for name in ("Processor-1", "Processor-2"):
        client.subscribe_to_events_store(
            subscription=EventsStoreSubscription(
                channel="audit.logs",
                group="log-processors",
                on_receive_event_callback=make_handler(name),
                on_error_callback=lambda e: print(f"Error: {e}"),
                events_store_type=EventsStoreType.StartFromFirst,
            ),
            cancel=cancel,
        )

    time.sleep(1)

    for i in range(6):
        client.send_events_store_message(
            EventStoreMessage(channel="audit.logs", body=f"log-{i+1}".encode())
        )

    time.sleep(3)
    cancel.cancel()
```

## Commands — Load-Balanced RPC Handlers

For commands, each incoming command is routed to exactly one handler in the group:

```python
import time
from kubemq import (
    CancellationToken,
    CommandMessage,
    CommandMessageReceived,
    CommandResponseMessage,
    CommandsSubscription,
    CQClient,
)


def make_handler(name: str, client: CQClient):
    def handler(request: CommandMessageReceived) -> None:
        print(f"[{name}] Handling: {request.body.decode('utf-8')}")
        client.send_response_message(
            CommandResponseMessage(command_received=request, is_executed=True)
        )
    return handler


with CQClient(
    address="localhost:50000",
    client_id="cmd-group-demo",
) as client:
    cancel = CancellationToken()

    for name in ("Handler-1", "Handler-2"):
        client.subscribe_to_commands(
            subscription=CommandsSubscription(
                channel="tasks.execute",
                group="task-handlers",
                on_receive_command_callback=make_handler(name, client),
                on_error_callback=lambda e: print(f"Error: {e}"),
            ),
            cancel=cancel,
        )

    time.sleep(1)

    for i in range(4):
        response = client.send_command_request(
            CommandMessage(
                channel="tasks.execute",
                body=f"task-{i+1}".encode(),
                timeout_in_seconds=10,
            )
        )
        print(f"Task {i+1} executed: {response.is_executed}")

    cancel.cancel()
```

## Queries — Load-Balanced Request/Reply

Queries follow the same pattern. Each query goes to one handler:

```python
import time
from kubemq import (
    CancellationToken,
    CQClient,
    QueriesSubscription,
    QueryMessage,
    QueryMessageReceived,
    QueryResponseMessage,
)


def make_handler(name: str, client: CQClient):
    def handler(request: QueryMessageReceived) -> None:
        print(f"[{name}] Query: {request.body.decode('utf-8')}")
        client.send_response_message(
            QueryResponseMessage(
                query_received=request,
                is_executed=True,
                body=f"result from {name}".encode(),
            )
        )
    return handler


with CQClient(
    address="localhost:50000",
    client_id="query-group-demo",
) as client:
    cancel = CancellationToken()

    for name in ("Reader-1", "Reader-2"):
        client.subscribe_to_queries(
            subscription=QueriesSubscription(
                channel="inventory.lookup",
                group="readers",
                on_receive_query_callback=make_handler(name, client),
                on_error_callback=lambda e: print(f"Error: {e}"),
            ),
            cancel=cancel,
        )

    time.sleep(1)

    for i in range(4):
        response = client.send_query_request(
            QueryMessage(
                channel="inventory.lookup",
                body=f"item-{i+1}".encode(),
                timeout_in_seconds=10,
            )
        )
        print(f"Query {i+1} response: {response.body}")

    cancel.cancel()
```

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| Every subscriber gets every message | `group` not set or empty | Set `group="your-group-name"` on the subscription |
| Messages go to only one subscriber | Intended behavior for consumer groups | This is correct — omit `group` for fan-out |
| Uneven message distribution | Normal with small message counts | Distribution evens out over larger volumes |
| Subscriber not receiving messages | Different group name | Verify all members use the exact same group string |
