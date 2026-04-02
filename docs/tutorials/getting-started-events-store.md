# Getting Started with Events Store

This tutorial walks you through building a persistent pub/sub system with KubeMQ events store in Python. By the end, you'll have a working publisher and subscriber exchanging durable messages — and you'll learn how to replay events from a specific position in the store.

## What You'll Build

A persistent event-driven system where a subscriber listens on a channel and receives events that are stored by KubeMQ. Unlike regular events (fire-and-forget), **events store** persists messages so subscribers can replay them from the beginning, from a specific sequence, or from a point in time. This makes events store ideal for audit logs, event sourcing, and scenarios where late-joining subscribers need to catch up on history.

## Prerequisites

- **Python 3.11 or later** ([download](https://www.python.org/downloads/))
- **KubeMQ server** running on `localhost:50000`. The quickest way:
  ```bash
  docker run -d -p 50000:50000 -p 9090:9090 kubemq/kubemq
  ```
- A virtual environment for your project:
  ```bash
  mkdir kubemq-events-store-tutorial && cd kubemq-events-store-tutorial
  python -m venv .venv
  source .venv/bin/activate  # On Windows: .venv\Scripts\activate
  ```

## Step 1: Install the SDK

Install the KubeMQ Python SDK:

```bash
pip install kubemq
```

## Step 2: Create a Subscriber with Start Position

Create a file called `events_store_tutorial.py`. The subscriber uses `EventsStoreSubscription` and must specify an `events_store_type` to control where to start receiving from.

```python
from __future__ import annotations

import time

from kubemq import (
    CancellationToken,
    EventStoreMessage,
    EventStoreMessageReceived,
    EventsStoreSubscription,
    PubSubClient,
)
from kubemq.pubsub.events_store_subscription import EventsStoreType


def main() -> None:
    with PubSubClient(
        address="localhost:50000",
        client_id="events-store-tutorial-client",
    ) as client:

        def on_receive_event(event: EventStoreMessageReceived) -> None:
            print(
                f"[Subscriber] Received — Id:{event.id}, Seq:{event.sequence}, "
                f"Body:{event.body.decode('utf-8')}"
            )

        def on_error(err: str) -> None:
            print(f"[Subscriber] Error: {err}")

        cancel = CancellationToken()

        client.subscribe_to_events_store(
            subscription=EventsStoreSubscription(
                channel="tutorial.events-store",
                on_receive_event_callback=on_receive_event,
                on_error_callback=on_error,
                events_store_type=EventsStoreType.StartNewOnly,
            ),
            cancel=cancel,
        )

        print("[Subscriber] Listening on channel: tutorial.events-store (StartNewOnly)")

        # ... publisher code will go here (Step 3) ...


if __name__ == "__main__":
    main()
```

Key points:

- **`EventsStoreSubscription`** defines the channel and an `events_store_type`. `StartNewOnly` means "only receive events published after I subscribe" — ideal when you care only about new messages.
- **`EventStoreMessageReceived`** includes a `sequence` field — each stored event gets a monotonically increasing sequence number.
- Use `subscribe_to_events_store` (not `subscribe_to_events`) — the store API is separate from the fire-and-forget events API.

## Step 3: Publish Persistent Events

Add the publishing code. Use `send_events_store_message` with `EventStoreMessage` — these messages are persisted by KubeMQ:

```python
        time.sleep(1)

        result = client.send_events_store_message(
            EventStoreMessage(
                channel="tutorial.events-store",
                body=b"Hello from the KubeMQ Events Store!",
            )
        )
        print(f"[Publisher] Event sent, result: {result}")

        time.sleep(2)
        cancel.cancel()
        print("\nDone!")
```

- **`EventStoreMessage`** takes a channel and body (as `bytes`). The message is stored on the server.
- **`send_events_store_message`** returns a result indicating success or failure — unlike fire-and-forget events, the store API confirms delivery.

## Step 4: Replay from a Sequence

To replay events from a specific sequence number (e.g., after a crash or for debugging), use `EventsStoreType.StartAtSequence` with `events_store_sequence_value`:

```python
        client.subscribe_to_events_store(
            subscription=EventsStoreSubscription(
                channel="tutorial.events-store",
                on_receive_event_callback=on_receive_event,
                on_error_callback=on_error,
                events_store_type=EventsStoreType.StartAtSequence,
                events_store_sequence_value=3,
            ),
            cancel=cancel,
        )
```

This subscribes starting from sequence 3 — you'll receive events 3, 4, 5, and so on. Other useful types include `StartFromFirst` (replay everything) and `StartFromLast` (only new events, like StartNewOnly).

## Complete Program

Here's the full program with persistent pub/sub and a brief replay demo:

```python
from __future__ import annotations

import time

from kubemq import (
    CancellationToken,
    EventStoreMessage,
    EventStoreMessageReceived,
    EventsStoreSubscription,
    PubSubClient,
)
from kubemq.pubsub.events_store_subscription import EventsStoreType


def main() -> None:
    with PubSubClient(
        address="localhost:50000",
        client_id="events-store-tutorial-client",
    ) as client:

        def on_receive_event(event: EventStoreMessageReceived) -> None:
            print(
                f"[Subscriber] Received — Id:{event.id}, Seq:{event.sequence}, "
                f"Body:{event.body.decode('utf-8')}"
            )

        def on_error(err: str) -> None:
            print(f"[Subscriber] Error: {err}")

        cancel = CancellationToken()

        client.subscribe_to_events_store(
            subscription=EventsStoreSubscription(
                channel="tutorial.events-store",
                on_receive_event_callback=on_receive_event,
                on_error_callback=on_error,
                events_store_type=EventsStoreType.StartNewOnly,
            ),
            cancel=cancel,
        )
        print("[Subscriber] Listening on channel: tutorial.events-store (StartNewOnly)")

        time.sleep(1)

        result = client.send_events_store_message(
            EventStoreMessage(
                channel="tutorial.events-store",
                body=b"Hello from the KubeMQ Events Store!",
            )
        )
        print(f"[Publisher] Event sent, result: {result}")

        time.sleep(2)
        cancel.cancel()
        print("\nDone!")


if __name__ == "__main__":
    main()
```

Run it:

```bash
python events_store_tutorial.py
```

### Expected Output

```
[Subscriber] Listening on channel: tutorial.events-store (StartNewOnly)
[Publisher] Event sent, result: <result>
[Subscriber] Received — Id:<message-id>, Seq:<sequence>, Body:Hello from the KubeMQ Events Store!

Done!
```

## Key Concepts

| Concept | What It Means |
|---|---|
| **Events Store** | Persistent pub/sub — messages are stored and can be replayed. Use `subscribe_to_events_store` and `send_events_store_message`. |
| **EventsStoreType** | Controls where the subscription starts: `StartNewOnly`, `StartFromFirst`, `StartFromLast`, `StartAtSequence`, `StartAtTime`, `StartAtTimeDelta`. |
| **Sequence** | Each stored event gets a monotonically increasing sequence number. Use `StartAtSequence` with `events_store_sequence_value` to replay from a specific point. |
| **EventStoreMessage** | Use this (not `EventMessage`) for persistent events. Body must be `bytes`. |

## Next Steps

- **Replay from time**: Use `EventsStoreType.StartAtTime` or `StartAtTimeDelta` to replay from a timestamp. See [`examples/events_store/replay_from_time.py`](../../examples/events_store/replay_from_time.py) and [`examples/events_store/start_at_time_delta.py`](../../examples/events_store/start_at_time_delta.py).
- **Consumer groups**: Load-balance events store processing across multiple subscribers. See [`examples/events_store/consumer_group.py`](../../examples/events_store/consumer_group.py).
- **Regular events**: For fire-and-forget (no persistence), see [Getting Started with Events](getting-started-events.md).
- **Queues**: For guaranteed delivery with acknowledgment, see [Building a Task Queue](building-a-task-queue.md).
