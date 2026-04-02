# Getting Started with Events

This tutorial walks you through building your first real-time pub/sub system with KubeMQ events in Python. By the end, you'll have a working publisher and subscriber exchanging messages — using both synchronous and asynchronous approaches — and a solid understanding of how each piece fits together.

## What You'll Build

A simple event-driven system where a subscriber listens on a channel and a publisher sends a message to it. Events in KubeMQ are **fire-and-forget**: the publisher sends a message and moves on. This makes them ideal for telemetry, logs, notifications, and any scenario where delivery acknowledgment isn't required.

## Prerequisites

- **Python 3.9 or later** ([download](https://www.python.org/downloads/))
- **KubeMQ server** running on `localhost:50000`. The quickest way:
  ```bash
  docker run -d -p 50000:50000 -p 9090:9090 kubemq/kubemq
  ```
- A virtual environment for your project:
  ```bash
  mkdir kubemq-events-tutorial && cd kubemq-events-tutorial
  python -m venv .venv
  source .venv/bin/activate  # On Windows: .venv\Scripts\activate
  ```

## Step 1: Install the SDK

Install the KubeMQ Python SDK:

```bash
pip install kubemq
```

This single package gives you access to events, queues, commands, and queries — with both sync and async clients.

## Step 2: Create a Subscriber

Create a file called `events_tutorial.py`. The subscriber needs to be listening *before* events are published — otherwise the event has no one to deliver to.

```python
from __future__ import annotations

import time

from kubemq import (
    CancellationToken,
    EventMessage,
    EventMessageReceived,
    EventsSubscription,
    PubSubClient,
)


def main() -> None:
    with PubSubClient(
        address="localhost:50000",
        client_id="events-tutorial-client",
    ) as client:

        def on_receive_event(event: EventMessageReceived) -> None:
            print("[Subscriber] Received event:")
            print(f"  Channel: {event.channel}")
            print(f"  Body:    {event.body.decode('utf-8')}")
            print(f"  ID:      {event.id}")

        def on_error(err: str) -> None:
            print(f"[Subscriber] Error: {err}")

        cancel = CancellationToken()

        client.subscribe_to_events(
            subscription=EventsSubscription(
                channel="tutorial.events",
                on_receive_event_callback=on_receive_event,
                on_error_callback=on_error,
            ),
            cancel=cancel,
        )

        print("[Subscriber] Listening on channel: tutorial.events")

        # ... publisher code will go here (Step 3) ...


if __name__ == "__main__":
    main()
```

Let's break down the key parts:

- **`PubSubClient`** creates a connection to the KubeMQ server. Using it as a context manager (`with`) ensures the connection is properly closed when you're done.
- **`EventsSubscription`** defines what channel to listen on and provides two callbacks: one for received events and one for errors. The subscription starts as soon as `subscribe_to_events` is called.
- **`CancellationToken`** gives you a way to stop the subscription later. Call `cancel.cancel()` when you want to unsubscribe.
- The callbacks run on a background thread, so the main thread can continue doing other work (like publishing).

## Step 3: Create a Publisher

Add the publishing code right after the subscription call. The subscriber needs a moment to register with the server, so we add a brief sleep before publishing:

```python
        time.sleep(1)

        client.publish_event(
            EventMessage(
                channel="tutorial.events",
                body=b"Hello from the KubeMQ Python SDK!",
                metadata="greeting",
            )
        )
        print("[Publisher] Event sent to channel: tutorial.events")

        time.sleep(2)
        cancel.cancel()
        print("\nDone!")
```

A few things to notice:

- **`EventMessage`** takes a channel, body (as `bytes`), and optional metadata. The body must be bytes — use `.encode()` on strings.
- **`publish_event`** is non-blocking from the publisher's perspective — it fires the event to the server and returns. There's no delivery receipt because events are fire-and-forget by design.
- We sleep for 2 seconds to give the subscriber time to receive and print the event, then cancel the subscription.

## Step 4: Run and See Output

Here's the complete program assembled:

```python
from __future__ import annotations

import time

from kubemq import (
    CancellationToken,
    EventMessage,
    EventMessageReceived,
    EventsSubscription,
    PubSubClient,
)


def main() -> None:
    with PubSubClient(
        address="localhost:50000",
        client_id="events-tutorial-client",
    ) as client:

        def on_receive_event(event: EventMessageReceived) -> None:
            print("[Subscriber] Received event:")
            print(f"  Channel: {event.channel}")
            print(f"  Body:    {event.body.decode('utf-8')}")
            print(f"  ID:      {event.id}")

        def on_error(err: str) -> None:
            print(f"[Subscriber] Error: {err}")

        cancel = CancellationToken()

        client.subscribe_to_events(
            subscription=EventsSubscription(
                channel="tutorial.events",
                on_receive_event_callback=on_receive_event,
                on_error_callback=on_error,
            ),
            cancel=cancel,
        )
        print("[Subscriber] Listening on channel: tutorial.events")

        time.sleep(1)

        client.publish_event(
            EventMessage(
                channel="tutorial.events",
                body=b"Hello from the KubeMQ Python SDK!",
                metadata="greeting",
            )
        )
        print("[Publisher] Event sent to channel: tutorial.events")

        time.sleep(2)
        cancel.cancel()
        print("\nDone!")


if __name__ == "__main__":
    main()
```

Run it:

```bash
python events_tutorial.py
```

### Expected Output

```
[Subscriber] Listening on channel: tutorial.events
[Publisher] Event sent to channel: tutorial.events
[Subscriber] Received event:
  Channel: tutorial.events
  Body:    Hello from the KubeMQ Python SDK!
  ID:      ...

Done!
```

## Async Version

The SDK provides native async clients for `asyncio`-based applications. Here's the same tutorial rewritten with `AsyncPubSubClient`:

```python
from __future__ import annotations

import asyncio

from kubemq import (
    AsyncCancellationToken,
    AsyncPubSubClient,
    EventMessage,
    EventsSubscription,
)


async def main() -> None:
    async with AsyncPubSubClient(
        address="localhost:50000",
        client_id="events-tutorial-async-client",
    ) as client:

        received = asyncio.Event()

        token = AsyncCancellationToken()

        async for event in client.subscribe_to_events(
            subscription=EventsSubscription(
                channel="tutorial.events.async",
                on_receive_event_callback=lambda e: None,
                on_error_callback=lambda e: print(f"Error: {e}"),
            ),
            cancellation_token=token,
        ):
            print(f"[Subscriber] Received: {event.body.decode('utf-8')}")
            received.set()
            break

        # Note: The async iterator pattern above blocks.
        # In practice, run the subscriber as a background task:


async def main_with_background() -> None:
    async with AsyncPubSubClient(
        address="localhost:50000",
        client_id="events-tutorial-async-client",
    ) as client:
        token = AsyncCancellationToken()
        received = asyncio.Event()

        async def subscriber() -> None:
            async for event in client.subscribe_to_events(
                subscription=EventsSubscription(
                    channel="tutorial.events.async",
                    on_receive_event_callback=lambda e: None,
                    on_error_callback=lambda e: print(f"Error: {e}"),
                ),
                cancellation_token=token,
            ):
                print(f"[Subscriber] Received: {event.body.decode('utf-8')}")
                received.set()
                break

        sub_task = asyncio.create_task(subscriber())
        await asyncio.sleep(1)

        await client.publish_event(
            EventMessage(
                channel="tutorial.events.async",
                body=b"Hello from async Python!",
            )
        )
        print("[Publisher] Async event sent")

        await asyncio.wait_for(received.wait(), timeout=5.0)
        token.cancel()
        sub_task.cancel()
        print("\nDone!")


if __name__ == "__main__":
    asyncio.run(main_with_background())
```

**Why async?** If your application already uses `asyncio` (web frameworks like FastAPI or aiohttp), the async client integrates naturally without blocking the event loop. For scripts and simpler applications, the sync client is perfectly fine.

## Key Concepts

| Concept | What It Means |
|---|---|
| **Fire-and-forget** | The publisher doesn't wait for subscribers to confirm receipt. Fast, but no delivery guarantee. |
| **Channel** | A named topic that publishers send to and subscribers listen on. Any string works. |
| **CancellationToken** | Controls the lifecycle of a subscription. Call `.cancel()` to stop listening. |
| **Context manager** | Using `with PubSubClient(...)` ensures the connection is cleaned up even if an exception occurs. |
| **Consumer group** | Set `group` on the subscription to load-balance events across multiple subscribers. |

## Next Steps

- **Persistent events**: Switch to `subscribe_to_events_store` / `send_event_store` for durable, replayable events. See [`examples/events_store/`](../../examples/events_store/).
- **Multiple subscribers**: Add consumer groups for load-balanced processing. See [`examples/events/consumer_group.py`](../../examples/events/consumer_group.py).
- **Wildcard subscriptions**: Subscribe to `tutorial.*` to catch events on any sub-channel. See [`examples/events/wildcard_subscription.py`](../../examples/events/wildcard_subscription.py).
- **Queues**: If you need guaranteed delivery with acknowledgment, see [Building a Task Queue](building-a-task-queue.md).
