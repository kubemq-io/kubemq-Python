# Events (Pub/Sub)

Fire-and-forget messaging with at-most-once delivery to all subscribers
on a channel.

## Overview

- **Delivery:** At-most-once — if no subscribers are connected, the event is
  silently dropped
- **Pattern:** Publish/subscribe (fan-out to all subscribers)
- **Use cases:** Real-time notifications, log streaming, metrics broadcasting

## Sync Usage

### Publish an event

```python
from kubemq import PubSubClient, EventMessage

with PubSubClient(address="localhost:50000") as client:
    client.publish_event(
        EventMessage(
            channel="notifications",
            body=b"User signed up",
            metadata="signup-event",
            tags={"user_id": "123", "event_type": "signup"},
        )
    )
```

### Subscribe to events

```python
import time
from kubemq import PubSubClient, EventsSubscription, CancellationToken

def on_event(event):
    print(f"Channel: {event.channel}")
    print(f"Body: {event.body.decode('utf-8')}")
    print(f"Tags: {event.tags}")

cancel = CancellationToken()

with PubSubClient(address="localhost:50000") as client:
    client.subscribe_to_events(
        subscription=EventsSubscription(
            channel="notifications",
            on_receive_event_callback=on_event,
            on_error_callback=lambda e: print(f"Error: {e}"),
        ),
        cancel=cancel,
    )
    time.sleep(60)
    cancel.cancel()
```

## Async Usage

```python
import asyncio
from kubemq import AsyncPubSubClient, EventMessage

async def main():
    async with AsyncPubSubClient(address="localhost:50000") as client:
        await client.publish_event(
            EventMessage(channel="notifications", body=b"Hello async!")
        )

asyncio.run(main())
```

## Channel Management

```python
with PubSubClient(address="localhost:50000") as client:
    client.create_events_channel("my-channel")
    channels = client.list_events_channels()
    for ch in channels:
        print(f"Channel: {ch.name}, Subscribers: {ch.incoming}")
    client.delete_events_channel("my-channel")
```

## See Also

- [Events Store](events-store.md) — persistent events with replay
- [API Reference](../api/pubsub.md)
- [Examples](https://github.com/kubemq-io/kubemq-Python/tree/v4/examples/pubsub)
