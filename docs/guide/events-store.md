# Events Store

Persistent pub/sub with replay capability. Subscribers can start from a
sequence number, timestamp, or receive only new messages.

## Overview

- **Delivery:** At-least-once — messages are persisted and replayed
- **Pattern:** Persistent publish/subscribe with replay
- **Use cases:** Audit trails, event sourcing, replay for late subscribers

## Publish a persistent event

```python
from kubemq import PubSubClient, EventStoreMessage

with PubSubClient(address="localhost:50000") as client:
    result = client.publish_event_store(
        EventStoreMessage(
            channel="audit-log",
            body=b"User logged in",
            tags={"user_id": "456"},
        )
    )
    print(f"Sent: {result.sent}, ID: {result.id}")
```

## Subscribe with replay

```python
import time
from kubemq import (
    PubSubClient, EventsStoreSubscription,
    CancellationToken, StartPosition,
)

def on_event(event):
    print(f"Seq: {event.sequence}, Body: {event.body.decode('utf-8')}")

cancel = CancellationToken()

with PubSubClient(address="localhost:50000") as client:
    # Start from the beginning of the store
    client.subscribe_to_events_store(
        subscription=EventsStoreSubscription(
            channel="audit-log",
            on_receive_event_callback=on_event,
            on_error_callback=lambda e: print(f"Error: {e}"),
            events_store_type=StartPosition.StartFromFirst,
        ),
        cancel=cancel,
    )
    time.sleep(60)
    cancel.cancel()
```

### Start Position Options

| Position | Description |
|----------|-------------|
| `StartFromNew` | Only new messages after subscription |
| `StartFromFirst` | Replay all messages from the beginning |
| `StartFromLast` | Start from the last message |
| `StartFromSequence` | Start from a specific sequence number |
| `StartFromTime` | Start from a specific timestamp |

## See Also

- [Events](events.md) — fire-and-forget events
- [API Reference](../api/pubsub.md)
