# Queues

Pull-based message queues with acknowledgment, reject, requeue, dead-letter
queues, delayed delivery, and visibility timeout.

## Overview

- **Delivery:** At-least-once with explicit acknowledgment
- **Pattern:** Point-to-point (one consumer per message)
- **Use cases:** Job processing, task distribution, work queues

## Send a message

```python
from kubemq import QueuesClient, QueueMessage

with QueuesClient(address="localhost:50000") as client:
    result = client.send_queue_message(
        QueueMessage(
            channel="jobs",
            body=b"Process order #1001",
            tags={"priority": "high"},
        )
    )
    print(f"Sent: ID={result.id}")
```

## Receive and acknowledge

```python
from kubemq import QueuesClient

with QueuesClient(address="localhost:50000") as client:
    response = client.receive_queue_messages(
        channel="jobs",
        max_messages=10,
        wait_timeout_in_seconds=30,
    )
    for msg in response.messages:
        try:
            process(msg.body)
            msg.ack()
        except Exception:
            msg.reject()
```

## Delayed delivery

```python
from kubemq import QueueMessage

msg = QueueMessage(
    channel="scheduled-jobs",
    body=b"Run in 60 seconds",
    policy={"delay_seconds": 60},
)
```

## Visibility timeout

```python
response = client.receive_queue_messages(
    channel="jobs",
    max_messages=1,
    wait_timeout_in_seconds=10,
    visibility_seconds=120,
)
```

## Peek (waiting messages)

```python
waiting = client.waiting(
    channel="jobs",
    max_messages=10,
    wait_timeout_in_seconds=5,
)
for msg in waiting.messages:
    print(f"Waiting: {msg.body.decode('utf-8')}")
```

## See Also

- [API Reference](../api/queues.md)
- [Examples](https://github.com/kubemq-io/kubemq-Python/tree/v4/examples/queues)
