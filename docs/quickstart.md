# Quick Start

Get your first message sent and received in under 5 minutes.

## Prerequisites

- Python 3.11+
- `pip install kubemq`
- KubeMQ server running at `localhost:50000`
  ([install KubeMQ](https://github.com/kubemq-io/kubemq-community))

Start KubeMQ locally:

```bash
docker run -d -p 8080:8080 -p 50000:50000 kubemq/kubemq-community
```

---

## Events (Pub/Sub)

Fire-and-forget messaging to all subscribers on a channel.

### Send an event

```python
from kubemq import PubSubClient, EventMessage

with PubSubClient(address="localhost:50000") as client:
    client.publish_event(
        EventMessage(channel="quickstart", body=b"Hello KubeMQ!")
    )
    print("Event sent!")
```

### Subscribe to events

```python
import time
from kubemq import PubSubClient, EventsSubscription, CancellationToken

def on_event(event):
    print(f"Received: {event.body.decode('utf-8')}")

with PubSubClient(address="localhost:50000") as client:
    client.subscribe_to_events(
        subscription=EventsSubscription(
            channel="quickstart",
            on_receive_event_callback=on_event,
            on_error_callback=lambda e: print(f"Error: {e}"),
        ),
        cancel=CancellationToken(),
    )
    time.sleep(60)  # Keep listening
```

**Expected output (subscriber):**

```
Received: Hello KubeMQ!
```

### Async variant

```python
import asyncio
from kubemq import AsyncPubSubClient, EventMessage

async def main():
    async with AsyncPubSubClient(address="localhost:50000") as client:
        await client.publish_event(
            EventMessage(channel="quickstart", body=b"Hello async KubeMQ!")
        )
        print("Async event sent!")

asyncio.run(main())
```

---

## Queues

Pull-based message queues with acknowledgment.

### Send and receive a queue message

```python
from kubemq import QueuesClient, QueueMessage

with QueuesClient(address="localhost:50000") as client:
    # Send
    result = client.send_queue_message(
        QueueMessage(channel="quickstart-queue", body=b"Task #1")
    )
    print(f"Sent: ID={result.id}")

    # Receive
    response = client.receive_queue_messages(
        channel="quickstart-queue", max_messages=1, wait_timeout_in_seconds=10
    )
    for msg in response.messages:
        print(f"Received: {msg.body.decode('utf-8')}")
        msg.ack()
```

**Expected output:**

```
Sent: ID=<uuid>
Received: Task #1
```

### Async variant

```python
import asyncio
from kubemq import AsyncQueuesClient, QueueMessage

async def main():
    async with AsyncQueuesClient(address="localhost:50000") as client:
        result = await client.send_queue_message(
            QueueMessage(channel="quickstart-queue", body=b"Task #1")
        )
        print(f"Sent: ID={result.id}")

        response = await client.receive_queue_messages(
            channel="quickstart-queue", max_messages=1, wait_timeout_in_seconds=10
        )
        for msg in response.messages:
            print(f"Received: {msg.body.decode('utf-8')}")
            await msg.ack()

asyncio.run(main())
```

---

## RPC (Commands & Queries)

Request/reply messaging for command execution and data queries.

### Handle and send a command

```python
import time
from kubemq import (
    CQClient, CommandMessage, CommandsSubscription,
    CommandResponseMessage, CancellationToken,
)

client = CQClient(address="localhost:50000")

def on_command(cmd):
    print(f"Received command: {cmd.body.decode('utf-8')}")
    client.send_response_message(
        CommandResponseMessage(command_received=cmd, is_executed=True)
    )

client.subscribe_to_commands(
    subscription=CommandsSubscription(
        channel="quickstart-commands",
        on_receive_command_callback=on_command,
        on_error_callback=lambda e: print(f"Error: {e}"),
    ),
    cancel=CancellationToken(),
)
time.sleep(1)

response = client.send_command(
    CommandMessage(
        channel="quickstart-commands",
        body=b"Turn on lights",
        timeout_in_seconds=10,
    )
)
print(f"Command executed: {response.is_executed}")
client.close()
```

**Expected output:**

```
Received command: Turn on lights
Command executed: True
```

### Async variant

```python
import asyncio
from kubemq import (
    AsyncCQClient, CommandMessage, CommandsSubscription,
    CommandResponseMessage, AsyncCancellationToken,
)

async def main():
    async with AsyncCQClient(address="localhost:50000") as client:
        async def on_command(cmd):
            print(f"Received command: {cmd.body.decode('utf-8')}")
            await client.send_response_message(
                CommandResponseMessage(command_received=cmd, is_executed=True)
            )

        await client.subscribe_to_commands(
            subscription=CommandsSubscription(
                channel="quickstart-commands",
                on_receive_command_callback=on_command,
                on_error_callback=lambda e: print(f"Error: {e}"),
            ),
            cancel=AsyncCancellationToken(),
        )
        await asyncio.sleep(1)

        response = await client.send_command(
            CommandMessage(
                channel="quickstart-commands",
                body=b"Turn on lights",
                timeout_in_seconds=10,
            )
        )
        print(f"Command executed: {response.is_executed}")

asyncio.run(main())
```

---

## Next Steps

- [Code Examples](https://github.com/kubemq-io/kubemq-Python/tree/v4/examples) — comprehensive examples for all patterns
- [API Reference](https://kubemq-io.github.io/kubemq-Python/api/pubsub/) — auto-generated API docs
- [Troubleshooting](https://github.com/kubemq-io/kubemq-Python/blob/v4/docs/troubleshooting.md) — common issues and solutions
