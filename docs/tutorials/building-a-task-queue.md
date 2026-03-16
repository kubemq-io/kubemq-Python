# Building a Task Queue

This tutorial shows you how to build a reliable task queue with KubeMQ in Python. Unlike fire-and-forget events, queues guarantee that every message is delivered and processed with explicit acknowledgment. By the end, you'll know how to send tasks, consume them, handle failures, and route poison messages to a dead-letter queue — using both sync and async clients.

## What You'll Build

A task processing pipeline where:
1. A producer enqueues work items
2. A consumer pulls and processes them with manual acknowledgment
3. Failed messages are rejected back to the queue for retry
4. Messages that fail too many times land in a dead-letter queue for investigation

## Prerequisites

- **Python 3.9 or later** ([download](https://www.python.org/downloads/))
- **KubeMQ server** running on `localhost:50000`:
  ```bash
  docker run -d -p 50000:50000 -p 9090:9090 kubemq/kubemq
  ```
- A virtual environment:
  ```bash
  mkdir kubemq-queue-tutorial && cd kubemq-queue-tutorial
  python -m venv .venv
  source .venv/bin/activate
  pip install kubemq
  ```

## Step 1: Send Messages to a Queue

Queues differ from events in a fundamental way: messages persist until a consumer explicitly acknowledges them. This makes queues the right choice for tasks that *must* be processed — order fulfillment, payment processing, email dispatch, and so on.

Create `queue_tutorial.py`:

```python
from __future__ import annotations

from kubemq import QueueMessage, QueuesClient


def main() -> None:
    with QueuesClient(
        address="localhost:50000",
        client_id="task-queue-tutorial",
    ) as client:

        tasks = ["resize-image-001", "send-email-042", "generate-report-7"]

        for task in tasks:
            result = client.send_queue_message(
                QueueMessage(
                    channel="tutorial.tasks",
                    body=task.encode(),
                    metadata="task",
                )
            )
            print(f"Enqueued: {task} (id={result.id})")


if __name__ == "__main__":
    main()
```

**What's happening:**

- `QueuesClient` handles the gRPC connection to the KubeMQ server. Using it as a context manager ensures proper cleanup.
- `QueueMessage` takes a channel, body (as `bytes`), and optional metadata and tags. Tags are key-value pairs useful for routing and filtering.
- `send_queue_message` delivers the message to the server and returns a result containing the assigned message ID and timestamp.
- Unlike events, the message now sits in the queue waiting for a consumer. It won't disappear until someone acknowledges it.

## Step 2: Receive and Process Messages

Now let's consume those tasks. Replace the contents of `queue_tutorial.py` with a program that sends *and* receives:

```python
from __future__ import annotations

from kubemq import QueueMessage, QueuesClient


def main() -> None:
    with QueuesClient(
        address="localhost:50000",
        client_id="task-queue-tutorial",
    ) as client:

        # --- Producer: enqueue tasks ---
        tasks = ["resize-image-001", "send-email-042", "generate-report-7"]
        for task in tasks:
            result = client.send_queue_message(
                QueueMessage(
                    channel="tutorial.tasks",
                    body=task.encode(),
                    metadata="task",
                )
            )
            print(f"[Producer] Enqueued: {task} (id={result.id})")

        # --- Consumer: pull and process ---
        response = client.receive_queue_messages(
            channel="tutorial.tasks",
            max_messages=10,
            wait_timeout_in_seconds=5,
            auto_ack=True,
        )

        print(f"\n[Consumer] Received {len(response.messages)} messages:")
        for msg in response.messages:
            print(f"  Task: {msg.body.decode('utf-8')} (metadata={msg.metadata})")


if __name__ == "__main__":
    main()
```

**Why this matters:**

- `receive_queue_messages` is a pull-based API. You ask for up to N messages and wait up to M seconds. This gives you control over batch size and processing pace — critical for backpressure management.
- The `wait_timeout_in_seconds` parameter implements long polling: the server holds the connection open until messages arrive or the timeout expires. This is more efficient than repeatedly polling.
- Setting `auto_ack=True` automatically acknowledges every message upon receipt. In Step 3, we'll switch to manual acknowledgment for finer control.

### Expected Output

```
[Producer] Enqueued: resize-image-001 (id=...)
[Producer] Enqueued: send-email-042 (id=...)
[Producer] Enqueued: generate-report-7 (id=...)

[Consumer] Received 3 messages:
  Task: resize-image-001 (metadata=task)
  Task: send-email-042 (metadata=task)
  Task: generate-report-7 (metadata=task)
```

## Step 3: Handle Acknowledgment and Rejection

In production, you need to tell the queue whether processing succeeded (ack) or failed (reject). Without explicit acknowledgment, the message stays invisible but isn't removed — it will eventually reappear for another attempt.

Omit `auto_ack` (defaults to `False`) for manual control:

```python
from __future__ import annotations

from kubemq import QueueMessage, QueuesClient


def main() -> None:
    with QueuesClient(
        address="localhost:50000",
        client_id="task-queue-ack-tutorial",
    ) as client:

        # Enqueue tasks — some will "fail" processing.
        tasks = ["valid-task-1", "bad-task-crash", "valid-task-2"]
        for task in tasks:
            client.send_queue_message(
                QueueMessage(
                    channel="tutorial.tasks.ack",
                    body=task.encode(),
                )
            )
        print("[Producer] Sent 3 tasks\n")

        # Receive with manual acknowledgment.
        response = client.receive_queue_messages(
            channel="tutorial.tasks.ack",
            max_messages=10,
            wait_timeout_in_seconds=5,
        )

        print(f"[Consumer] Received {len(response.messages)} messages:")
        for msg in response.messages:
            body = msg.body.decode("utf-8")

            if "bad" in body:
                msg.reject()
                print(f"  REJECT: {body} (will return to queue)")
            else:
                msg.ack()
                print(f"  ACK:    {body} (removed from queue)")


if __name__ == "__main__":
    main()
```

**The ack/reject decision:**

- **`msg.ack()`**: "I processed this successfully." The message is permanently removed from the queue.
- **`msg.reject()`**: "Processing failed." The message goes back to the queue so another consumer (or the same one later) can retry it.

This is the foundation of reliable message processing. Every message gets exactly one chance per pull, and your code decides the outcome.

### Expected Output

```
[Producer] Sent 3 tasks

[Consumer] Received 3 messages:
  ACK:    valid-task-1 (removed from queue)
  REJECT: bad-task-crash (will return to queue)
  ACK:    valid-task-2 (removed from queue)
```

## Step 4: Add Dead-Letter Queue Handling

Rejecting messages endlessly creates an infinite retry loop. Dead-letter queues (DLQs) solve this by catching messages that have been rejected too many times. You set a max receive count, and KubeMQ automatically routes the message to a DLQ channel after that threshold is crossed.

```python
from __future__ import annotations

from kubemq import QueueMessage, QueuesClient


def main() -> None:
    with QueuesClient(
        address="localhost:50000",
        client_id="task-queue-dlq-tutorial",
    ) as client:

        channel = "tutorial.tasks.dlq"
        dlq_channel = "tutorial.tasks.dlq.dead-letters"

        # Send a message with DLQ policy:
        # after 3 failed receives, move to the dead-letter queue.
        client.send_queue_message(
            QueueMessage(
                channel=channel,
                body=b"fragile-task-99",
                metadata="needs-careful-handling",
                attempts_before_dead_letter_queue=3,
                dead_letter_queue=dlq_channel,
            )
        )
        print(f"[Producer] Sent task with DLQ policy")
        print(f"  Max attempts: 3")
        print(f"  DLQ channel:  {dlq_channel}\n")

        # Simulate 3 failed processing attempts.
        for attempt in range(1, 4):
            response = client.receive_queue_messages(
                channel=channel,
                max_messages=1,
                wait_timeout_in_seconds=3,
            )
            if not response.messages:
                print(f"[Consumer] Attempt {attempt}: No message (moved to DLQ)")
                break

            for msg in response.messages:
                print(
                    f"[Consumer] Attempt {attempt}: Received "
                    f"'{msg.body.decode('utf-8')}' "
                    f"(receive_count={msg.receive_count}) — rejecting"
                )
                msg.reject()

        # Check the DLQ — the message should be there now.
        print("\n[DLQ Monitor] Checking dead-letter queue...")
        dlq_response = client.receive_queue_messages(
            channel=dlq_channel,
            max_messages=10,
            wait_timeout_in_seconds=3,
            auto_ack=True,
        )

        if dlq_response.messages:
            for msg in dlq_response.messages:
                print(f"  Dead letter: {msg.body.decode('utf-8')}")
        else:
            print("  No messages in DLQ yet")


if __name__ == "__main__":
    main()
```

**Why dead-letter queues matter:**

- Without a DLQ, a poison message (one that always fails processing) blocks your pipeline. Every consumer that pulls it will fail, reject it, and the cycle repeats.
- `attempts_before_dead_letter_queue=3` tells KubeMQ: "If this message is received 3 times without being acknowledged, stop retrying."
- `dead_letter_queue=dlq_channel` tells KubeMQ where to send the message after the limit is reached. You can then monitor the DLQ channel separately for manual investigation or alerting.

### Expected Output

```
[Producer] Sent task with DLQ policy
  Max attempts: 3
  DLQ channel:  tutorial.tasks.dlq.dead-letters

[Consumer] Attempt 1: Received 'fragile-task-99' (receive_count=1) — rejecting
[Consumer] Attempt 2: Received 'fragile-task-99' (receive_count=2) — rejecting
[Consumer] Attempt 3: No message (moved to DLQ)

[DLQ Monitor] Checking dead-letter queue...
  Dead letter: fragile-task-99
```

## Async Version

For `asyncio`-based applications, use `AsyncQueuesClient`. Here's the send-and-receive pattern:

```python
from __future__ import annotations

import asyncio

from kubemq import AsyncQueuesClient, QueueMessage


async def main() -> None:
    async with AsyncQueuesClient(
        address="localhost:50000",
        client_id="task-queue-async-tutorial",
    ) as client:

        # Send tasks
        for i in range(3):
            result = await client.send_queue_message(
                QueueMessage(
                    channel="tutorial.tasks.async",
                    body=f"async-task-{i + 1}".encode(),
                )
            )
            print(f"[Producer] Enqueued: async-task-{i + 1} (id={result.id})")

        # Receive and acknowledge
        response = await client.receive_queue_messages(
            channel="tutorial.tasks.async",
            max_messages=10,
            wait_timeout_seconds=5,
        )

        print(f"\n[Consumer] Received {len(response.messages)} messages:")
        for msg in response.messages:
            print(f"  {msg.body.decode('utf-8')}")
            msg.ack()


if __name__ == "__main__":
    asyncio.run(main())
```

The async API mirrors the sync API but uses `await` for all operations. Use it when your application already runs an asyncio event loop (FastAPI, aiohttp, etc.).

## Key Concepts

| Concept | What It Means |
|---|---|
| **At-least-once delivery** | Every message is delivered at least once. Ack removes it; reject returns it for retry. |
| **Pull-based consumption** | Consumers control the pace — no messages are pushed until requested. |
| **Long polling** | `wait_timeout_in_seconds` avoids busy-waiting by holding the connection open until messages arrive. |
| **Dead-letter queue** | A safety net for messages that repeatedly fail processing. Prevents poison messages from blocking the pipeline. |
| **Context manager** | `with QueuesClient(...)` ensures the connection is always properly closed. |

## Next Steps

- **Batch sending**: Send many messages in one call for high-throughput producers. See [`examples/queues/batch_send.py`](../../examples/queues/batch_send.py).
- **Delayed messages**: Schedule messages to become visible after a delay. See [`examples/queues/delayed_messages.py`](../../examples/queues/delayed_messages.py).
- **Peek messages**: Inspect messages without consuming them. See [`examples/queues/peek_messages.py`](../../examples/queues/peek_messages.py).
- **Request-reply**: If your tasks need to send results back, see [Implementing Request-Reply with Commands](request-reply-with-commands.md).
