# Building an Event-Driven Processing Pipeline

This guide walks through a multi-stage processing pipeline that combines KubeMQ events for real-time fan-out with queues for reliable, exactly-once delivery.

## Architecture

```
┌──────────┐    events     ┌─────────────┐    queue     ┌──────────┐
│ Producer │──────────────▶│  Processor  │─────────────▶│  Output  │
│ (ingest) │  (fan-out)    │  (transform)│  (reliable)  │ (persist)│
└──────────┘               └─────────────┘              └──────────┘
```

1. **Producer** publishes raw data as events on a pub/sub channel.
2. **Processor** subscribes to events, transforms each payload, and enqueues results into a queue.
3. **Output** worker pulls from the queue with ack/nack semantics to guarantee delivery.

This separation lets you scale each stage independently. Events handle real-time fan-out while queues provide backpressure and delivery guarantees.

## Prerequisites

- KubeMQ server on `localhost:50000`
- `pip install kubemq`

## Stage 1 — Event Producer

The producer ingests raw data and publishes it as events. Multiple subscribers can receive each event.

```python
from kubemq import EventMessage, PubSubClient

def run_producer():
    with PubSubClient(
        address="localhost:50000",
        client_id="pipeline-producer",
    ) as client:
        orders = [
            '{"id":"ORD-1","item":"widget","qty":5}',
            '{"id":"ORD-2","item":"gadget","qty":2}',
            '{"id":"ORD-3","item":"gizmo","qty":10}',
        ]
        for order in orders:
            client.publish_event(
                EventMessage(
                    channel="pipeline.ingest",
                    body=order.encode(),
                    metadata="source:api",
                )
            )
            print(f"[Producer] Published: {order}")
```

## Stage 2 — Event Processor

The processor subscribes to events, transforms payloads, and enqueues enriched results for reliable downstream consumption.

```python
import json
from datetime import datetime, timezone

from kubemq import (
    CancellationToken,
    EventMessageReceived,
    EventsSubscription,
    PubSubClient,
    QueueMessage,
    QueuesClient,
)

def run_processor():
    pubsub = PubSubClient(
        address="localhost:50000",
        client_id="pipeline-processor-pubsub",
    )
    queues = QueuesClient(
        address="localhost:50000",
        client_id="pipeline-processor-queues",
    )
    cancel = CancellationToken()

    def on_event(event: EventMessageReceived) -> None:
        body = event.body.decode("utf-8")
        print(f"[Processor] Received event: {body}")

        enriched = json.dumps({
            "original": json.loads(body),
            "processed_at": datetime.now(timezone.utc).isoformat(),
        })

        queues.send_queue_message(
            QueueMessage(
                channel="pipeline.output",
                body=enriched.encode(),
            )
        )
        print(f"[Processor] Enqueued for output")

    def on_error(err: str) -> None:
        print(f"[Processor] Error: {err}")

    pubsub.subscribe_to_events(
        subscription=EventsSubscription(
            channel="pipeline.ingest",
            on_receive_event_callback=on_event,
            on_error_callback=on_error,
        ),
        cancel=cancel,
    )
    return cancel, pubsub, queues
```

## Stage 3 — Output Worker

The output worker pulls from the queue with exactly-once semantics. Failed messages remain on the queue for retry.

```python
from kubemq import QueuesClient

def run_output_worker():
    with QueuesClient(
        address="localhost:50000",
        client_id="pipeline-output-worker",
    ) as client:
        response = client.receive_queue_messages(
            channel="pipeline.output",
            max_messages=10,
            wait_timeout_in_seconds=5,
        )
        print(f"[Output] Received {len(response.messages)} messages:")
        for msg in response.messages:
            print(f"  → {msg.body.decode('utf-8')}")
            msg.ack()
```

## Putting It Together

```python
import time

def main():
    cancel, pubsub, queues = run_processor()
    time.sleep(1)

    run_producer()
    time.sleep(2)

    run_output_worker()

    cancel.cancel()
    pubsub.close()
    queues.close()

if __name__ == "__main__":
    main()
```

## Error Handling

- **Producer failures**: Log and skip; events are fire-and-forget by design.
- **Processor failures**: If the queue send fails, the event is lost. Use events-store instead of events if you need replay capability.
- **Output failures**: Messages stay in the queue. Use dead-letter queues for messages that fail repeatedly.

## When to Use This Pattern

- Stream processing with decoupled stages
- Ingestion pipelines where throughput matters more than ordering
- Systems that need both real-time notifications and guaranteed delivery
