# KubeMQ Python SDK

Production-grade Python client for [KubeMQ](https://kubemq.io) message broker.

## Features

- **5 messaging patterns:** Events, Events Store, Queues, Commands, Queries
- **Sync + Async:** Native `asyncio` clients alongside sync clients
- **Type-safe:** Full type annotations with `py.typed` marker
- **Observable:** OpenTelemetry integration via `kubemq[otel]`
- **Resilient:** Auto-reconnect, retry policies, structured error hierarchy
- **Context managers:** `with`/`async with` for automatic resource cleanup

## Quick Install

```bash
pip install kubemq
```

## Quick Example

```python
from kubemq import PubSubClient, EventMessage

with PubSubClient(address="localhost:50000") as client:
    client.publish_event(
        EventMessage(channel="my-channel", body=b"Hello KubeMQ!")
    )
```

## Getting Started

- [Quick Start](quickstart.md) — first message in 5 minutes
- [Installation](installation.md) — detailed setup instructions
- [Examples](https://github.com/kubemq-io/kubemq-Python/tree/v4/examples) — comprehensive code examples

## Navigation

- **User Guide** — pattern-by-pattern usage guides
- **API Reference** — auto-generated from docstrings
- **Troubleshooting** — common issues and solutions
- **Migration Guide** — upgrading from v3 to v4
