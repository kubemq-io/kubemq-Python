# KubeMQ Python SDK

[![PyPI version](https://img.shields.io/pypi/v/kubemq.svg)](https://pypi.org/project/kubemq/)
[![CI](https://github.com/kubemq-io/kubemq-Python/actions/workflows/ci.yml/badge.svg)](https://github.com/kubemq-io/kubemq-Python/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/kubemq-io/kubemq-Python/branch/v4/graph/badge.svg)](https://codecov.io/gh/kubemq-io/kubemq-Python)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Description

KubeMQ is an enterprise-grade message broker for containers, designed for any
workload and architecture running in Kubernetes. This SDK provides a
production-ready Python client supporting all KubeMQ messaging patterns:
Events (pub/sub), Events Store (persistent pub/sub), Queues (with ack/reject),
Commands, and Queries (RPC).

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
  - [Send and Receive Events](#send-and-receive-events)
- [Messaging Patterns](#messaging-patterns)
  - [Events](#events)
  - [Events Store](#events-store)
  - [Queues](#queues)
  - [Commands](#commands)
  - [Queries](#queries)
- [Configuration](#configuration)
- [Error Handling](#error-handling)
- [Troubleshooting](#troubleshooting)
- [Security](#security)
- [Additional Resources](#additional-resources)
- [Contributing](#contributing)
- [License](#license)

## Installation

Requires **Python 3.11+**.

```bash
pip install kubemq
```

For optional features:

```bash
pip install kubemq[docs]    # API reference generation
pip install kubemq[otel]    # OpenTelemetry integration
```

## Quick Start

> **Prerequisites:** Python 3.11+, KubeMQ server running at `localhost:50000`
> ([install KubeMQ](https://github.com/kubemq-io/kubemq-community))

### Send and Receive Events

**Send an event:**

```python
from kubemq import PubSubClient, EventMessage

with PubSubClient(address="localhost:50000") as client:
    client.publish_event(
        EventMessage(channel="quickstart", body=b"Hello KubeMQ!")
    )
    print("Event sent!")
```

**Subscribe to events:**

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

See also: [Queues Quick Start](https://github.com/kubemq-io/kubemq-Python/blob/v4/docs/quickstart.md#queues) |
[RPC Quick Start](https://github.com/kubemq-io/kubemq-Python/blob/v4/docs/quickstart.md#rpc-commands--queries)

## Messaging Patterns

| Pattern | Delivery Guarantee | Use When | Example Use Case |
|---------|--------------------|----------|------------------|
| [Events](#events) | At-most-once | Fire-and-forget broadcasting to multiple subscribers | Real-time notifications, log streaming |
| [Events Store](#events-store) | At-least-once (persistent) | Subscribers must not miss messages, even if offline | Audit trails, event sourcing, replay |
| [Queues](#queues) | At-least-once (with ack) | Work must be processed exactly by one consumer with acknowledgment | Job processing, task distribution |
| [Commands](#commands) | At-most-once (request/reply) | You need a response confirming the action was executed | Device control, configuration changes |
| [Queries](#queries) | At-most-once (request/reply) | You need to retrieve data from a responder | Data lookups, service-to-service reads |

### Events

Fire-and-forget pub/sub. Use `PubSubClient` (sync) or `AsyncPubSubClient` (async).
[View examples →](https://github.com/kubemq-io/kubemq-Python/tree/v4/examples/pubsub)

### Events Store

Persistent pub/sub with replay. Subscribers can start from a sequence number,
timestamp, or receive only new messages.
[View examples →](https://github.com/kubemq-io/kubemq-Python/tree/v4/examples/pubsub)

### Queues

Pull-based message queues with acknowledgment, reject, requeue, dead-letter
queues, and delayed delivery.
[View examples →](https://github.com/kubemq-io/kubemq-Python/tree/v4/examples/queues)

### Commands

Request/reply where the sender expects confirmation of execution (no response payload).
[View examples →](https://github.com/kubemq-io/kubemq-Python/tree/v4/examples/cq)

### Queries

Request/reply where the sender expects a data response.
[View examples →](https://github.com/kubemq-io/kubemq-Python/tree/v4/examples/cq)

## Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `address` | `str` | `"localhost:50000"` | KubeMQ server gRPC address |
| `client_id` | `str` | hostname | Unique client identifier |
| `auth_token` | `str` | `""` | Authentication token |
| `tls` | `TLSConfig` | disabled | TLS configuration (cert, key, CA files) |
| `max_send_size` | `int` | `104857600` | Maximum send message size in bytes |
| `max_receive_size` | `int` | `104857600` | Maximum receive message size in bytes |
| `auto_reconnect` | `bool` | `True` | Auto-reconnect on connection loss |
| `reconnect_interval_seconds` | `int` | `1` | Seconds between reconnect attempts |
| `log_level` | `int \| None` | `None` (no logging) | Python logging level |

All clients accept these options as constructor arguments:

```python
from kubemq import PubSubClient, TLSConfig

client = PubSubClient(
    address="kubemq-server:50000",
    client_id="my-service",
    auth_token="your-token",
    tls=TLSConfig(
        enabled=True,
        cert_file="/path/to/cert.pem",
        key_file="/path/to/key.pem",
        ca_file="/path/to/ca.pem",
    ),
)
```

## Error Handling

All SDK errors extend `KubeMQError`:

```python
from kubemq import (
    PubSubClient, EventMessage,
    KubeMQError, KubeMQConnectionError, KubeMQTimeoutError,
)

try:
    with PubSubClient(address="localhost:50000") as client:
        client.publish_event(
            EventMessage(channel="ch1", body=b"hello")
        )
except KubeMQConnectionError as e:
    print(f"Connection failed: {e}")
except KubeMQTimeoutError as e:
    print(f"Operation timed out: {e}")
except KubeMQError as e:
    print(f"KubeMQ error: {e}")
```

See the [Troubleshooting Guide](https://github.com/kubemq-io/kubemq-Python/blob/v4/docs/troubleshooting.md)
for solutions to common errors.

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `Connection refused` | Ensure KubeMQ is running: `docker run -p 50000:50000 kubemq/kubemq-community` |
| `Authentication failed` | Verify `auth_token` matches server configuration |
| `Channel not found` | Create the channel first or enable auto-create on the server |
| `Timeout / deadline exceeded` | Increase `timeout_in_seconds` or check network latency |
| `No messages received` | Verify subscriber is connected *before* sender publishes |

→ [Full Troubleshooting Guide](https://github.com/kubemq-io/kubemq-Python/blob/v4/docs/troubleshooting.md) (11+ entries)

## Security

See [SECURITY.md](SECURITY.md) for vulnerability reporting. The SDK supports TLS and mTLS connections — for configuration details, see [How to Connect with TLS](docs/how-to/connect-with-tls.md).

## Additional Resources

- [KubeMQ Documentation](https://docs.kubemq.io/) — Official KubeMQ documentation and guides
- [API Reference](https://kubemq-io.github.io/kubemq-Python/) — Auto-generated API documentation
- [Full Documentation Index](docs/INDEX.md) — Complete SDK documentation index
- [KubeMQ Concepts](docs/CONCEPTS.md) — Core KubeMQ messaging concepts
- [SDK Feature Parity Matrix](../sdk-feature-parity-matrix.md) — Cross-SDK feature comparison
- [CHANGELOG.md](https://github.com/kubemq-io/kubemq-Python/blob/v4/CHANGELOG.md) — Release history
- [TROUBLESHOOTING.md](https://github.com/kubemq-io/kubemq-Python/blob/v4/docs/troubleshooting.md) — Common issues and solutions
- [Examples](https://github.com/kubemq-io/kubemq-Python/tree/v4/examples) — Runnable code examples for all patterns

## Contributing

Contributions are welcome! See [CONTRIBUTING.md](https://github.com/kubemq-io/kubemq-Python/blob/v4/CONTRIBUTING.md)
for development setup, coding standards, and pull request guidelines.

## License

This project is licensed under the MIT License — see the
[LICENSE](https://github.com/kubemq-io/kubemq-Python/blob/v4/LICENSE) file for details.
