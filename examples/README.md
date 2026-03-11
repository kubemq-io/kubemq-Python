# KubeMQ Python SDK Examples

## Prerequisites

- Python 3.11+
- `pip install kubemq`
- KubeMQ server running at `localhost:50000`

Start KubeMQ locally:

```bash
docker run -d -p 8080:8080 -p 50000:50000 kubemq/kubemq-community
```

## Quick Start

| Example | Description | Command |
|---------|-------------|---------|
| [Events](quickstart/events_quickstart.py) | Pub/sub fire-and-forget | `python examples/quickstart/events_quickstart.py` |
| [Queues](quickstart/queues_quickstart.py) | Send/receive with ack | `python examples/quickstart/queues_quickstart.py` |
| [RPC](quickstart/rpc_quickstart.py) | Command request/reply | `python examples/quickstart/rpc_quickstart.py` |

## Events (Pub/Sub)

| Example | Description |
|---------|-------------|
| [events.py](pubsub/events.py) | Basic event publish and subscribe |
| [events_store.py](pubsub/events_store.py) | Persistent events with replay |
| [async.py](pubsub/async.py) | Async events with full feature set |
| [async_simple.py](pubsub/async_simple.py) | Simple async events |
| [wildcard_subscription.py](pubsub/wildcard_subscription.py) | Subscribe with wildcard patterns |
| [multiple_subscribers.py](pubsub/multiple_subscribers.py) | Multiple subscribers, group load balancing |

## Queues

| Example | Description |
|---------|-------------|
| [queues.py](queues/queues.py) | Comprehensive: send, receive, ack, reject, requeue, DLQ, delay, batch |
| [async.py](queues/async.py) | Async queue operations |
| [queues_workers.py](queues/queues_workers.py) | Worker pool processing |
| [waiting_pulled.py](queues/waiting_pulled.py) | Peek waiting messages and pull |
| [stream_upstream.py](queues/stream_upstream.py) | Send messages via streaming |
| [stream_downstream.py](queues/stream_downstream.py) | Receive with stream ack/reject/requeue |

## Commands & Queries (RPC)

| Example | Description |
|---------|-------------|
| [commands.py](cq/commands.py) | Send command, handle response |
| [queries.py](cq/queries.py) | Send query, return data |
| [async.py](cq/async.py) | Async RPC operations |
| [async_simple.py](cq/async_simple.py) | Simple async RPC |

## Configuration

| Example | Description |
|---------|-------------|
| [basic_client.py](client/basic_client.py) | Basic client setup |
| [tls_client.py](client/tls_client.py) | TLS-encrypted connection |
| [auth_client.py](client/auth_client.py) | Token authentication |
| [tls_setup.py](configuration/tls_setup.py) | Detailed TLS configuration |
| [mtls_setup.py](configuration/mtls_setup.py) | Mutual TLS (mTLS) |
| [token_auth.py](configuration/token_auth.py) | Token auth configuration |
| [custom_timeouts.py](configuration/custom_timeouts.py) | Custom timeout settings |

## Channel Management

| Example | Description |
|---------|-------------|
| [create.py](pubsub/create.py) | Create channels |
| [delete.py](pubsub/delete.py) | Delete channels |
| [list.py](pubsub/list.py) | List channels |
