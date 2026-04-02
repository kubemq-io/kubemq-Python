# Migration Guide: v3 → v4

This guide covers all breaking changes between KubeMQ Python SDK v3 and v4,
with before/after code examples and a step-by-step upgrade procedure.

## Table of Contents

1. [Breaking Changes Summary](#breaking-changes-summary)
2. [Step-by-Step Upgrade Procedure](#step-by-step-upgrade-procedure)
3. [Import Path Changes](#import-path-changes)
4. [Client Constructor Changes](#client-constructor-changes)
5. [TLS Configuration Changes](#tls-configuration-changes)
6. [Async API Changes](#async-api-changes)
7. [Message Model Changes](#message-model-changes)
8. [Error Handling Changes](#error-handling-changes)
9. [Build System Changes](#build-system-changes)
10. [Compatibility Notes](#compatibility-notes)

---

## Breaking Changes Summary

| # | Change | v3 | v4 | Impact |
|---|--------|----|----|--------|
| 1 | Python version | 3.8+ | 3.11+ | Update runtime/CI |
| 2 | Import paths | `from kubemq.pubsub import Client` | `from kubemq import PubSubClient` | Update all imports |
| 3 | Client class names | `Client` (per module) | `PubSubClient`, `QueuesClient`, `CQClient` | Rename all references |
| 4 | TLS config | `tls=True, tls_cert_file="..."` | `tls=TLSConfig(enabled=True, cert_file="...")` | Update TLS setup |
| 5 | Async API | `client.send_events_message_async()` | `await async_client.publish_event()` | Use dedicated async clients |
| 6 | Error types | `grpc.RpcError` | `KubeMQConnectionError`, etc. | Update except clauses |
| 7 | Build system | setuptools | hatchling | Update build scripts |
| 8 | Constructor params | Flat string params | Keyword args + dataclasses | Update client creation |

---

## Step-by-Step Upgrade Procedure

### 1. Update Python version

Ensure you are running Python 3.11+:

```bash
python --version  # Must be 3.11 or higher
```

### 2. Update the package

```bash
pip install --upgrade kubemq
```

### 3. Update imports

Find and replace all imports:

| Find | Replace |
|------|---------|
| `from kubemq.pubsub import Client` | `from kubemq import PubSubClient` |
| `from kubemq.queues import Client` | `from kubemq import QueuesClient` |
| `from kubemq.cq import Client` | `from kubemq import CQClient` |
| `Client(address=` | `PubSubClient(address=` (or `QueuesClient`/`CQClient`) |

### 4. Update method names to new verbs

| Old Method (deprecated) | New Method (preferred) |
|------------------------|----------------------|
| `send_events_message()` | `publish_event()` |
| `send_events_store_message()` | `publish_event_store()` |
| `send_queues_message()` | `send_queue_message()` |
| `receive_queues_messages()` | `receive_queue_messages()` |
| `send_command_request()` | `send_command()` |
| `send_query_request()` | `send_query()` |

### 5. Update TLS configuration

Replace flat TLS parameters with `TLSConfig`:

```python
# Before
tls=True, tls_cert_file="cert.pem"
# After
tls=TLSConfig(enabled=True, cert_file="cert.pem")
```

### 6. Update keep-alive configuration

Replace flat keep-alive parameters with `KeepAliveConfig`:

```python
# Before
keep_alive=True, ping_interval_in_seconds=30
# After
keep_alive=KeepAliveConfig(enabled=True, ping_interval_in_seconds=30)
```

### 7. Update error handling

Replace `grpc.RpcError` catches with typed exceptions:

```python
# Before
except grpc.RpcError as e: ...
# After
except KubeMQError as e: ...
```

### 8. Update async code (if applicable)

Replace `*_async()` methods with dedicated async clients:

```python
# Before
client = Client(address="localhost:50000")
await client.send_events_message_async(msg)
# After
async with AsyncPubSubClient(address="localhost:50000") as client:
    await client.publish_event(msg)
```

### 9. Test

Run your test suite to verify all changes work correctly:

```bash
python -Wd -m pytest  # -Wd shows deprecation warnings
```

---

## Import Path Changes

### Before (v3)

```python
from kubemq.pubsub import Client, EventMessage, EventsSubscription
from kubemq.pubsub import CancellationToken
from kubemq.queues import Client as QueuesClient
from kubemq.cq import Client as CQClient
```

### After (v4)

```python
from kubemq import PubSubClient, EventMessage, EventsSubscription
from kubemq import CancellationToken
from kubemq import QueuesClient
from kubemq import CQClient
```

> **Note:** The v4 imports from `kubemq.pubsub import Client` still work (backward
> compatible), but the recommended import is from the top-level `kubemq` package.

---

## Client Constructor Changes

### Before (v3)

```python
import logging

client = Client(
    address="localhost:50000",
    client_id="my-client",
    auth_token="token",
    tls=True,
    tls_cert_file="/path/to/cert.pem",
    tls_key_file="/path/to/key.pem",
    tls_ca_file="/path/to/ca.pem",
    max_send_size=1048576,
    disable_auto_reconnect=False,
    reconnect_interval_seconds=5,
    keep_alive=True,
    ping_interval_in_seconds=30,
    ping_timeout_in_seconds=10,
    log_level=logging.DEBUG,
)
```

### After (v4)

```python
import logging
from kubemq import PubSubClient, TLSConfig, KeepAliveConfig

client = PubSubClient(
    address="localhost:50000",
    client_id="my-client",
    auth_token="token",
    tls=TLSConfig(
        enabled=True,
        cert_file="/path/to/cert.pem",
        key_file="/path/to/key.pem",
        ca_file="/path/to/ca.pem",
    ),
    max_send_size=1048576,
    # Note: disable_auto_reconnect is now auto_reconnect (inverted)
    disable_auto_reconnect=False,  # legacy param still works
    keep_alive=True,               # legacy param still works
    ping_interval_in_seconds=30,   # legacy param still works
    ping_timeout_in_seconds=10,    # legacy param still works
    log_level=logging.DEBUG,
)
```

### Key differences

- `tls` changed from `bool` to `TLSConfig` dataclass
- `tls_cert_file`, `tls_key_file`, `tls_ca_file` moved into `TLSConfig`
- Legacy constructor parameters still work for backward compatibility
- `ClientConfig` dataclass available for centralized configuration

---

## TLS Configuration Changes

### Before (v3)

```python
client = Client(
    address="localhost:50000",
    tls=True,
    tls_cert_file="/path/to/cert.pem",
    tls_key_file="/path/to/key.pem",
    tls_ca_file="/path/to/ca.pem",
)
```

### After (v4)

```python
from kubemq import PubSubClient, TLSConfig

client = PubSubClient(
    address="localhost:50000",
    tls=TLSConfig(
        enabled=True,
        cert_file="/path/to/cert.pem",
        key_file="/path/to/key.pem",
        ca_file="/path/to/ca.pem",
    ),
)
```

---

## Async API Changes

### Before (v3) — thread-based async wrappers

```python
from kubemq.pubsub import Client, EventMessage

client = Client(address="localhost:50000")

# Thread-based async wrapper (not truly async)
await client.send_events_message_async(
    EventMessage(channel="ch1", body=b"hello")
)
result = await client.ping_async()
```

### After (v4) — native async clients

```python
from kubemq import AsyncPubSubClient, EventMessage

async with AsyncPubSubClient(address="localhost:50000") as client:
    # Native async — proper asyncio integration
    await client.publish_event(
        EventMessage(channel="ch1", body=b"hello")
    )
    result = await client.ping()
```

### Key differences

- Dedicated `AsyncPubSubClient`, `AsyncQueuesClient`, `AsyncCQClient` classes
- Use `async with` context manager for lifecycle management
- New verb names: `publish_event()`, `send_command()`, `send_query()`
- Native `asyncio`/`grpc.aio` — no thread pool overhead
- The sync `Client.*_async()` methods still work but are deprecated

---

## Message Model Changes

The message models (`EventMessage`, `QueueMessage`, etc.) have the same field
names and types in v3 and v4. Code that constructs messages via keyword arguments
will work unchanged:

```python
# Works in both v3 and v4
msg = EventMessage(
    channel="my-channel",
    body=b"Hello!",
    metadata="context",
    tags={"key": "value"},
)
```

The main difference is that v4 models use Pydantic v2 `BaseModel` with
validation, so invalid messages raise `KubeMQValidationError` at construction
time rather than at send time.

---

## Error Handling Changes

### Before (v3)

```python
import grpc

try:
    client.send_events_message(msg)
except grpc.RpcError as e:
    if e.code() == grpc.StatusCode.UNAVAILABLE:
        print("Server unavailable")
    elif e.code() == grpc.StatusCode.UNAUTHENTICATED:
        print("Auth failed")
    else:
        print(f"gRPC error: {e.details()}")
except Exception as e:
    print(f"Unknown error: {e}")
```

### After (v4)

```python
from kubemq import (
    KubeMQError, KubeMQConnectionError,
    KubeMQAuthenticationError, KubeMQTimeoutError,
)

try:
    client.publish_event(msg)
except KubeMQConnectionError as e:
    print(f"Connection failed: {e}")
except KubeMQAuthenticationError as e:
    print(f"Auth failed: {e}")
except KubeMQTimeoutError as e:
    print(f"Timeout: {e}")
except KubeMQError as e:
    print(f"KubeMQ error: {e}")
```

### Key differences

- No need to import `grpc` or check `grpc.StatusCode`
- Typed exception hierarchy with semantic meaning
- Each exception carries structured error information
- Original gRPC error preserved via exception chaining (`__cause__`)
- `is_retryable` attribute on exceptions for retry logic

---

## Build System Changes

### Before (v3)

```bash
# setup.py / setup.cfg based
python setup.py install
python setup.py sdist bdist_wheel
```

### After (v4)

```bash
# pyproject.toml / hatchling based
pip install .
python -m build
```

No action needed if you install via `pip install kubemq`.

---

## Compatibility Notes

### Backward-compatible imports

The v3-style imports from submodules (`from kubemq.pubsub import Client`) still
work in v4. The `Client` class in each submodule is the same class exported as
`PubSubClient`/`QueuesClient`/`CQClient` from the top level. However, these
submodule imports are considered internal and may be removed in a future version.

### Deprecated sync async wrappers

The `*_async()` methods on sync clients (`send_events_message_async`, etc.)
still work in v4 but are deprecated. They use thread-based wrappers which are
less efficient than the dedicated async clients. They will be removed in v5.

| Deprecated Method | Replacement |
|------------------|-------------|
| `PubSubClient.ping_async()` | `AsyncPubSubClient.ping()` |
| `PubSubClient.send_events_message_async()` | `AsyncPubSubClient.publish_event()` |
| `PubSubClient.send_events_store_message_async()` | `AsyncPubSubClient.publish_event_store()` |
| `QueuesClient.ping_async()` | `AsyncQueuesClient.ping()` |
| `QueuesClient.send_queues_message_async()` | `AsyncQueuesClient.send_queue_message()` |
| `CQClient.ping_async()` | `AsyncCQClient.ping()` |
| `CQClient.send_command_request_async()` | `AsyncCQClient.send_command()` |
| `CQClient.send_query_request_async()` | `AsyncCQClient.send_query()` |
| `CQClient.send_response_message_async()` | `AsyncCQClient.send_response()` |

### Deprecated method names

The old method names still work but emit deprecation warnings:

| Deprecated Method | Replacement |
|------------------|-------------|
| `send_events_message()` | `publish_event()` |
| `send_events_store_message()` | `publish_event_store()` |
| `send_queues_message()` | `send_queue_message()` |
| `receive_queues_messages()` | `receive_queue_messages()` |
| `send_command_request()` | `send_command()` |
| `send_query_request()` | `send_query()` |

---

## See Also

- [CHANGELOG](https://github.com/kubemq-io/kubemq-Python/blob/v4/CHANGELOG.md)
- [Quick Start Guide](https://github.com/kubemq-io/kubemq-Python/blob/v4/docs/quickstart.md)
- [Troubleshooting Guide](https://github.com/kubemq-io/kubemq-Python/blob/v4/docs/troubleshooting.md)
