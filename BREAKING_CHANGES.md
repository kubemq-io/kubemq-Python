# Breaking Changes: v3.x to v4.0

This document tracks all breaking changes planned for the KubeMQ Python SDK v4.0 release.

## Overview

v4.0 is a major release that includes architectural improvements, bug fixes, and new features.
While we strive to maintain backward compatibility where possible, some breaking changes are necessary
to deliver a production-grade SDK.

---

## Import Path Changes

### Module Structure
```python
# v3.x - Current
from kubemq.pubsub.client import Client
from kubemq.queues.client import Client
from kubemq.cq.client import Client

# v4.0 - New (preferred)
from kubemq.pubsub import Client, AsyncClient
from kubemq.queues import Client, AsyncClient
from kubemq.cq import Client, AsyncClient
```

**Note:** The old import paths will continue to work but are deprecated.

---

## Exception Renames

To avoid shadowing Python built-in exceptions:

| v3.x | v4.0 |
|------|------|
| `ConnectionError` | `KubeMQConnectionError` |
| `BaseError` | `KubeMQError` |

### Migration Example
```python
# v3.x
from kubemq.common.exceptions import ConnectionError, ValidationError

try:
    client.ping()
except ConnectionError as e:
    handle_error(e)

# v4.0
from kubemq.core.exceptions import KubeMQConnectionError, KubeMQValidationError

try:
    client.ping()
except KubeMQConnectionError as e:
    handle_error(e)
```

**Note:** The old exception names will be available as deprecated aliases during the v4.x cycle.

---

## Client Initialization Changes

### TLS Configuration
```python
# v3.x
client = Client(
    address="localhost:50000",
    tls=True,
    tls_cert_file="/path/to/cert.pem",
    tls_key_file="/path/to/key.pem",
    tls_ca_file="/path/to/ca.pem",
)

# v4.0
from kubemq.core.config import TLSConfig
from pathlib import Path

client = Client(
    address="localhost:50000",
    tls=TLSConfig(
        enabled=True,
        cert_file=Path("/path/to/cert.pem"),
        key_file=Path("/path/to/key.pem"),
        ca_file=Path("/path/to/ca.pem"),
    ),
)
```

### Keep-Alive Configuration
```python
# v3.x
client = Client(
    address="localhost:50000",
    keep_alive=True,
    ping_interval_in_seconds=30,
    ping_timeout_in_seconds=10,
)

# v4.0
from kubemq.core.config import KeepAliveConfig

client = Client(
    address="localhost:50000",
    keep_alive=KeepAliveConfig(
        enabled=True,
        ping_interval_seconds=30,  # Note: renamed from ping_interval_in_seconds
        ping_timeout_seconds=10,   # Note: renamed from ping_timeout_in_seconds
    ),
)
```

---

## Async API Changes

### Dedicated Async Clients
```python
# v3.x - Thread-wrapped async
client = Client(address="localhost:50000")
result = await client.ping_async()

# v4.0 - Native async with dedicated client
async with AsyncClient(address="localhost:50000") as client:
    result = await client.ping()
```

### Async Method Names
In v4.0, async clients use the same method names as sync clients (without `_async` suffix):

| v3.x | v4.0 AsyncClient |
|------|------------------|
| `client.ping_async()` | `client.ping()` |
| `client.send_events_message_async()` | `client.send_event()` |
| `client.close_async()` | `client.close()` |

---

## Method Signature Changes

### PubSub Client
```python
# v3.x
client.send_events_message(message)
client.send_events_store_message(message)

# v4.0
client.send_event(message)
client.send_event_store(message)
```

### Return Type Fixes
```python
# v3.x - Incorrect type annotation
def create_events_channel(self, channel: str) -> [bool, None]:  # Invalid syntax

# v4.0 - Correct type annotation
def create_events_channel(self, channel: str) -> Optional[bool]:
```

---

## Removed Features

- None planned

---

## New Required Dependencies

- `typing_extensions>=4.0.0` (for Python < 3.10)

---

## Configuration Changes

### Environment Variables
New environment variable prefix pattern:
```bash
# v3.x - Various patterns
# (no standardized pattern)

# v4.0 - Standardized KUBEMQ_ prefix
KUBEMQ_ADDRESS=localhost:50000
KUBEMQ_CLIENT_ID=my-service
KUBEMQ_AUTH_TOKEN=secret
KUBEMQ_TLS_ENABLED=true
KUBEMQ_TLS_CERT=/path/to/cert.pem
```

### Config File Support (New)
```toml
# kubemq.toml (new in v4.0)
address = "localhost:50000"
client_id = "my-service"
auth_token = "secret"

[tls]
enabled = true
cert_file = "/path/to/cert.pem"
key_file = "/path/to/key.pem"
```

---

## Bug Fixes That May Affect Behavior

### 1. Keep-Alive Configuration Fix
**Issue:** `ping_interval` and `ping_timeout` were swapped in v3.x.
**Impact:** If you configured keep-alive with specific values, the actual behavior will change to match the intended configuration.

### 2. Thread Safety Fix
**Issue:** Lazy initialization of senders was not thread-safe.
**Impact:** Applications using the same client from multiple threads may see different behavior (now correctly synchronized).

### 3. Subscription Encoding Consistency
**Issue:** Some subscriptions used `encode()` while others used `decode()` for the same operation.
**Impact:** Internal change, no user-facing impact expected.

---

## Migration Checklist

- [ ] Update import statements
- [ ] Rename exception catches
- [ ] Update TLS configuration if used
- [ ] Update keep-alive configuration if used
- [ ] Switch to AsyncClient for async applications
- [ ] Update method names (`send_events_message` → `send_event`)
- [ ] Run characterization tests to verify behavior
- [ ] Update type annotations in your code

---

## Deprecation Timeline

| Feature | Deprecated In | Removed In |
|---------|--------------|------------|
| Old import paths | v4.0 | v5.0 |
| Old exception names | v4.0 | v5.0 |
| `send_events_message` | v4.0 | v5.0 |
| `_async` suffix methods on sync client | v4.0 | v5.0 |

---

## Getting Help

If you encounter issues migrating to v4.0:

1. Check the [Migration Guide](docs/migration-v4.md)
2. Review the [CHANGELOG](CHANGELOG.md)
3. Open an issue on [GitHub](https://github.com/kubemq-io/kubemq-Python/issues)
