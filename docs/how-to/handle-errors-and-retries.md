# How to Handle Errors and Retries

Work with the SDK's structured exception hierarchy to build resilient applications with retry logic.

## Exception Hierarchy

All exceptions inherit from `KubeMQError`:

```
KubeMQError
├── KubeMQConnectionError
│   └── KubeMQAuthenticationError
├── KubeMQTimeoutError
├── KubeMQValidationError
├── KubeMQChannelError
├── KubeMQMessageError
├── KubeMQTransactionError
├── KubeMQConfigurationError
├── KubeMQCircuitOpenError
├── KubeMQBufferFullError
├── KubeMQCancellationError
├── KubeMQClientClosedError
├── KubeMQConnectionNotReadyError
├── KubeMQTransportError
├── KubeMQHandlerError
└── KubeMQStreamBrokenError
```

## KubeMQError Attributes

Every `KubeMQError` has these attributes:

| Attribute | Type | Description |
|-----------|------|-------------|
| `message` | `str` | Human-readable description |
| `code` | `ErrorCode` | Machine-readable enum (`TRANSIENT`, `CONNECTION_TIMEOUT`, `AUTH_FAILED`, etc.) |
| `operation` | `str` | The SDK operation that failed |
| `channel` | `str` | The channel involved, if any |
| `is_retryable` | `bool` | Whether the error is safe to retry |
| `request_id` | `str` | Correlation ID for log tracing |

## Catching Specific Exceptions

Use Python's exception handling to catch errors at the right specificity:

```python
from kubemq import (
    PubSubClient,
    EventMessage,
    KubeMQError,
    KubeMQConnectionError,
    KubeMQAuthenticationError,
    KubeMQTimeoutError,
    KubeMQValidationError,
)


def send_event(client: PubSubClient, channel: str, body: bytes) -> bool:
    try:
        client.publish_event(EventMessage(channel=channel, body=body))
        return True
    except KubeMQAuthenticationError as e:
        print(f"Auth failed (non-retryable): {e.message}")
        return False
    except KubeMQTimeoutError as e:
        print(f"Timeout (retryable={e.is_retryable}): {e.message}")
        return False
    except KubeMQConnectionError as e:
        print(f"Connection error: {e.message}")
        return False
    except KubeMQValidationError as e:
        print(f"Bad request: {e.message}")
        return False
    except KubeMQError as e:
        print(f"KubeMQ error [{e.code}]: {e.message}, retryable={e.is_retryable}")
        return False
```

## Checking is_retryable

The `is_retryable` attribute indicates whether an error is safe to retry. Retryable errors include transient failures, timeouts, and throttling:

```python
from kubemq import KubeMQError

try:
    client.publish_event(EventMessage(channel="demo", body=b"test"))
except KubeMQError as e:
    if e.is_retryable:
        print(f"Retryable error: {e.code} — will retry")
    else:
        print(f"Non-retryable error: {e.code} — fix before retrying")
```

## Custom Retry Logic

Build application-level retry with exponential backoff:

```python
import time
from kubemq import PubSubClient, EventMessage, KubeMQError


def publish_with_retry(
    client: PubSubClient,
    channel: str,
    body: bytes,
    max_attempts: int = 5,
    base_delay: float = 0.1,
) -> None:
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            client.publish_event(EventMessage(channel=channel, body=body))
            return
        except KubeMQError as e:
            last_error = e
            if not e.is_retryable:
                raise

            delay = base_delay * (2 ** (attempt - 1))
            print(f"Attempt {attempt}/{max_attempts} failed: {e.code}, "
                  f"retrying in {delay:.1f}s")
            time.sleep(delay)

    raise RuntimeError(
        f"Exhausted {max_attempts} retries: {last_error}"
    ) from last_error
```

## Reconnection Configuration

Configure automatic reconnection at the client level:

```python
from kubemq import ClientConfig, PubSubClient, KeepAliveConfig

config = ClientConfig(
    address="localhost:50000",
    client_id="resilient-client",
    auto_reconnect=True,
    reconnect_interval_seconds=2,
    max_reconnect_attempts=10,
    reconnect_initial_delay_ms=500,
    reconnect_max_delay_ms=30_000,
    reconnect_backoff_multiplier=2.0,
    keep_alive=KeepAliveConfig(
        enabled=True,
        ping_interval_in_seconds=10,
        ping_timeout_in_seconds=5,
    ),
)

with PubSubClient(config=config) as client:
    info = client.ping()
    print(f"Connected with auto-reconnect: {info.host}")
```

## Connection Error Handling

Detect unreachable servers at startup:

```python
from kubemq import PubSubClient, KubeMQConnectionError

try:
    client = PubSubClient(
        address="localhost:59999",
        client_id="fail-fast-client",
    )
    client.ping()
except KubeMQConnectionError as e:
    print(f"Server unreachable: {e.message}")
    print("Check that KubeMQ is running at the specified address")
```

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `KubeMQAuthenticationError` | Invalid or expired auth token | Refresh the token before retrying |
| `KubeMQValidationError` | Bad channel name or empty body | Fix request parameters — not retryable |
| `KubeMQTimeoutError` | Server didn't respond in time | Increase `timeout_in_seconds` or check connectivity |
| `KubeMQConnectionError` with `is_retryable=True` | Transient network issue | Enable `auto_reconnect` in config |
| `KubeMQClientClosedError` | Operation on a closed client | Create a new client instance |
| `KubeMQBufferFullError` | Reconnection buffer overflow | Wait for reconnection or increase buffer size |
