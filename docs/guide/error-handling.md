# Error Handling

## Exception Hierarchy

All SDK exceptions inherit from `KubeMQError`:

```python
from kubemq import (
    KubeMQError,
    KubeMQConnectionError,
    KubeMQAuthenticationError,
    KubeMQTimeoutError,
    KubeMQValidationError,
    KubeMQChannelError,
    KubeMQMessageError,
    KubeMQTransactionError,
)
```

## Handling Errors

```python
from kubemq import (
    PubSubClient, EventMessage,
    KubeMQError, KubeMQConnectionError,
    KubeMQAuthenticationError, KubeMQTimeoutError,
    KubeMQValidationError,
)

try:
    with PubSubClient(address="localhost:50000") as client:
        client.publish_event(
            EventMessage(channel="ch1", body=b"hello")
        )
except KubeMQConnectionError as e:
    print(f"Connection failed: {e}")
except KubeMQAuthenticationError as e:
    print(f"Authentication failed: {e}")
except KubeMQTimeoutError as e:
    print(f"Operation timed out: {e}")
except KubeMQValidationError as e:
    print(f"Invalid message: {e}")
except KubeMQError as e:
    print(f"KubeMQ error: {e}")
```

## Retryable Errors

Exceptions have an `is_retryable` attribute:

```python
try:
    client.publish_event(msg)
except KubeMQError as e:
    if e.is_retryable:
        # Safe to retry (connection errors, timeouts)
        pass
    else:
        # Do not retry (validation errors, auth errors)
        raise
```

| Exception | Retryable | Action |
|-----------|-----------|--------|
| `KubeMQConnectionError` | Yes | Retry with backoff |
| `KubeMQTimeoutError` | Yes | Retry or increase timeout |
| `KubeMQValidationError` | No | Fix the message |
| `KubeMQAuthenticationError` | No | Fix credentials |
| `KubeMQChannelError` | No | Create channel first |

## See Also

- [Troubleshooting](../troubleshooting.md)
- [API Reference — Exceptions](../api/exceptions.md)
