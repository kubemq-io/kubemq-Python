# Configuration

## Client Configuration

All clients accept configuration via constructor keyword arguments or a
`ClientConfig` dataclass.

### Keyword arguments

```python
from kubemq import PubSubClient

client = PubSubClient(
    address="localhost:50000",
    client_id="my-service",
    auth_token="your-token",
    auto_reconnect=True,
    reconnect_interval_seconds=5,
)
```

### ClientConfig object

```python
from kubemq import PubSubClient, ClientConfig, TLSConfig, KeepAliveConfig

config = ClientConfig(
    address="kubemq-server:50000",
    client_id="my-service",
    auth_token="your-token",
    tls=TLSConfig(enabled=True, ca_file="/path/to/ca.pem"),
    keep_alive=KeepAliveConfig(
        enabled=True,
        ping_interval_in_seconds=30,
        ping_timeout_in_seconds=10,
    ),
    auto_reconnect=True,
    reconnect_interval_seconds=5,
)

client = PubSubClient(config=config)
```

## Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `address` | `str` | `"localhost:50000"` | KubeMQ server gRPC address |
| `client_id` | `str` | hostname | Unique client identifier |
| `auth_token` | `str` | `""` | Authentication token |
| `tls` | `TLSConfig` | disabled | TLS configuration |
| `keep_alive` | `KeepAliveConfig` | disabled | Keep-alive ping configuration |
| `max_send_size` | `int` | `104857600` | Max send message size (bytes) |
| `max_receive_size` | `int` | `104857600` | Max receive message size (bytes) |
| `auto_reconnect` | `bool` | `True` | Auto-reconnect on connection loss |
| `reconnect_interval_seconds` | `int` | `1` | Seconds between reconnect attempts |
| `log_level` | `int \| None` | `None` | Python logging level |

## TLS Configuration

See [TLS & Authentication](security.md) for detailed TLS setup.

## See Also

- [API Reference — Config](../api/config.md)
- [TLS & Authentication](security.md)
