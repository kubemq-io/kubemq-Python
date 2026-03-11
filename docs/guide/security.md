# TLS & Authentication

## TLS (Server Certificate Verification)

```python
from kubemq import PubSubClient, TLSConfig

client = PubSubClient(
    address="kubemq-server:50000",
    tls=TLSConfig(
        enabled=True,
        ca_file="/path/to/ca.pem",
    ),
)
```

## Mutual TLS (mTLS)

Both client and server authenticate each other:

```python
from kubemq import PubSubClient, TLSConfig

client = PubSubClient(
    address="kubemq-server:50000",
    tls=TLSConfig(
        enabled=True,
        cert_file="/path/to/client-cert.pem",
        key_file="/path/to/client-key.pem",
        ca_file="/path/to/ca.pem",
    ),
)
```

## Token Authentication

```python
from kubemq import PubSubClient

client = PubSubClient(
    address="kubemq-server:50000",
    auth_token="your-authentication-token",
)
```

## Combining TLS and Authentication

```python
from kubemq import PubSubClient, TLSConfig

client = PubSubClient(
    address="kubemq-server:50000",
    auth_token="your-token",
    tls=TLSConfig(
        enabled=True,
        cert_file="/path/to/client-cert.pem",
        key_file="/path/to/client-key.pem",
        ca_file="/path/to/ca.pem",
    ),
)
```

## See Also

- [Configuration](configuration.md)
- [Troubleshooting — TLS handshake failure](../troubleshooting.md#problem-tls-handshake-failure)
- [Examples — TLS setup](https://github.com/kubemq-io/kubemq-Python/tree/v4/examples/configuration)
