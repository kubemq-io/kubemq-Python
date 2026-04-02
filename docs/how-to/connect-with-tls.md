# How to Connect with TLS/mTLS

Secure your KubeMQ Python client connections using TLS (server verification) or mTLS (mutual authentication).

## Prerequisites

| File | Purpose | Required for |
|------|---------|-------------|
| `ca.pem` | CA certificate that signed the server cert | TLS and mTLS |
| `client-cert.pem` | Client certificate signed by the CA | mTLS only |
| `client-key.pem` | Client private key | mTLS only |

Certificates must be PEM-encoded. Verify your CA cert matches the server's issuing CA before proceeding.

## TLS — Server Verification Only

The client verifies the server's identity but does not present a certificate of its own.

```python
from kubemq import PubSubClient, TLSConfig, EventMessage

tls_config = TLSConfig(
    enabled=True,
    ca_file="/path/to/ca.pem",
)

with PubSubClient(
    address="kubemq-server:50000",
    client_id="my-tls-client",
    tls=tls_config,
) as client:
    info = client.ping()
    print(f"Connected via TLS to {info.host}")

    client.publish_event(
        EventMessage(
            channel="secure.events",
            body=b"Encrypted message",
        )
    )
```

`TLSConfig(enabled=True, ca_file=...)` configures the gRPC transport to verify the server's certificate chain.

## mTLS — Mutual Authentication

Both the client and server present certificates. The server verifies the client's identity.

```python
from kubemq import PubSubClient, TLSConfig, EventMessage

tls_config = TLSConfig(
    enabled=True,
    cert_file="/path/to/client-cert.pem",
    key_file="/path/to/client-key.pem",
    ca_file="/path/to/ca.pem",
)

with PubSubClient(
    address="kubemq-server:50000",
    client_id="my-mtls-client",
    tls=tls_config,
) as client:
    info = client.ping()
    print(f"Connected via mTLS to {info.host}")

    client.publish_event(
        EventMessage(
            channel="secure.events",
            body=b"mTLS secured message",
        )
    )
```

## TLS with CQ and Queue Clients

`TLSConfig` works the same way with all client types:

```python
from kubemq import CQClient, QueuesClient, TLSConfig

tls = TLSConfig(enabled=True, ca_file="/path/to/ca.pem")

# Command/Query client
with CQClient(address="kubemq-server:50000", client_id="cq-tls", tls=tls) as cq:
    info = cq.ping()
    print(f"CQ client connected via TLS: {info.host}")

# Queues client
with QueuesClient(address="kubemq-server:50000", client_id="q-tls", tls=tls) as q:
    info = q.ping()
    print(f"Queues client connected via TLS: {info.host}")
```

## Combining TLS with Auth Tokens

TLS and token authentication can be layered:

```python
from kubemq import PubSubClient, TLSConfig

with PubSubClient(
    address="kubemq-server:50000",
    client_id="secure-client",
    tls=TLSConfig(enabled=True, ca_file="/path/to/ca.pem"),
    auth_token="your-jwt-token",
) as client:
    info = client.ping()
    print(f"Connected with TLS + auth: {info.host}")
```

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `certificate verify failed` | CA cert doesn't match server's issuing CA | Use the correct CA cert that signed the server certificate |
| `certificate has expired` | Server or client cert past its validity period | Renew the expired certificate |
| `bad handshake` / `CERTIFICATE_REQUIRED` | Server requires mTLS but no client cert provided | Switch from `TLSConfig(ca_file=...)` to include `cert_file` and `key_file` |
| `KubeMQConnectionError` on port 50000 | Server not configured for TLS | Enable TLS in the KubeMQ server configuration |
| `hostname mismatch` | Address doesn't match cert's SAN | Connect using the hostname in the server cert's SAN field |

**Tip:** Test cert validity before connecting:

```bash
openssl x509 -in ca.pem -noout -dates
openssl verify -CAfile ca.pem client-cert.pem
```
