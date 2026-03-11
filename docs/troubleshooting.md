# Troubleshooting Guide

This guide covers common issues when using the KubeMQ Python SDK, with exact
error messages, causes, and step-by-step solutions.

---

## Problem: Cannot connect to KubeMQ server

**Error message:**

```
KubeMQConnectionError: Failed to connect to localhost:50000: Connection refused
```

**Cause:** The KubeMQ server is not running, not reachable at the specified
address, or a firewall is blocking port 50000.

**Solution:**

1. Verify KubeMQ is running:
   ```bash
   docker ps | grep kubemq
   # or check the health endpoint
   curl http://localhost:8080/health
   ```
2. If not running, start it:
   ```bash
   docker run -d -p 8080:8080 -p 50000:50000 kubemq/kubemq-community
   ```
3. If running on a different host/port, update the client address:
   ```python
   client = PubSubClient(address="kubemq-host:50000")
   ```
4. Check firewall rules allow traffic on port 50000.

**See also:** [Configuration](https://github.com/kubemq-io/kubemq-Python#configuration)

---

## Problem: Authentication failed (invalid token)

**Error message:**

```
KubeMQAuthenticationError: Authentication failed: invalid or expired token
```

**Cause:** The `auth_token` provided to the client does not match the token
configured on the KubeMQ server, or the token has expired.

**Solution:**

1. Verify the auth token matches your server configuration:
   ```python
   client = PubSubClient(
       address="localhost:50000",
       auth_token="your-correct-token",
   )
   ```
2. Check the server logs for authentication error details.
3. If using JWT tokens, verify the token has not expired.
4. Ensure the server has authentication enabled — if not, remove
   the `auth_token` parameter.

---

## Problem: Authorization denied (insufficient permissions)

**Error message:**

```
KubeMQAuthenticationError: Permission denied: insufficient permissions for channel 'orders'
```

**Cause:** The authenticated client does not have permission to perform
the requested operation on the specified channel.

**Solution:**

1. Check the KubeMQ server ACL configuration for your token/client.
2. Verify the client has read/write permissions for the target channel.
3. Contact your KubeMQ administrator to update permissions.

---

## Problem: Channel not found

**Error message:**

```
KubeMQChannelError: Channel 'my-channel' not found
```

**Cause:** The specified channel does not exist and auto-create is not
enabled on the server.

**Solution:**

1. Create the channel before using it:
   ```python
   with PubSubClient(address="localhost:50000") as client:
       client.create_events_channel("my-channel")
   ```
2. Or enable auto-create in the KubeMQ server configuration.
3. Verify the channel name is spelled correctly (case-sensitive).

---

## Problem: Message too large

**Error message:**

```
KubeMQValidationError: Message size 5242880 exceeds maximum 4194304 bytes
```

**Cause:** The message body exceeds the maximum message size configured
on the client or server.

**Solution:**

1. Reduce the message size by compressing the payload or splitting
   into smaller messages.
2. Increase the client-side limit:
   ```python
   client = PubSubClient(
       address="localhost:50000",
       max_send_size=10 * 1024 * 1024,  # 10 MB
   )
   ```
3. Also increase the server-side limit if needed (check server config).

---

## Problem: Timeout / deadline exceeded

**Error message:**

```
KubeMQTimeoutError: Operation timed out after 10.0 seconds
```

**Cause:** The operation did not complete within the configured timeout.
This can happen due to network latency, server overload, or large message
processing time.

**Solution:**

1. Increase the timeout for the specific operation:
   ```python
   response = client.send_command(
       CommandMessage(
           channel="my-commands",
           body=b"heavy-operation",
           timeout_in_seconds=60,  # increase from default
       )
   )
   ```
2. Check network latency between client and server.
3. Monitor server resource usage (CPU, memory, disk I/O).
4. Consider breaking large operations into smaller ones.

---

## Problem: Rate limiting / throttling

**Error message:**

```
KubeMQError: Rate limit exceeded: too many requests
```

**Cause:** The client is sending messages faster than the server can
process them, or a rate limit is configured on the server.

**Solution:**

1. Reduce the send rate by adding delays between messages.
2. Use batch operations where available.
3. Check server rate limit configuration.
4. Scale up the KubeMQ server or add replicas.

---

## Problem: Internal server error

**Error message:**

```
KubeMQError: Internal server error (code: INTERNAL)
```

**Cause:** An unexpected error occurred on the KubeMQ server.

**Solution:**

1. Check the KubeMQ server logs for detailed error information.
2. Verify the server is healthy:
   ```bash
   curl http://localhost:8080/health
   ```
3. Restart the server if the error persists.
4. Report the issue with server logs to
   [KubeMQ support](https://github.com/kubemq-io/kubemq-community/issues).

---

## Problem: TLS handshake failure

**Error message:**

```
KubeMQConnectionError: TLS handshake failed: certificate verify failed
```

**Cause:** The TLS certificate presented by the server cannot be verified
by the client. Common reasons: self-signed certificate without proper CA,
expired certificate, hostname mismatch.

**Solution:**

1. Provide the correct CA certificate:
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
2. For mutual TLS (mTLS), also provide client certificate and key:
   ```python
   tls=TLSConfig(
       enabled=True,
       cert_file="/path/to/client-cert.pem",
       key_file="/path/to/client-key.pem",
       ca_file="/path/to/ca.pem",
   )
   ```
3. Verify the certificate is not expired: `openssl x509 -in cert.pem -noout -dates`
4. Verify the hostname matches the certificate CN or SAN.

---

## Problem: Subscriber connected but not receiving messages

**Error message:** None — the subscriber connects successfully but no messages
arrive.

**Cause:** Multiple possible causes, listed from most to least common.

**Solution:**

1. **Timing:** Ensure the subscriber is connected *before* the publisher sends.
   Events are not stored (use Events Store for persistence).
2. **Channel name:** Verify exact channel name match (case-sensitive):
   ```python
   # Publisher
   client.publish_event(EventMessage(channel="orders"))  # ← "orders"
   # Subscriber
   subscription = EventsSubscription(channel="orders")  # ← must match
   ```
3. **Group subscription:** If using groups, only one subscriber per group
   receives each message. Remove the `group` parameter for broadcast.
4. **Enable debug logging:**
   ```python
   import logging
   logging.basicConfig(level=logging.DEBUG)
   client = PubSubClient(address="localhost:50000", log_level=logging.DEBUG)
   ```
5. **Check server dashboard:** Open `http://localhost:8080` to see connected
   clients and active channels.

**See also:** [Events Quick Start](https://github.com/kubemq-io/kubemq-Python/blob/v4/docs/quickstart.md#events-pubsub)

---

## Problem: Queue message not acknowledged

**Error message:**

```
KubeMQTransactionError: Message ack failed: visibility timeout expired
```

**Cause:** The message visibility timeout expired before the consumer
acknowledged the message. The message becomes visible to other consumers again.

**Solution:**

1. Increase the visibility timeout when receiving messages:
   ```python
   response = client.receive_queue_messages(
       channel="my-queue",
       max_messages=1,
       wait_timeout_in_seconds=10,
       visibility_seconds=120,  # 2 minutes to process
   )
   ```
2. Acknowledge messages as soon as possible after processing:
   ```python
   for msg in response.messages:
       process(msg)
       msg.ack()  # ack immediately after processing
   ```
3. If processing takes variable time, consider extending the visibility
   timeout before it expires.

---

## General Debugging Tips

### Enable verbose logging

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Enable asyncio debug mode

```python
import asyncio
asyncio.get_event_loop().set_debug(True)
```

### Check SDK version

```python
import kubemq
print(kubemq.__version__)
```

### Verify server connectivity

```python
from kubemq import PubSubClient

with PubSubClient(address="localhost:50000") as client:
    info = client.ping()
    print(f"Server: {info.host}, Version: {info.version}")
```

### Check server health via HTTP

```bash
curl http://localhost:8080/health
curl http://localhost:8080/metrics
```

### Run with deprecation warnings visible

```bash
python -Wd your_script.py
```

This helps identify any v3 API usage that should be migrated to v4.
