# KubeMQ Python SDK — Performance Guide

## Architecture Overview

The KubeMQ Python SDK uses gRPC (HTTP/2) for all server communication. A single
client instance maintains one long-lived gRPC channel that multiplexes all operations.

### Key Properties

- **Single channel per client:** All operations (publish, subscribe, queue send/receive)
  share one gRPC channel. HTTP/2 multiplexing handles concurrency.
- **Fire-and-forget events:** Uses bidirectional gRPC streaming (`SendEventsStream`).
  Events are queued internally and sent asynchronously.
- **Event store / commands / queries:** Unary gRPC calls with synchronous response.
- **Queue operations:** Bidirectional streaming for send; unary for batch send.

## Throughput Characteristics

See [BENCHMARK_BASELINE.md](BENCHMARK_BASELINE.md) for measured numbers.

| Operation | Expected Throughput | Notes |
|-----------|-------------------|-------|
| Event publish (fire-and-forget) | High (thousands/sec) | Async streaming, no ack wait |
| Event store publish | Moderate | Waits for server confirmation |
| Queue send | Moderate | Unary call per message |
| Queue batch send | High | Single call for entire batch |

## Tuning Parameters

| Parameter | Default | Location | Effect |
|-----------|---------|----------|--------|
| `max_send_size` | 104857600 (100MB) | `ClientConfig` | Max gRPC message size for sends |
| `max_receive_size` | 104857600 (100MB) | `ClientConfig` | Max gRPC message size for receives |
| `default_timeout_seconds` | 10 | `ClientConfig` | Default timeout for unary RPCs |
| `reconnect_interval_seconds` | 5 | `ClientConfig` | Delay between reconnection attempts |
| `max_send_queue_size` | 10,000 | `ClientConfig` | Internal send queue depth; raise for bursty workloads |
| Batch size | User-controlled | Input list length | Larger batches = fewer RPCs |
| Semaphore concurrency | 100 | `max_concurrent` param | Max concurrent async sends |

## Known Limitations

| Limitation | Value | Notes |
|-----------|-------|-------|
| Max message size | 100MB default | Configurable via `max_send_size` / `max_receive_size` |
| Max concurrent streams | gRPC default (100) | HTTP/2 concurrent stream limit per channel |
| Internal send queue | 10,000 messages | Fire-and-forget events buffered before send; configurable via `max_send_queue_size` |
| Python GIL | Single-threaded CPU | gRPC C-extension releases GIL during I/O |

## Python-Specific Considerations

### Async vs Sync

- **Async clients** (`AsyncPubSubClient`, `AsyncQueuesClient`, `AsyncCQClient`) use
  `grpc.aio` natively — best for I/O-bound workloads and high concurrency.
- **Sync clients** use threads internally for streaming operations. Suitable for
  simple scripts and synchronous application frameworks.
- For maximum throughput, use async clients with `asyncio`.

### GIL and gRPC

The gRPC C-extension (`grpcio`) releases the GIL during network I/O. This means:
- Multiple threads CAN perform gRPC calls concurrently.
- CPU-bound message processing in Python IS subject to the GIL.
- For CPU-heavy message processing, consider `multiprocessing` or offloading to workers.

### Event Loop

- Do NOT block the asyncio event loop in subscription callbacks.
- Long-running message processing should be offloaded to a thread pool:
  `await asyncio.get_running_loop().run_in_executor(None, heavy_processing, msg)`

## Performance Tips

### 1. Reuse Client Instances

**Do NOT** create a new client for every operation. Each client establishes a gRPC
channel with connection overhead. Create one client and reuse it for the lifetime of
your application.

```python
# GOOD — create once, reuse
client = PubSubClient(address="localhost:50000")
for msg in messages:
    client.send_event(msg)
client.close()

# BAD — creates a new connection per message
for msg in messages:
    client = PubSubClient(address="localhost:50000")
    client.send_event(msg)
    client.close()
```

### 2. Use Batching for High-Throughput Queue Sends

For queue operations, batch multiple messages into a single call
instead of sending them individually.

```python
# GOOD — single RPC for all messages
results = client.send_queue_messages([msg1, msg2, msg3, ...])

# LESS OPTIMAL — one RPC per message
for msg in messages:
    client.send_queue_message(msg)
```

### 3. Do Not Block Subscription Callbacks

Subscription callbacks run on the gRPC event loop (async) or a dedicated thread (sync).
Blocking the callback delays processing of subsequent messages.

```python
# GOOD — offload heavy work
async def on_event(event):
    await asyncio.get_running_loop().run_in_executor(None, process_heavy, event)

# BAD — blocks the event loop
async def on_event(event):
    time.sleep(5)  # Blocks all message processing
    process(event)
```

### 4. Close Streams When Done

Always close clients when finished to release gRPC channels and threads.
Use context managers for automatic cleanup.

```python
# GOOD — automatic cleanup
async with AsyncPubSubClient(address="localhost:50000") as client:
    await client.send_event(msg)

# GOOD — explicit cleanup
client = PubSubClient(address="localhost:50000")
try:
    client.send_event(msg)
finally:
    client.close()
```

### 5. Use Async for I/O-Bound Workloads (Python-Specific)

Async clients avoid thread overhead and scale better for concurrent I/O.
Use a semaphore to limit concurrency and avoid unbounded task creation:

```python
# Best for high-concurrency scenarios — throttle with semaphore
async with AsyncPubSubClient(address="localhost:50000") as client:
    sem = asyncio.Semaphore(50)  # limit to 50 concurrent sends

    async def _send_throttled(msg):
        async with sem:
            await client.send_event(msg)

    tasks = [asyncio.create_task(_send_throttled(msg)) for msg in messages]
    await asyncio.gather(*tasks)
```

### 6. GIL Awareness (Python-Specific)

The gRPC C-extension releases the GIL during network I/O, so multiple threads CAN
perform gRPC operations concurrently. However, Python-side message construction
and processing is still subject to the GIL. For CPU-heavy workloads, consider
`multiprocessing`.

### 7. Use `async with` for Automatic Cleanup (Python-Specific)

All async clients support the async context manager protocol. This ensures
proper cleanup even if exceptions occur:

```python
async with AsyncQueuesClient(address="localhost:50000") as client:
    # client is automatically closed on exit
    pass
```
