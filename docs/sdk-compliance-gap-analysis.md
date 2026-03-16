# KubeMQ Python SDK — Deep Compliance Gap Analysis

**Reference:** [sdk-api-feature-list.md](../sdk-api-feature-list.md) | [sdk-compliance-checklist.md](../sdk-compliance-checklist.md)
**Date:** 2026-03-13
**SDK Branch:** v4
**Overall Compliance:** 86.2% (169/196 items fully implemented)

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Architecture Overview & Structural Gaps](#2-architecture-overview--structural-gaps)
3. [GAP-01: Ping Does Not Bypass Authentication](#3-gap-01-ping-does-not-bypass-authentication)
4. [GAP-02: Async Client Missing Bidirectional SendEventsStream](#4-gap-02-async-client-missing-bidirectional-sendeventsstream)
5. [GAP-03: Async Client Missing Bidirectional QueuesUpstream Stream](#5-gap-03-async-client-missing-bidirectional-queuesupstream-stream)
6. [GAP-04: OpenTelemetry Span Field Not Set on Protobuf](#6-gap-04-opentelemetry-span-field-not-set-on-protobuf)
7. [GAP-05: maxReceiveSize Default Mismatch (100MB vs 4MB)](#7-gap-05-maxreceivesize-default-mismatch-100mb-vs-4mb)
8. [GAP-06: No Minimum 5-Second Keepalive Floor](#8-gap-06-no-minimum-5-second-keepalive-floor)
9. [GAP-07: Sync Client Missing send_queue_messages_batch()](#9-gap-07-sync-client-missing-send_queue_messages_batch)
10. [GAP-08: Async Client Per-Message Ack/Reject Broken](#10-gap-08-async-client-per-message-ackreject-broken)
11. [GAP-09: List Channels Retry Inconsistency](#11-gap-09-list-channels-retry-inconsistency)
12. [GAP-10: Queue Validation Missing Upper Bounds](#12-gap-10-queue-validation-missing-upper-bounds)
13. [GAP-11: EventsStoreTypeData Not Enforced as Undefined for Non-Store](#13-gap-11-eventsstoretypedata-not-enforced-as-undefined-for-non-store)
14. [GAP-12: Downstream Request/Response Metadata Field Not Mapped](#14-gap-12-downstream-requestresponse-metadata-field-not-mapped)
15. [GAP-13: Async Client Missing Channel Management Methods](#15-gap-13-async-client-missing-channel-management-methods)
16. [GAP-14: Queue Info Returns Raw Protobuf](#16-gap-14-queue-info-returns-raw-protobuf)
17. [GAP-15: Missing Code Examples](#17-gap-15-missing-code-examples)
18. [GAP-16: Sync-Async Feature Parity Matrix](#18-gap-16-sync-async-feature-parity-matrix)
19. [GAP-17: Async Path Missing gRPC Keepalive Safety Options](#19-gap-17-async-path-missing-grpc-keepalive-safety-options)
20. [GAP-18: RequestID Validation for SendResponse](#20-gap-18-requestid-validation-for-sendresponse)
21. [Minor Issues & Observations](#21-minor-issues--observations)
22. [Prioritized Remediation Plan](#22-prioritized-remediation-plan)

---

## 1. Executive Summary

The Python SDK is **broadly compliant** with the KubeMQ API feature list. The core messaging patterns (Events, Events Store, Queues, Commands, Queries) all work correctly for the sync client. However, the deep analysis uncovered **significant structural gaps** stemming from an asymmetric dual-architecture design: the sync client uses bidirectional gRPC streams (as the spec requires), while the async client falls back to simpler unary RPCs. This creates a fundamental feature-parity problem.

### Critical Findings

| Severity | Count | Description |
|----------|:-----:|-------------|
| **Blocking** | 4 | Gaps that violate the spec or break expected behavior |
| **High** | 5 | Missing features that impact correctness or completeness |
| **Medium** | 6 | Feature parity issues, validation gaps, DX concerns |
| **Low** | 3 | Minor issues, cosmetic, non-functional |

### Impact Summary

```
Sync client compliance:  ~93% (well-aligned with spec)
Async client compliance: ~78% (significant gaps in streaming, channel mgmt)
Combined compliance:     ~86% (169/196 items)
```

---

## 2. Architecture Overview & Structural Gaps

The SDK has a dual-layer architecture that is the root cause of most gaps:

```
┌──────────────────────┐  ┌───────────────────────┐
│   Sync Client        │  │   Async Client         │
│   (Thread-based)     │  │   (Native grpc.aio)    │
├──────────────────────┤  ├───────────────────────┤
│ EventSender          │  │ (no stream sender)     │ ← GAP-02
│ UpstreamSender       │  │ (uses unary RPC)       │ ← GAP-03
│ DownstreamReceiver   │  │ (uses unary + partial) │ ← GAP-08
│ Channel mgmt methods │  │ (missing)              │ ← GAP-13
│ No RetryExecutor     │  │ RetryExecutor          │
│ No batch send        │  │ Batch send             │ ← GAP-07
├──────────────────────┤  ├───────────────────────┤
│ SyncTransport        │  │ AsyncTransport         │
│ ChannelManager       │  │ ReconnectionManager    │
│ Simple reconnect     │  │ Exponential backoff    │
└──────────────────────┘  └───────────────────────┘
```

**Root cause:** The async client was built as a "Phase 4" native async implementation on top of `AsyncTransport`, but the bidirectional stream managers (`EventSender`, `UpstreamSender`, `DownstreamReceiver`) were never ported from the sync path. Instead, the async client uses simpler unary RPCs where possible.

---

## 3. GAP-01: Ping Does Not Bypass Authentication

| Attribute | Value |
|-----------|-------|
| **Severity** | Blocking |
| **Spec Reference** | §1.2 |
| **Affected Clients** | Both sync and async |

### Requirement

> This method **bypasses authentication** — it can be called without an auth token, making it suitable for health checks.

### Current Behavior

Both `SyncTransport.ping()` and `AsyncTransport.ping()` make gRPC calls through the standard stub, which is wrapped in auth interceptors. Every call — including `Ping` — gets the `"authorization"` metadata header injected.

If the SDK is configured with an invalid or expired token, `Ping` will fail with `UNAUTHENTICATED`, even though the server explicitly skips authentication for Ping.

### Impact

- Health check endpoints cannot be used before authentication is established
- Token rotation health probes fail during the rotation window
- Load balancer health checks require valid tokens

### Root Cause

The auth interceptors in `interceptors.py` inject the token on ALL calls unconditionally:

```python
# interceptors.py:22-28
def _inject_auth_metadata(metadata, token_holder):
    token = token_holder.token
    if not token or not token.strip():
        return metadata or ()
    existing = list(metadata or [])
    existing.append(("authorization", token))
    return tuple(existing)
```

There is no exclusion list for specific RPC methods.

### Recommended Fix

Option A: Create a separate gRPC channel/stub without auth interceptors for Ping only.
Option B: Add method-name inspection in the interceptor to skip auth for the `Ping` method.
Option C: Create the Ping stub before interceptors are applied and keep a reference.

---

## 4. GAP-02: Async Client Missing Bidirectional SendEventsStream

| Attribute | Value |
|-----------|-------|
| **Severity** | Blocking |
| **Spec Reference** | §2.2, §3.2 |
| **Affected Clients** | Async only |

### Requirement

> Open a persistent bidirectional stream for high-throughput event publishing. Client sends `Event` messages on the stream. Stream stays open until client or server closes it. SDK should handle stream reconnection on disconnect.

### Current Behavior

- **Sync client**: Fully implements `SendEventsStream` via `EventSender` class with a background daemon thread, internal queue, response tracking, and auto-reconnection.
- **Async client**: Uses the unary `SendEvent` RPC for every message individually. No persistent stream. No message batching through the stream.

### Impact

- **Throughput**: Each event requires a full gRPC round-trip (connect → send → receive → done). A bidirectional stream amortizes connection overhead across thousands of messages.
- **Events Store confirmation**: Unary calls work correctly (server returns Result for each), but they're significantly slower than streaming for high-volume scenarios.
- **Behavioral divergence**: Sync and async clients have fundamentally different performance characteristics for the same logical operation.

### Root Cause

`AsyncTransport` exposes `send_event()` as a unary call (`self._stub.SendEvent(event)` at line 545 of `async_transport.py`). There is no async equivalent of the `EventSender` class.

### Recommended Fix

Implement an `AsyncEventSender` that:
1. Opens `grpc.aio.StreamStreamCall` via `stub.SendEventsStream()`
2. Uses an `asyncio.Queue` for message buffering
3. Runs a background `asyncio.Task` for reading responses
4. Handles stream reconnection via the existing `ReconnectionManager`

---

## 5. GAP-03: Async Client Missing Bidirectional QueuesUpstream Stream

| Attribute | Value |
|-----------|-------|
| **Severity** | Blocking |
| **Spec Reference** | §4.1 |
| **Affected Clients** | Async only |

### Requirement

> Open a persistent stream for sending messages to queues. Uses `QueuesUpstream(stream QueuesUpstreamRequest) → stream QueuesUpstreamResponse`.

### Current Behavior

- **Sync client**: Fully implements via `UpstreamSender` — persistent bidirectional stream with request/response correlation, auto-reconnection, and background thread.
- **Async client**: Uses the unary `SendQueueMessage` RPC (`AsyncTransport.send_queue_message()` at line 816). The `AsyncTransport.queues_upstream()` method exists (line 855) but is **never called** by `AsyncClient`.

### Impact

- Same throughput concerns as GAP-02
- The unary `SendQueueMessage` is the "Simple API" (§5.1), not the primary stream API (§4.1)
- Batch sending via `send_queue_messages_batch()` uses `SendQueueMessagesBatch` unary RPC, not the stream

### Root Cause

`AsyncClient.send_queue_message()` directly calls `self._transport.send_queue_message(pb_message)` instead of routing through a persistent stream.

### Recommended Fix

Implement an `AsyncUpstreamSender` that wraps `AsyncTransport.queues_upstream()` with:
1. Persistent `grpc.aio.StreamStreamCall`
2. `asyncio.Queue` for request buffering
3. Response correlation via `RefRequestID`
4. Auto-reconnection

---

## 6. GAP-04: OpenTelemetry Span Field Not Set on Protobuf

| Attribute | Value |
|-----------|-------|
| **Severity** | Blocking |
| **Spec Reference** | §6.1, §6.3, §7.1, Notes §10 |
| **Affected Clients** | Both sync and async |

### Requirement

> The `Span` field on `Request` and `Response` carries distributed tracing context. SDKs should support propagating trace context through this field when OpenTelemetry is configured.

### Current Behavior

The Python SDK does NOT populate the protobuf `Span` field. Instead, it injects W3C TraceContext into message Tags via `KubeMQTagsCarrier`:

```python
# cq/client.py:205-206
carrier = KubeMQTagsCarrier(tags_dict)
carrier.inject()
```

The `Span` bytes field on the protobuf `Request` and `Response` messages is never set.

### Impact

- **Cross-SDK trace propagation broken**: Other KubeMQ SDKs (Go, Java, C#) that extract trace context from the `Span` field will not see the Python client's traces. The Tags-based approach is Python-only and not interoperable.
- **Server-side tracing**: If the server reads `Span` for its own observability, Python client traces will be invisible.

### Root Cause

The OTel instrumentation (`_internal/telemetry/instrumentation.py`) was implemented using W3C TraceContext propagation via Tags rather than the protobuf `Span` binary field.

### Recommended Fix

In addition to (or instead of) Tags-based propagation, serialize the OTel SpanContext into the `Span` bytes field using the same format as other SDKs (likely protobuf-encoded `SpanContext` or binary trace context).

---

## 7. GAP-05: maxReceiveSize Default Mismatch (100MB vs 4MB)

| Attribute | Value |
|-----------|-------|
| **Severity** | Medium |
| **Spec Reference** | §1.1 |
| **Affected Clients** | Both |

### Requirement

> `maxReceiveSize` — Default: 4194304 (4MB). Note: programmatic default without config file is 100MB, but deployed servers default to 4MB via TOML config.

### Current Behavior

Both `Connection.max_receive_size` and `ClientConfig.max_receive_size` default to **100MB** (104857600 bytes).

### Impact

The spec notes that 100MB is acceptable as the "programmatic default without config file." However, in production deployments where the server defaults to 4MB via TOML config, messages larger than 4MB will be rejected server-side even though the client allows them. This creates a confusing error at the gRPC level.

### Recommended Fix

Consider adding documentation noting this discrepancy, or add a config option to match the server's TOML defaults.

---

## 8. GAP-06: No Minimum 5-Second Keepalive Floor

| Attribute | Value |
|-----------|-------|
| **Severity** | High |
| **Spec Reference** | §1.1 |
| **Affected Clients** | Both |

### Requirement

> The server enforces a minimum client ping interval of 5 seconds — clients that ping more aggressively will receive `GOAWAY` and be disconnected. SDKs should configure their gRPC keepalive parameters accordingly (client ping interval >= 5s).

### Current Behavior

`KeepAliveConfig` defaults to `ping_interval_in_seconds=10` (safe), but the only validation is `> 0`:

```python
# core/config.py — KeepAliveConfig
if self.ping_interval_in_seconds <= 0:
    raise ValueError(...)
```

A user can set `ping_interval_in_seconds=1` and the SDK will accept it, causing the server to send `GOAWAY` and disconnect.

### Impact

Users who tune keepalive aggressively (e.g., for low-latency detection) will experience mysterious disconnections.

### Recommended Fix

Add a minimum floor of 5 seconds:

```python
if self.ping_interval_in_seconds < 5:
    raise ValueError("Keepalive ping interval must be >= 5 seconds (server minimum)")
```

---

## 9. GAP-07: Sync Client Missing send_queue_messages_batch()

| Attribute | Value |
|-----------|-------|
| **Severity** | Medium |
| **Spec Reference** | §5.2 |
| **Affected Clients** | Sync only |

### Requirement

> `SendQueueMessagesBatch(QueueMessagesBatchRequest) → QueueMessagesBatchResponse` — Send batch of queue messages.

### Current Behavior

- **Async client**: Has `send_queue_messages_batch()` → `SendQueueMessagesBatch` gRPC call.
- **Sync client**: No `send_queue_messages_batch()` method. Users must send messages one at a time through `send_queues_message()` (which routes through `UpstreamSender` stream).

### Impact

Users of the sync client cannot send a batch of queue messages in a single atomic operation with per-message error reporting.

### Recommended Fix

Add `send_queue_messages_batch()` to the sync `Client` class that calls the unary `SendQueueMessagesBatch` gRPC method.

---

## 10. GAP-08: Async Client Per-Message Ack/Reject Broken

| Attribute | Value |
|-----------|-------|
| **Severity** | High |
| **Spec Reference** | §4.2, §4.3 |
| **Affected Clients** | Async only |

### Requirement

> SDKs must support AckRange, NAckRange, and ReQueueRange — operations on specific messages by sequence range.

### Current Behavior

`AsyncClient.receive_queue_messages()` at line 149 passes `response_handler=None` to `QueueMessageReceived.decode()`. When users call `.ack()`, `.reject()`, or `.re_queue()` on individual messages, these methods call `self._do_operation()` which calls `self.response_handler(request)` — raising:

```
ValueError("response_handler is not set")
```

The `AsyncQueuesPollResponse` class provides bulk operations (`ack_all()`, `reject_all()`, `re_queue_all()`) that work correctly because they use a separate `_do_operation()` path with its own response handler. But per-message operations are broken.

### Impact

- Users cannot selectively ack/reject individual messages in async mode
- The async receive flow is limited to all-or-nothing operations (AckAll/NAckAll)
- This makes fine-grained message processing impossible in async applications

### Root Cause

`QueueMessageReceived.decode()` requires a `response_handler` callable that can send `QueuesDownstreamRequest` back through the bidirectional stream. The async client doesn't maintain a persistent downstream stream for individual message operations.

### Recommended Fix

Either:
1. Wire the response handler through `AsyncQueuesPollResponse._do_operation()` to also handle per-message operations, or
2. Implement an `AsyncDownstreamReceiver` that maintains a persistent stream and provides response handlers to individual messages.

---

## 11. GAP-09: List Channels Retry Inconsistency

| Attribute | Value |
|-----------|-------|
| **Severity** | Medium |
| **Spec Reference** | §8.2.3 |
| **Affected Clients** | Both |

### Requirement

> List channels only returns results when the request reaches the cluster master node. If the request is routed to a non-master node, the SDK's SendRequest will time out. SDKs should handle this timeout gracefully and retry.

### Current Behavior

| Function | Retry | Max Attempts | Handles Snapshot Not Ready |
|----------|-------|:------------:|:--------------------------:|
| `list_queues_channels()` | Yes | 3 (2s delay) | Yes |
| `list_cq_channels()` | **No** | — | **No** |
| `list_pubsub_channels()` | **No** | — | **No** |

### Impact

Listing CQ or PubSub channels on a multi-node cluster may silently fail with a timeout if the request doesn't reach the master node.

### Recommended Fix

Apply the same retry logic from `list_queues_channels()` to `list_cq_channels()` and `list_pubsub_channels()`.

---

## 12. GAP-10: Queue Validation Missing Upper Bounds

| Attribute | Value |
|-----------|-------|
| **Severity** | Medium |
| **Spec Reference** | §10.4 |
| **Affected Clients** | Both |

### Requirement

> `MaxNumberOfMessages` must be >= 1 and <= server max (default 1024).
> `WaitTimeSeconds` must be >= 0 and <= server max (default 3600).

### Current Behavior

`QueuesPollRequest` and the client methods accept any positive integer for `max_messages` and `wait_timeout_in_seconds`. There is no upper bound validation. The server may silently truncate or reject values beyond its configured limits.

### Recommended Fix

Add client-side validation:

```python
if max_messages < 1 or max_messages > 1024:
    raise ValueError("max_messages must be between 1 and 1024")
if wait_timeout < 0 or wait_timeout > 3600:
    raise ValueError("wait_timeout must be between 0 and 3600 seconds")
```

---

## 13. GAP-11: EventsStoreTypeData Not Enforced as Undefined for Non-Store

| Attribute | Value |
|-----------|-------|
| **Severity** | Low |
| **Spec Reference** | §10.3 |
| **Affected Clients** | Both |

### Requirement

> `EventsStoreTypeData` must be `Undefined` (0) for non-store event subscriptions. The server rejects non-store subscriptions with a non-zero `EventsStoreTypeData`.

### Current Behavior

`EventsSubscription.encode()` simply doesn't set `EventsStoreTypeData`, which defaults to 0 (Undefined) in protobuf. This is correct **in practice** because protobuf's zero value is Undefined. However, there is no explicit validation preventing a user from somehow setting a non-zero value.

### Impact

Minimal — the protobuf default is correct. This is a defense-in-depth concern.

### Recommended Fix

No action required — the current behavior is functionally correct. If desired, add a note in documentation.

---

## 14. GAP-12: Downstream Request/Response Metadata Field Not Mapped

| Attribute | Value |
|-----------|-------|
| **Severity** | Low |
| **Spec Reference** | §4.2 |
| **Affected Clients** | Both |

### Requirement

The `QueuesDownstreamRequest` has a `Metadata` field (`map[string]string`) for arbitrary key-value metadata. The `QueuesDownstreamResponse` also has a `Metadata` field.

### Current Behavior

Neither the downstream request builder nor the response parser maps the `Metadata` field. The protobuf field exists but is ignored.

### Impact

Low — this field is for arbitrary metadata and rarely used. But users cannot pass or receive custom metadata on downstream operations.

### Recommended Fix

Add `metadata: dict[str, str] = {}` to `QueuesPollRequest` and `QueuesPollResponse`.

---

## 15. GAP-13: Async Client Missing Channel Management Methods

| Attribute | Value |
|-----------|-------|
| **Severity** | High |
| **Spec Reference** | §8.2 |
| **Affected Clients** | Async only |

### Requirement

All SDK clients must support channel management (create, delete, list).

### Current Behavior

| Operation | Sync Client | Async Client |
|-----------|:-----------:|:------------:|
| `create_*_channel()` | Yes | **No** |
| `delete_*_channel()` | Yes | **No** |
| `list_*_channels()` | Yes | **No** |
| `queues_info()` | Yes | Yes |

The async `Client` classes (`AsyncPubSubClient`, `AsyncCQClient`, `AsyncQueuesClient`) do not expose `create_*`, `delete_*`, or `list_*` methods. Users must use the sync client for channel management even in async applications.

### Impact

- Async applications must instantiate both sync and async clients for full functionality
- Forces mixing of sync and async code, defeating the purpose of native async

### Recommended Fix

Add channel management methods to all async client classes using `AsyncTransport.send_request()`.

---

## 16. GAP-14: Queue Info Returns Raw Protobuf

| Attribute | Value |
|-----------|-------|
| **Severity** | Low |
| **Spec Reference** | §8.1 |
| **Affected Clients** | Both |

### Requirement

SDK should expose `QueuesInfo` with `TotalQueue`, `Sent`, `Delivered`, `Waiting` and per-queue `QueueInfo[]` with all stats.

### Current Behavior

Both `Client.queues_info()` and `AsyncClient.queues_info()` return the raw protobuf `QueuesInfoResponse` object directly. There is no Python domain model wrapping it.

### Impact

Users must work with protobuf objects directly, which is inconsistent with the rest of the SDK that uses Pydantic models.

### Recommended Fix

Create `QueuesInfoResult` and `QueueInfoItem` Pydantic models with a `decode()` classmethod.

---

## 17. GAP-15: Missing Code Examples

| Attribute | Value |
|-----------|-------|
| **Severity** | Medium |
| **Spec Reference** | §12 |

### Missing Examples

| Example | Category | Priority |
|---------|----------|----------|
| AckRange (selective ack) | Queues Stream | High |
| NAckAll (reject all) | Queues Stream | High |
| ReQueueAll (move to queue) | Queues Stream | High |
| AutoAck mode | Queues Stream | Medium |
| Simple send single queue message | Queues Simple | Medium |
| Simple pull queue messages | Queues Simple | Medium |

### Existing Examples That Need Enhancement

- Async examples use the deprecated thread-wrapped `_async` methods, not native `AsyncClient`
- No example demonstrating the new exception hierarchy
- No example using `ClientConfig.from_env()` or `ClientConfig.from_file()`
- No example of `subscribe_with_callback()` pattern

---

## 18. GAP-16: Sync-Async Feature Parity Matrix

This is a structural gap that encompasses multiple individual gaps. The following table shows the full parity status:

| Feature | Sync Client | Async Client | Gap |
|---------|:-----------:|:------------:|:---:|
| **Events** | | | |
| Send event (unary) | Via stream | Via unary | — |
| Send events (bidirectional stream) | `EventSender` | **Missing** | GAP-02 |
| Subscribe to events | Thread-based | AsyncIterator + callback | — |
| **Events Store** | | | |
| Send event store (unary) | Via stream (blocks for ack) | Via unary | — |
| Send events store (stream) | `EventSender` | **Missing** | GAP-02 |
| Subscribe to events store | Thread-based | AsyncIterator + callback | — |
| **Queues** | | | |
| Send via stream | `UpstreamSender` | **Missing** (uses unary) | GAP-03 |
| Send batch | **Missing** | `send_queue_messages_batch()` | GAP-07 |
| Receive via stream | `DownstreamReceiver` | Uses unary `ReceiveQueueMessages` | GAP-08 |
| Per-message ack/reject | Works | **Broken** (handler=None) | GAP-08 |
| Subscribe (continuous poll) | **Missing** | `subscribe_to_queue()` | — |
| Process (callback-based) | **Missing** | `process_queue_messages()` | — |
| **CQ** | | | |
| Send command/query | Direct gRPC | With `RetryExecutor` | — |
| Subscribe to commands/queries | Thread-based | AsyncIterator + callback | — |
| Batch send commands/queries | **Missing** | `send_commands_batch()` | — |
| **Channel Management** | | | |
| Create/Delete/List channels | All types | **Missing** | GAP-13 |
| Queue Info | Returns raw protobuf | Returns raw protobuf | GAP-14 |
| **Infrastructure** | | | |
| Retry logic | **Not available** | `RetryExecutor` | — |
| Reconnection | Simple sleep-retry | Exponential backoff + buffer | — |
| State machine | **Not available** | `ConnectionStateManager` | — |

---

## 19. GAP-17: Async Path Missing gRPC Keepalive Safety Options

| Attribute | Value |
|-----------|-------|
| **Severity** | Medium |
| **Spec Reference** | §1.1 |
| **Affected Clients** | Async only |

### Current Behavior

The sync transport sets these additional gRPC channel options:

```python
("grpc.http2.min_time_between_pings_ms", interval * 1000)
("grpc.http2.min_ping_interval_without_data_ms", interval * 1000)
```

The async transport (`AsyncTransport._build_channel_options()`) does NOT set these options.

### Impact

Without `min_time_between_pings_ms`, the gRPC library may send pings more aggressively than expected in certain edge cases, potentially triggering server `GOAWAY`.

### Recommended Fix

Add the same options to `AsyncTransport._build_channel_options()`.

---

## 20. GAP-18: RequestID Validation for SendResponse

| Attribute | Value |
|-----------|-------|
| **Severity** | Low |
| **Spec Reference** | §10.2 |
| **Affected Clients** | Both |

### Requirement

> `RequestID` must be non-empty for `SendResponse` (§6.3) — server validates this.

### Current Behavior

`CommandResponseMessage.encode()` copies `self.command_received.id` to `pb_response.RequestID`. There is no explicit validation that this ID is non-empty. However, since `CommandMessageReceived` always has an `id` from the server, this is unlikely to be empty in practice.

### Recommended Fix

Add defensive validation in `encode()`:

```python
if not self.command_received.id:
    raise ValueError("RequestID is required for SendResponse")
```

---

## 21. Minor Issues & Observations

### 21.1 Duplicate `with_updates()` in QueryMessage

`query_message.py` defines `with_updates()` twice (lines 86–95 and 97–106). The second definition overrides the first. Both are identical, so there's no behavioral impact, but it's dead code.

### 21.2 Timestamp Unit Inconsistency

`QueueMessageReceived.decode()` uses different divisors:
- `Timestamp`: divided by `1e9` (nanoseconds → seconds)
- `ExpirationAt`, `DelayedTo`: divided by `1e6` (microseconds → seconds)

`QueueSendResult.decode()` uses `1e9` for all three. This may be intentional (server uses different units for different contexts) but should be verified.

### 21.3 Channel Management Uses Legacy Exceptions

`requests.py` raises `CreateChannelError`, `DeleteChannelError`, `ListChannelsError`, `GRPCError` — all deprecated wrappers from `common/exceptions.py`. These should be migrated to the new exception hierarchy (`KubeMQChannelError`, etc.).

### 21.4 `QueuesPollRequest` Model Is Dead Code

`queues_poll_request.py` defines `QueuesPollRequest` as a user-facing convenience model, but neither sync nor async client uses it. Both build `QueuesDownstreamRequest` protobuf directly.

### 21.5 `CommandResponseMessage.decode()` Called on Instance

In `client.py:216`, `CommandResponseMessage().decode(response)` creates a throwaway instance then calls `decode()`. The `decode()` method is a `@classmethod` and works correctly when called on an instance, but this pattern is unconventional.

---

## 22. Prioritized Remediation Plan

### Phase 1: Blocking Issues (Must Fix)

| # | Gap | Effort | Description |
|---|-----|:------:|-------------|
| 1 | GAP-01 | Small | Make Ping bypass auth interceptors |
| 2 | GAP-04 | Medium | Populate protobuf `Span` field for cross-SDK trace propagation |
| 3 | GAP-06 | Small | Add 5-second minimum keepalive floor |
| 4 | GAP-17 | Small | Add missing gRPC keepalive options to async transport |

### Phase 2: High Priority (Should Fix Before Release)

| # | Gap | Effort | Description |
|---|-----|:------:|-------------|
| 5 | GAP-08 | Medium | Fix async per-message ack/reject (wire response handler) |
| 6 | GAP-13 | Medium | Add channel management methods to async clients |
| 7 | GAP-09 | Small | Add retry logic to CQ/PubSub list channels |
| 8 | GAP-02 | Large | Implement async bidirectional SendEventsStream |
| 9 | GAP-03 | Large | Implement async bidirectional QueuesUpstream |

### Phase 3: Medium Priority (Track for Next Release)

| # | Gap | Effort | Description |
|---|-----|:------:|-------------|
| 10 | GAP-07 | Small | Add sync `send_queue_messages_batch()` |
| 11 | GAP-10 | Small | Add upper bound validation for queue params |
| 12 | GAP-14 | Medium | Create Python models for queue info response |
| 13 | GAP-15 | Medium | Add missing code examples |
| 14 | GAP-12 | Small | Map Metadata field on downstream request/response |

### Phase 4: Low Priority (Nice to Have)

| # | Gap | Effort | Description |
|---|-----|:------:|-------------|
| 15 | GAP-11 | Small | Document EventsStoreTypeData behavior for non-store |
| 16 | GAP-18 | Small | Add defensive RequestID validation in SendResponse |
| 17 | Minor | Small | Fix duplicate `with_updates()`, migrate legacy exceptions, verify timestamps |

---

## Appendix A: Files Analyzed

| Category | Files |
|----------|-------|
| Transport | `transport/transport.py`, `transport/async_transport.py`, `transport/connection.py`, `transport/channel_manager.py`, `transport/interceptors.py`, `transport/keep_alive.py`, `transport/tls_config.py`, `transport/server_info.py` |
| Core | `core/config.py`, `core/client.py`, `core/exceptions.py`, `core/types.py`, `core/compat.py` |
| Internal | `_internal/retry.py`, `_internal/auth.py`, `_internal/transport/reconnect.py`, `_internal/transport/state.py` |
| PubSub | `pubsub/client.py`, `pubsub/async_client.py`, `pubsub/event_message.py`, `pubsub/event_store_message.py`, `pubsub/events_subscription.py`, `pubsub/events_store_subscription.py`, `pubsub/event_sender.py`, `pubsub/event_message_received.py`, `pubsub/event_store_message_received.py`, `pubsub/event_send_result.py` |
| CQ | `cq/client.py`, `cq/async_client.py`, `cq/command_message.py`, `cq/query_message.py`, `cq/command_response_message.py`, `cq/query_response_message.py`, `cq/commands_subscription.py`, `cq/queries_subscription.py`, `cq/command_message_received.py`, `cq/query_message_received.py` |
| Queues | `queues/client.py`, `queues/async_client.py`, `queues/queues_message.py`, `queues/queues_send_result.py`, `queues/queues_message_received.py`, `queues/queues_poll_request.py`, `queues/queues_poll_response.py`, `queues/queues_messages_waiting_pulled.py`, `queues/upstream_sender.py`, `queues/downstream_receiver.py` |
| Common | `common/requests.py`, `common/channel_validators.py`, `common/channel_stats.py`, `common/subscribe_type.py`, `common/cancellation_token.py`, `common/async_cancellation_token.py`, `common/exceptions.py`, `common/helpers.py` |
| Examples | 58 files in `examples/` directory |

## Appendix B: Test Coverage Notes

The unit test suite covers sync and async clients with mocked transports. Tests exist for:
- All message encode/decode paths
- Subscription flows with cancellation
- Queue downstream operations (all 11 types)
- Channel management
- Queue info
- Error handling and retry logic

However, integration tests against a live KubeMQ broker were not evaluated in this analysis.
