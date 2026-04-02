# KubeMQ Python SDK — Gap Analysis Report

**Date:** 2026-03-13
**SDK Version:** v4 (branch)
**Reference:** [sdk-api-feature-list.md](../sdk-api-feature-list.md) | [sdk-compliance-checklist.md](../sdk-compliance-checklist.md)

---

## Executive Summary

The KubeMQ Python SDK v4 covers the majority of the required API surface — **61.2% fully compliant**, rising to **75.6%** when including partial implementations. The core messaging primitives (Events, Queues, CQ) are functional and the transport layer is robust with reconnection, TLS/mTLS, and error mapping. However, there are **4 blocking issues** and **30+ non-blocking gaps** that must be addressed for full specification compliance.

| Severity | Count | Description |
|----------|:-----:|-------------|
| **CRITICAL** (data loss / broken feature) | 4 | Features that silently fail or produce wrong behavior |
| **HIGH** (missing functionality) | 11 | Features required by spec but not implemented |
| **MEDIUM** (incomplete implementation) | 15 | Features partially implemented with missing fields or validation |
| **LOW** (cosmetic / examples) | 30+ | Missing examples, non-standard defaults, minor field omissions |

---

## Table of Contents

1. [Critical Gaps](#1-critical-gaps)
2. [High Priority Gaps](#2-high-priority-gaps)
3. [Medium Priority Gaps](#3-medium-priority-gaps)
4. [Low Priority Gaps](#4-low-priority-gaps)
5. [Validation Gap Matrix](#5-validation-gap-matrix)
6. [Feature Coverage Heatmap](#6-feature-coverage-heatmap)
7. [Recommended Fix Order](#7-recommended-fix-order)

---

## 1. Critical Gaps

These are issues where the SDK **silently produces wrong behavior** or where a documented feature is **broken**.

---

### GAP-C1: `StartAtTimeDelta` Encode Is Broken

**Spec Reference:** §3.3 (Events Store Subscribe), §9.3 (EventsStoreType enum value 6)

**Location:** `src/kubemq/pubsub/events_store_subscription.py:92-104`

**What's wrong:**
The `EventsStoreSubscription.encode()` method has conditional branches for `StartAtSequence` and `StartAtTime`, but **no branch for `StartAtTimeDelta`**. When a user subscribes with `events_store_type=EventsStoreType.StartAtTimeDelta`, the enum value (6) is correctly set on `request.EventsStoreTypeData`, but `request.EventsStoreTypeValue` is **never assigned**. The server receives `EventsStoreTypeData=6` with `EventsStoreTypeValue=0`, causing undefined behavior (likely defaulting to "start from now" or erroring silently).

**Additionally:**
- No field exists to accept the delta value — `events_store_sequence_value` is semantically tied to sequence numbers only.
- No Pydantic validator checks that a positive delta value is provided when `StartAtTimeDelta` is selected.

**Impact:** Users who try to subscribe "from N seconds ago" will get incorrect message playback with no error or warning.

**Fix required:**
1. Add an `events_store_time_delta_seconds` field (or generalize `events_store_type_value`)
2. Add a Pydantic validator: if `events_store_type == StartAtTimeDelta`, require value > 0
3. Add encode branch: `if self.events_store_type == EventsStoreType.StartAtTimeDelta: request.EventsStoreTypeValue = self.events_store_time_delta_seconds`

---

### GAP-C2: `CacheHit` Field Missing from Query Response

**Spec Reference:** §7.1 (Send Query Response)

**Location:** `src/kubemq/cq/query_response_message.py:47-60`

**What's wrong:**
The `QueryResponseMessage.decode()` method maps `Executed`, `Error`, `RequestID`, `ClientID`, `Timestamp`, `Metadata`, `Body`, `Tags` from the protobuf Response — but **does NOT map `CacheHit: bool`**. The protobuf field exists (`kubemq_pb2.pyi:345`) but is silently dropped.

**Impact:** Cache-enabled queries are a key feature of KubeMQ Queries. Without `CacheHit`, callers have no way to know whether a response came from cache or was freshly computed. The entire cache feature is opaque to SDK users.

**Fix required:**
1. Add `cache_hit: bool = False` field to `QueryResponseMessage`
2. Map `response.CacheHit` in the `decode()` method

---

### GAP-C3: No `CloseByServer` Handling in Queue Downstream

**Spec Reference:** §4.2 (Queue Downstream), §4.3 (Typical Downstream Flow)

**Location:** `src/kubemq/queues/downstream_receiver.py:217-234`

**What's wrong:**
The `DownstreamReceiver._process_responses()` method routes responses by `RefRequestId` to waiting callers, but **never inspects `RequestTypeData` for `CloseByServer` (11)**. When the server decides to close the downstream stream (e.g., due to timeout, maintenance, or cluster rebalancing), the SDK does not detect the close signal and may:
- Continue trying to send on a closed stream
- Leave transactions in an ambiguous state
- Not trigger automatic reconnection

**Impact:** Server-initiated stream closures result in undefined behavior — potentially orphaned transactions, stuck poll operations, or silent message loss.

**Fix required:**
1. In `_process_responses()`, check if `response.RequestTypeData == QueuesDownstreamRequestType.CloseByServer`
2. On detection, close the stream gracefully, notify waiting callers, and trigger reconnection

---

### GAP-C4: No CacheKey/CacheTTL Cross-Validation

**Spec Reference:** §10.2 (Request-Specific Validation)

**Location:** `src/kubemq/cq/query_message.py`

**What's wrong:**
When a user sets `cache_key="my-key"` but leaves `cache_ttl_int_seconds=0` (the default), no validation error is raised. The message is sent to the server with a cache key but zero TTL, which may cause the server to ignore caching silently or cache with a zero-second expiry.

**Impact:** Users who attempt to use query caching may find it silently doesn't work. This is especially insidious because caching is an opt-in feature — users explicitly enabling it expect it to function.

**Fix required:**
1. Add a Pydantic `@model_validator` to `QueryMessage`: if `cache_key` is non-empty, require `cache_ttl_int_seconds > 0`

---

## 2. High Priority Gaps

Features required by the spec that are **not implemented** but don't cause silent failures.

---

### GAP-H1: No Channel Wildcard Validation (CQ / Queues / EventsStore)

**Spec Reference:** §10.1 (Common Validation)

**Locations:**
- `src/kubemq/cq/commands_subscription.py:18-20`
- `src/kubemq/cq/queries_subscription.py:30-33`
- `src/kubemq/cq/command_message.py:40-43`
- `src/kubemq/cq/query_message.py:52-55`
- `src/kubemq/queues/queues_message.py:107-121`
- `src/kubemq/queues/queues_poll_request.py:80-97`
- `src/kubemq/pubsub/events_store_subscription.py:32-36`

**What's wrong:**
The spec states: *"Channel cannot contain `*` or `>` wildcards — except Events subscribe."* However, **none** of the CQ, Queue, or EventsStore validators check for wildcard characters. Only Events subscribe (correctly) allows them. All other message types accept wildcards and forward them to the server, which rejects them with `ErrInvalidWildcards`.

**Impact:** Invalid channel names pass client-side validation, resulting in server-side errors that could be caught earlier with better error messages.

**Fix required:**
Add wildcard rejection (`*` and `>` characters) to channel validators in all non-Events-subscribe types.

---

### GAP-H2: No Channel Whitespace Validation

**Spec Reference:** §10.1 (Common Validation)

**All channel validators affected.**

**What's wrong:**
The spec states: *"Channel cannot contain whitespace."* No validator checks for whitespace characters. A channel like `"my channel"` or `"  "` passes all validation.

**Fix required:**
Add regex or character-set validation to reject whitespace in channel names.

---

### GAP-H3: No Channel Trailing-Dot Validation

**Spec Reference:** §10.1 (Common Validation)

**All channel validators affected.**

**What's wrong:**
The spec states: *"Channel cannot end with `.`"*. No validator checks for trailing dots. A channel like `"orders."` passes all validation.

**Fix required:**
Add `if v.endswith("."): raise ValueError(...)` to all channel validators.

---

### GAP-H4: `CommandResponseMessage` Missing Metadata, Body, Tags

**Spec Reference:** §6.3 (Send Command Response)

**Location:** `src/kubemq/cq/command_response_message.py`

**What's wrong:**
The `CommandResponseMessage` model only has `is_executed: bool` and `error: str` as response fields. It does **not** include `metadata`, `body`, or `tags` — neither on the model nor in the `encode()` method. By contrast, `QueryResponseMessage` correctly includes all three fields.

**Impact:** Command responders cannot send metadata, payload, or tags back to the command sender. This limits the utility of command responses to simple success/failure signals.

**Fix required:**
1. Add `metadata: Optional[str]`, `body: Optional[bytes]`, `tags: dict[str, str]` fields
2. Encode them in `encode()`: `pb_response.Metadata`, `pb_response.Body`, `pb_response.Tags`

---

### GAP-H5: `CommandResponseMessage.decode()` Drops Metadata, Body, Tags

**Spec Reference:** §6.1 (Send Command Response)

**Location:** `src/kubemq/cq/command_response_message.py:31-38`

**What's wrong:**
When a command response is **received** (decoded from protobuf), the `decode()` method only maps `Executed`, `Error`, `RequestID`, `ClientID`, `Timestamp`. The `Metadata`, `Body`, and `Tags` fields from the protobuf Response are dropped. Compare with `QueryResponseMessage.decode()` which correctly maps all fields.

**Impact:** Even if the server (or another SDK) sends a command response with metadata/body/tags, the Python SDK caller never sees them.

**Fix required:**
Add `metadata`, `body`, `tags` to the decode method, mirroring `QueryResponseMessage.decode()`.

---

### GAP-H6: Missing Queue Downstream Operations (ActiveOffsets, TransactionStatus, CloseByClient)

**Spec Reference:** §4.2 (Queue Downstream Request Types 8, 9, 10)

**Location:** `src/kubemq/queues/downstream_receiver.py`, `src/kubemq/queues/queues_poll_response.py`

**What's wrong:**
Three downstream request types are not implemented:
- **ActiveOffsets (8):** Get list of active (unacknowledged) sequence numbers in the current transaction
- **TransactionStatus (9):** Check if a transaction is still active
- **CloseByClient (10):** Explicitly signal stream close from the client side

**Impact:**
- `ActiveOffsets`: Users cannot inspect which messages remain unprocessed in a transaction
- `TransactionStatus`: Users cannot verify if a transaction timed out before attempting ack/nack
- `CloseByClient`: Streams are never explicitly closed from the client side; relies on implicit gRPC stream termination

**Fix required:**
Add methods `active_offsets()`, `transaction_status()`, and `close()` to `QueuesPollResponse`.

---

### GAP-H7: `QueuesInfo` Not Exposed in User-Facing Queue Clients

**Spec Reference:** §8.1 (Queue Info)

**Location:** `src/kubemq/transport/async_transport.py:926-954` (exists), `src/kubemq/queues/async_client.py` (missing)

**What's wrong:**
The `AsyncTransport` layer has a working `queues_info()` method that calls `self._stub.QueuesInfo(request)`, but neither `AsyncClient` (queues) nor `Client` (queues) expose this method to end users.

**Impact:** Users cannot query queue statistics (message counts, subscriber counts, byte counts) through the SDK's public API.

**Fix required:**
Add `queues_info(queue_name: Optional[str] = None) → QueuesInfoResponse` to both sync and async queue clients.

---

### GAP-H8: No Trace Propagation in SendResponse

**Spec Reference:** §6.3 (Send Command Response), §7.3 (Send Query Response)

**Location:** `src/kubemq/cq/client.py:353-378`, `src/kubemq/cq/async_client.py:313-345`

**What's wrong:**
When sending a command/query response, the SDK does NOT inject trace context (W3C traceparent/tracestate) into the response message's Tags. While the send-request path correctly uses `KubeMQTagsCarrier` for trace propagation, the response path has no equivalent injection. This breaks distributed tracing across the request-response boundary.

**Impact:** Distributed traces show the request leg but not the response leg, making it impossible to correlate end-to-end latencies in observability tools.

**Fix required:**
Inject OTel context into response Tags via `KubeMQTagsCarrier`, matching the pattern used in `send_command()`/`send_query()`.

---

### GAP-H9: `SendQueueMessagesBatch` Not Used by Client

**Spec Reference:** §5.2 (Send Batch Messages)

**Location:** `src/kubemq/queues/async_client.py:279-316`

**What's wrong:**
The async client's "batch send" method uses `asyncio.gather()` with a semaphore to send messages concurrently via individual `send_queue_message()` calls. This is client-side fan-out, not the gRPC `SendQueueMessagesBatch` RPC. The transport layer has the batch RPC method, but it's never called. Key differences:
- No `BatchID` tracking
- No `HaveErrors` aggregate flag
- Individual failures are not atomically reported as a batch result

**Impact:** Batch semantics differ from spec — no server-side batch tracking, no `BatchID` correlation, potentially different performance characteristics.

**Fix required:**
Wire the client's batch send to use `AsyncTransport.send_queue_messages_batch()`, which calls the gRPC `SendQueueMessagesBatch` RPC.

---

### GAP-H10: Sync Queue Client Missing `AckAllQueueMessages`

**Spec Reference:** §5.4 (Ack All Queue Messages)

**Location:** `src/kubemq/queues/client.py` (missing method)

**What's wrong:**
The `AckAllQueueMessages` operation is only available on `AsyncClient`. The sync `Client` class has no equivalent method. This means sync users cannot purge queues or ack all pending messages.

**Fix required:**
Add `ack_all_queue_messages(channel: str, wait_time_seconds: int = 60)` to the sync `Client`.

---

### GAP-H11: `AckAllQueueMessages` WaitTimeSeconds Not Configurable

**Spec Reference:** §5.4 (Ack All Queue Messages)

**Location:** `src/kubemq/queues/async_client.py:619`

**What's wrong:**
The `WaitTimeSeconds` parameter is hardcoded to `60`:
```python
request.WaitTimeSeconds = 60
```
Users cannot control how long the server waits to accumulate ack operations. For large queues, 60 seconds may be insufficient; for small operations, it causes unnecessary delay on timeout.

**Fix required:**
Add `wait_time_seconds: int = 60` as a method parameter.

---

## 3. Medium Priority Gaps

Partially implemented features with **missing fields or non-standard behavior**.

---

### GAP-M1: `maxReceiveSize` Default Is 100MB (Spec: 4MB)

**Spec Reference:** §1.1 (Client Construction)

**Location:** `src/kubemq/core/config.py:305`

**What's wrong:**
`DEFAULT_MAX_MESSAGE_SIZE = 100 * 1024 * 1024` (100MB). The spec says default should be 4MB (4194304 bytes). The spec note clarifies: "programmatic default without config file is 100MB, but deployed servers default to 4MB via TOML config."

**Impact:** Clients may attempt to send/receive messages up to 100MB, which the server (with default 4MB config) will reject. This causes confusing runtime errors for messages between 4MB and 100MB.

**Fix required:**
Consider aligning default to 4MB (`4 * 1024 * 1024`) or document the discrepancy prominently.

---

### GAP-M2: `SendQueueMessageResult` Missing Ref Fields

**Spec Reference:** §4.1 (Queue Upstream Response)

**Location:** `src/kubemq/queues/queues_send_result.py:129-163`

**What's wrong:**
`QueueSendResult.decode()` does not map `RefChannel`, `RefTopic`, `RefPartition`, `RefHash` from the protobuf `SendQueueMessageResult`. These fields exist in the protobuf but are dropped during decode.

**Impact:** Low for current queue operations (these are "reserved for Topics" per spec), but breaks forward compatibility when Topics feature is implemented.

**Fix required:**
Add `ref_channel`, `ref_topic`, `ref_partition`, `ref_hash` fields and map them in `decode()`.

---

### GAP-M3: `QueueMessageAttributes.MD5OfBody` Not Mapped

**Spec Reference:** §4.2 (QueueMessageAttributes)

**Location:** `src/kubemq/queues/queues_message_received.py:258-298`

**What's wrong:**
The `MD5OfBody` attribute from `QueueMessageAttributes` is not decoded into `QueueMessageReceived`. All other 7 attributes are mapped.

**Impact:** Users cannot verify message integrity via MD5 hash comparison.

**Fix required:**
Add `md5_of_body: Optional[str]` field and map `msg.Attributes.MD5OfBody` in decode.

---

### GAP-M4: `QueuesDownstreamRequest.Metadata` Never Populated

**Spec Reference:** §4.2 (Queue Downstream Request)

**What's wrong:**
The `Metadata` field on `QueuesDownstreamRequest` (`map[string]string`) is never set by any SDK code path. This field allows attaching arbitrary metadata to downstream operations.

**Impact:** Users cannot attach custom metadata to queue receive operations. Low priority since this is rarely used in practice.

---

### GAP-M5: `QueuesDownstreamResponse` Drops `RequestTypeData` and `Metadata`

**Spec Reference:** §4.2 (Queue Downstream Response)

**Location:** `src/kubemq/queues/queues_poll_response.py:243-298`

**What's wrong:**
`QueuesPollResponse.decode()` does not surface `RequestTypeData` or `Metadata` from the response. `RequestTypeData` is particularly important for detecting `CloseByServer` (GAP-C3).

**Fix required:**
Add `request_type: int` and `metadata: dict[str, str]` fields to `QueuesPollResponse`.

---

### GAP-M6: Simple API Response Fields Not Surfaced

**Spec Reference:** §5.3 (Receive Messages Response)

**Location:** `src/kubemq/queues/queues_messages_waiting_pulled.py`

**What's wrong:**
`ReceiveQueueMessagesResponse` contains `MessagesReceived`, `MessagesExpired`, and `IsPeak` fields that are available in the protobuf but not decoded into the SDK's `QueueMessagesWaiting`/`QueueMessagesPulled` models. Only `IsError`, `Error`, and `Messages` are extracted.

**Impact:** Users cannot see how many messages expired during the receive operation or confirm peek mode.

**Fix required:**
Add `messages_received: int`, `messages_expired: int`, `is_peak: bool` fields.

---

### GAP-M7: Queue Validation Missing Upper Bounds

**Spec Reference:** §10.4 (Queue Validation)

**Locations:**
- `src/kubemq/queues/queues_poll_request.py:60-64` — `MaxNumberOfMessages` lacks `<= 1024`
- `src/kubemq/queues/queues_poll_request.py:65-67` — `WaitTimeSeconds` lacks `<= 3600`

**What's wrong:**
Lower bounds are enforced (`>= 1`) but upper bounds are missing:
- `MaxNumberOfMessages` should be `<= 1024` (server default max)
- `WaitTimeSeconds` should be `<= 3600` (server default max, 1 hour)

**Impact:** Out-of-range values are sent to the server, which silently clamps or rejects them.

**Fix required:**
Add `le=1024` and `le=3600` constraints to the Pydantic field definitions.

---

### GAP-M8: Sync Client Uses Stream for Unary Sends

**Spec Reference:** §2.1 (Send Event Unary), §3.1 (Send Event to Store Unary), §5.1 (Send Single Queue Message)

**Locations:**
- `src/kubemq/pubsub/event_sender.py` — Events/EventsStore via bidirectional `SendEventsStream`
- `src/kubemq/queues/upstream_sender.py` — Queues via bidirectional `QueuesUpstream`

**What's wrong:**
The sync client always uses bidirectional streams for individual message sends rather than unary gRPC calls. While this is a valid optimization (connection reuse, reduced connection overhead), it means:
- The "unary send" API contract is different from spec
- Fire-and-forget events never return a `Result` (sync client returns `None`)
- A background thread must be running for the stream

**Impact:** The async client correctly uses unary calls. The sync client's stream-based approach is functional but deviates from spec semantics.

**Assessment:** Acceptable as a design choice if documented. The stream approach is arguably better for high-throughput scenarios.

---

### GAP-M9: `clientId` Not Strictly Required

**Spec Reference:** §1.1 (Client Construction)

**Location:** `src/kubemq/core/config.py:407-408`

**What's wrong:**
The spec says `clientId` is "Required", but the SDK auto-generates it from `socket.gethostname()` if not provided. While this is a reasonable Python-idiomatic approach, it differs from the explicit requirement.

**Impact:** Low. Auto-generation is user-friendly but may cause confusion in multi-client scenarios where hostname-based IDs collide.

---

### GAP-M10: `ClientID` Fallback to Empty String

**Spec Reference:** §10.1 (Common Validation)

**Locations:** Various encode methods use `self._config.client_id or ""`

**What's wrong:**
While `ClientConfig` auto-generates from hostname, the `or ""` fallback in encode methods means that if `client_id` were somehow `None` or empty after construction, an empty string would be sent to the server, which would reject it.

**Impact:** Very low — `ClientConfig.__post_init__` prevents this in normal usage. Defensive coding issue.

---

### GAP-M11: Span Field Not Used (Tags-Based Trace Propagation Instead)

**Spec Reference:** §6.1 (Send Command), §7.1 (Send Query)

**What's wrong:**
The spec defines a `Span` bytes field on `Request` and `Response` for OpenTelemetry trace propagation. The Python SDK instead injects W3C `traceparent`/`tracestate` headers into the message's `Tags` dictionary via `KubeMQTagsCarrier`.

**Assessment:** This is a **valid alternative approach** — W3C trace context is the standard propagation format and is more portable than opaque binary spans. The Tags-based approach also works across SDK languages that adopt the same convention. However, it means the proto `Span` field is unused, and interop with SDKs that use `Span` directly would fail.

---

### GAP-M12: List Channels Timeout Handling

**Spec Reference:** §8.2.3 (List Channels)

**Location:** `src/kubemq/common/requests.py:149-160`

**What's wrong:**
Two issues:
1. **No retry logic:** If the request routes to a non-master node, it times out with no retry. The spec notes this is expected and SDKs should handle it gracefully.
2. **No "cluster snapshot not ready yet" error handling:** The spec warns the server may return this error shortly after startup. The SDK does not detect or handle this specific error.

**Fix required:**
Add retry logic (2-3 attempts) and detect "cluster snapshot not ready yet" error with a user-friendly message or automatic retry.

---

### GAP-M13: Legacy Protobuf Types Still in Generated Code

**Spec Reference:** §13 (Deprecated/Remove)

**Location:** `src/kubemq/grpc/kubemq_pb2_grpc.py`, `src/kubemq/grpc/kubemq_pb2.pyi`

**What's wrong:**
The deprecated `StreamQueueMessage` RPC, `StreamQueueMessagesRequest/Response` types, and `StreamRequestType` enum are present in the generated protobuf code. While no SDK client code references them, they remain discoverable in IDE autocompletion and could confuse users.

**Fix required:**
Regenerate protobufs from a pruned `.proto` file, or accept this as an artifact of shared proto definitions (the proto is shared across SDKs and the server).

---

### GAP-M14: Command Timeout User-Facing Unit Mismatch

**Spec Reference:** §6.1, §7.1 (Timeout in milliseconds)

**Location:** `src/kubemq/cq/command_message.py:37,60`

**What's wrong:**
The spec says timeout is in **milliseconds** on the wire. The Python SDK accepts `timeout_in_seconds: int` and converts internally via `pb_command.Timeout = self.timeout_in_seconds * 1000`. This is good UX (seconds are more intuitive), but the field name `timeout_in_seconds` could be misleading if users read the proto spec.

**Assessment:** Acceptable — the internal conversion is correct. Just ensure documentation is clear.

---

### GAP-M15: Events Store Subscribe Missing `StartAtTimeDelta` Validation

**Spec Reference:** §10.3 (Events Store Subscription Validation)

**Location:** `src/kubemq/pubsub/events_store_subscription.py`

**What's wrong:**
Validators exist for `StartAtSequence` (value must be non-zero) and `StartAtTime` (value must not be None), but **no validator** exists for `StartAtTimeDelta` (value must be > 0). This is related to GAP-C1 but specifically about the validation layer.

---

## 4. Low Priority Gaps

Minor field omissions, cosmetic issues, and **missing code examples**.

---

### GAP-L1: 29 Missing Code Examples

**Spec Reference:** §13 (Code Examples section of checklist)

The following examples are missing from the `examples/` directory:

**Events (2 missing):**
- Subscribe to events with consumer group
- Cancel event subscription

**Events Store (8 missing):**
- Send events to store via stream
- Subscribe — StartFromFirst, StartFromLast, StartAtSequence, StartAtTime, StartAtTimeDelta
- Subscribe with consumer group
- Cancel store subscription

**Queues — Stream (6 missing):**
- Send with expiration/delay/DLQ policy
- AckRange, NAckAll, ReQueueAll, AutoAck mode

**Queues — Simple (3 missing):**
- Send batch, Peek messages, Ack all

**RPC (4 missing):**
- Command with consumer group, command timeout handling
- Query with cache, query with consumer group

**Channel Management (4 missing):**
- Get queue info (all/specific), list with search filter, purge queue

---

### GAP-L2: No Dedicated Stream Send Example for Events

A dedicated example showing the bidirectional stream API for high-throughput event publishing would help users understand the streaming pattern.

---

## 5. Validation Gap Matrix

| Validation Rule | Events Send | Events Subscribe | EventsStore Send | EventsStore Subscribe | Commands Send | Commands Subscribe | Queries Send | Queries Subscribe | Queue Send | Queue Receive |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| Channel non-empty | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| No wildcards (* >) | N/A | Allowed | N/A | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| No whitespace | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| No trailing `.` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Metadata/Body required | ✅ | N/A | ✅ | N/A | ✅ | N/A | ✅ | N/A | ✅ | N/A |
| Timeout > 0 | N/A | N/A | N/A | N/A | ✅ | N/A | ✅ | N/A | N/A | N/A |
| CacheKey→CacheTTL>0 | N/A | N/A | N/A | N/A | N/A | N/A | ❌ | N/A | N/A | N/A |
| MaxMessages bounds | N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A | ~(no upper) |
| WaitTime bounds | N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A | ~(no upper) |

**Legend:** ✅ = Validated | ❌ = Missing | ~ = Partial | N/A = Not applicable

---

## 6. Feature Coverage Heatmap

```
Feature Area                    Coverage    Status Bar
─────────────────────────────────────────────────────
Connection & Lifecycle          ████████░░  87.5%  ██████████
Ping                            ██████████  100%   ██████████
Close/Disconnect                ██████████  100%   ██████████
Error Handling                  ██████████  100%   ██████████
ID Auto-Generation              ██████████  100%   ██████████
Events Send                     ████████░░  85%    ██████████
Events Subscribe                ██████████  100%   ██████████
Events Store Send               ████████░░  85%    ██████████
Events Store Subscribe          ██████░░░░  65%    ██████████
Queues Upstream (Stream)        ████████░░  85%    ██████████
Queues Downstream (Stream)      ██████░░░░  60%    ██████████
Queues Simple API               █████░░░░░  50%    ██████████
Commands Send                   ████████░░  80%    ██████████
Commands Subscribe              █████████░  90%    ██████████
Commands Response               ██████░░░░  55%    ██████████
Queries Send                    ███████░░░  70%    ██████████
Queries Subscribe               █████████░  90%    ██████████
Channel Management              ████████░░  80%    ██████████
Validation Rules                █████░░░░░  47%    ██████████
Code Examples                   ███░░░░░░░  33%    ██████████
Deprecated Removal              ████████░░  80%    ██████████
```

---

## 7. Recommended Fix Order

### Phase 1: Critical Fixes (Must-fix before release)

| # | Gap ID | Effort | Description |
|---|--------|--------|-------------|
| 1 | GAP-C1 | Small | Fix `StartAtTimeDelta` encode + add validation |
| 2 | GAP-C2 | Small | Add `CacheHit` to `QueryResponseMessage.decode()` |
| 3 | GAP-C3 | Medium | Handle `CloseByServer` in queue downstream |
| 4 | GAP-C4 | Small | Add CacheKey/CacheTTL cross-validation |

### Phase 2: Validation Hardening

| # | Gap ID | Effort | Description |
|---|--------|--------|-------------|
| 5 | GAP-H1 | Medium | Add wildcard rejection to all non-Events-subscribe validators |
| 6 | GAP-H2 | Small | Add channel whitespace validation |
| 7 | GAP-H3 | Small | Add channel trailing-dot validation |
| 8 | GAP-M7 | Small | Add upper bounds to queue MaxMessages/WaitTime |

### Phase 3: Missing Features

| # | Gap ID | Effort | Description |
|---|--------|--------|-------------|
| 9 | GAP-H4/H5 | Medium | Fix `CommandResponseMessage` encode/decode (Metadata/Body/Tags) |
| 10 | GAP-H6 | Medium | Implement ActiveOffsets, TransactionStatus, CloseByClient |
| 11 | GAP-H7 | Small | Expose `QueuesInfo` in user-facing clients |
| 12 | GAP-H8 | Small | Add trace propagation to SendResponse path |
| 13 | GAP-H9 | Medium | Wire batch send to gRPC `SendQueueMessagesBatch` RPC |
| 14 | GAP-H10 | Small | Add `ack_all_queue_messages` to sync queue client |
| 15 | GAP-H11 | Small | Make `AckAllQueueMessages.WaitTimeSeconds` configurable |

### Phase 4: Field Completeness

| # | Gap ID | Effort | Description |
|---|--------|--------|-------------|
| 16 | GAP-M2 | Small | Add Ref fields to `QueueSendResult` |
| 17 | GAP-M3 | Small | Add `MD5OfBody` to `QueueMessageReceived` |
| 18 | GAP-M4/M5 | Small | Surface `Metadata` on downstream request/response |
| 19 | GAP-M6 | Small | Surface `MessagesReceived`/`MessagesExpired`/`IsPeak` in simple API |
| 20 | GAP-M12 | Medium | Add retry logic for list channels + snapshot error handling |

### Phase 5: Examples & Documentation

| # | Gap ID | Effort | Description |
|---|--------|--------|-------------|
| 21 | GAP-L1 | Large | Create 29 missing code examples |
| 22 | GAP-M1 | Small | Align `maxReceiveSize` default or document discrepancy |

---

## Appendix: File Reference

| File | Gaps Affecting It |
|------|-------------------|
| `src/kubemq/pubsub/events_store_subscription.py` | GAP-C1, GAP-H1, GAP-M15 |
| `src/kubemq/cq/query_response_message.py` | GAP-C2 |
| `src/kubemq/cq/query_message.py` | GAP-C4, GAP-H1 |
| `src/kubemq/queues/downstream_receiver.py` | GAP-C3 |
| `src/kubemq/cq/command_response_message.py` | GAP-H4, GAP-H5 |
| `src/kubemq/cq/command_message.py` | GAP-H1 |
| `src/kubemq/cq/commands_subscription.py` | GAP-H1 |
| `src/kubemq/cq/queries_subscription.py` | GAP-H1 |
| `src/kubemq/queues/queues_message.py` | GAP-H1 |
| `src/kubemq/queues/queues_poll_request.py` | GAP-H1, GAP-M7 |
| `src/kubemq/queues/queues_poll_response.py` | GAP-H6, GAP-M5 |
| `src/kubemq/queues/queues_send_result.py` | GAP-M2 |
| `src/kubemq/queues/queues_message_received.py` | GAP-M3 |
| `src/kubemq/queues/queues_messages_waiting_pulled.py` | GAP-M6 |
| `src/kubemq/queues/async_client.py` | GAP-H7, GAP-H9, GAP-H11 |
| `src/kubemq/queues/client.py` | GAP-H10 |
| `src/kubemq/cq/client.py` | GAP-H8 |
| `src/kubemq/cq/async_client.py` | GAP-H8 |
| `src/kubemq/common/requests.py` | GAP-M12 |
| `src/kubemq/core/config.py` | GAP-M1, GAP-M9 |
