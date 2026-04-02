# Python Burn-In Implementation Retrospective

This document captures Python-specific issues discovered during implementation.
Combined with the Go retrospective, it will guide Java/JS/C# implementations.

---

### 1. Three Separate Clients (Severity: High)
**What happened**: Python SDK uses PubSubClient, QueuesClient, CQClient (not a unified client like Go).
**Root cause**: SDK architecture design decision — each domain has its own gRPC transport.
**Fix**: Engine must create, manage, and pass all 3 clients to their respective workers. Disconnect manager must close/recreate all 3.
**Rule for other SDKs**: Check how many client instances the SDK requires. Plan client lifecycle around the actual SDK architecture.

### 2. NON-Blocking Subscription Model (Severity: Critical)
**What happened**: `subscribe_to_events()`, `subscribe_to_events_store()`, `subscribe_to_commands()`, `subscribe_to_queries()` are all **NON-BLOCKING** — they spawn an internal daemon thread and return immediately. Initially assumed they were blocking (like Go), which caused the events_store consumer to create multiple concurrent subscriptions, resulting in 80,000+ duplicate messages on 4,000 sent.
**Root cause**: The SDK's `_subscribe()` method creates a `threading.Thread(target=self._subscribe_task, ...)` internally and starts it. The SDK thread handles auto-reconnect with exponential backoff and, for events_store, automatic sequence-based resubscription.
**Fix**: After calling subscribe (which returns immediately), wait on `self._consumer_stop.wait()` instead of looping. Do NOT implement external resubscription — the SDK handles it.
**Rule for other SDKs**: CRITICALLY verify whether subscription calls are blocking or non-blocking BEFORE implementing. Test with a simple script first. If the SDK handles auto-reconnect internally, do NOT add external reconnect logic — it causes duplicate subscriptions.

### 3. No Streaming Queue API (Severity: Medium)
**What happened**: Python SDK has no bidirectional stream for queues (unlike Go's QueueUpstream/QueueDownstream).
**Root cause**: Python SDK uses unary `send_queue_message()` + polling `receive_queue_messages()` for both queue patterns.
**Fix**: QueueStream and QueueSimple both use the same API but with different ack modes: QueueStream uses `auto_ack=False` + manual `msg.ack()`, QueueSimple uses `auto_ack=True`.
**Rule for other SDKs**: Check if the SDK has streaming vs polling queue APIs. Adapt worker accordingly.

### 4. Response Message Constructor Requires `command_received`/`query_received` (Severity: Medium)
**What happened**: `CommandResponseMessage` and `QueryResponseMessage` require the received message as constructor arg (not just reply_channel/request_id).
**Root cause**: Pydantic model with validator that checks `command_received` is not None and has a `reply_channel`.
**Fix**: Pass the full received message object: `CommandResponseMessage(command_received=cmd, is_executed=True, body=cmd.body)`.
**Rule for other SDKs**: Check how response messages are constructed — some SDKs require the full received message, others just need IDs.

### 5. Error Field vs is_error Property (Severity: Medium)
**What happened**: `CommandResponseMessage` and `QueryResponseMessage` have `.error` string field (not `.is_error` boolean).
**Root cause**: Different API design from Go where `resp.Error` is checked.
**Fix**: Check `if resp.error:` (truthy string check) instead of `if resp.is_error:`.
**Rule for other SDKs**: Verify the exact error checking mechanism for RPC responses in each SDK.

### 6. EventSendResult.sent Check (Severity: High)
**What happened**: `publish_event_store()` returns `EventSendResult` which has a `.sent` boolean that must be checked.
**Root cause**: Events Store requires server confirmation, unlike fire-and-forget events.
**Fix**: Only call `record_send()` when `result.sent == True` (Lesson 7 from Go).
**Rule for other SDKs**: Always verify the send result for events_store pattern.

### 7. QueueSendResult.is_error Check (Severity: Medium)
**What happened**: `send_queue_message()` returns `QueueSendResult` with `.is_error` boolean.
**Root cause**: Queue sends can fail at the server side.
**Fix**: Only call `record_send()` when `not result.is_error`.
**Rule for other SDKs**: Each SDK may represent queue send failure differently.

### 8. GIL and Threading Considerations (Severity: Low)
**What happened**: Python's GIL means true parallelism is limited for CPU-bound work, but gRPC calls release the GIL during I/O.
**Root cause**: CPython GIL architecture.
**Fix**: Threading is fine for I/O-bound burn-in work (gRPC calls, network I/O). Simple int counter increments are atomic under GIL. Added explicit locks for compound operations.
**Rule for other SDKs**: JS (single-threaded) needs different concurrency model. Java/C# have true threading.

### 9. EventsStoreType Import Path (Severity: Low)
**What happened**: `EventsStoreType` enum is not exported at top-level `kubemq` package.
**Root cause**: SDK design — enum lives in `kubemq.pubsub.events_store_subscription`.
**Fix**: Import from submodule: `from kubemq.pubsub.events_store_subscription import EventsStoreType`.
**Rule for other SDKs**: Check where subscription configuration enums are exported.

### 10. Channel Listing Methods Differ Per Client (Severity: Low)
**What happened**: Events channels listed via `PubSubClient.list_events_channels()`, queue channels via `QueuesClient.list_queues_channels()`, etc.
**Root cause**: 3-client architecture means channel operations are distributed.
**Fix**: Cleanup must call list/delete on all 3 clients across all 5 channel types.
**Rule for other SDKs**: Map out which client handles which channel type for cleanup operations.

### 11. Python Project Layout for Sub-Package Burn-In (Severity: Medium)
**What happened**: `pyproject.toml` in `burnin/` directory couldn't find the `burnin` package because `burnin/` IS the package but hatch couldn't detect it.
**Root cause**: Hatch's auto-detection expects a subdirectory matching the project name. When the project root IS the package, `packages = ["."]` doesn't work as expected.
**Fix**: Use `src/` layout — move all source files to `burnin/src/burnin/` and set `packages = ["src/burnin"]` in hatch config.
**Rule for other SDKs**: For JS/Java/C#, use the standard project layout for each ecosystem. Don't fight the build tool.

### 12. Config Dataclass Type Resolution with `from __future__ import annotations` (Severity: Medium)
**What happened**: `_from_dict()` used `dc_class.__dataclass_fields__[f].type` to detect nested dataclasses, but with `from __future__ import annotations`, the type is a string, not the actual class.
**Root cause**: PEP 563 deferred annotations makes all type annotations strings at runtime.
**Fix**: Use an explicit `_NESTED_TYPES` mapping dict instead of introspecting field types.
**Rule for other SDKs**: Python-specific. Other languages have concrete types at runtime.

---

## Verification Results

Verified against live KubeMQ broker v2.11.0 on localhost:50000.

### 30-second soak test
- **Events**: 4071 sent / 4071 received — 0 loss, 0 dups, 0 corruption
- **Events Store**: 4062 sent / 4062 received — 0 loss, 0 dups, 0 corruption
- **Queue Stream**: 2037 sent / 2029 received — 8 in-flight at shutdown, 0 dups
- **Queue Simple**: 2037 sent / 2027 received — 10 in-flight at shutdown, 0 dups
- **Commands**: 816 RPC success, 0 timeout, 0 error — P99 5.6ms
- **Queries**: 816 RPC success, 0 timeout, 0 error — P99 6.4ms
- **Memory**: baseline 63.2MB, peak 63.4MB, growth 1.00x
- **Verdict**: PASSED_WITH_WARNINGS (memory advisory only)

### HTTP Endpoints
- `/health` — returns `{"status": "alive"}` (200)
- `/ready` — returns `{"status": "ready"}` (200) after warmup
- `/status` — returns per-pattern status map
- `/summary` — returns full JSON summary with all fields populated
- `/metrics` — returns Prometheus metrics with all 26 burnin_ metrics
