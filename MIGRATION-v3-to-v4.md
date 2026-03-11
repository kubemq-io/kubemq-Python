# Migrating from KubeMQ Python SDK v3 to v4

## Breaking Changes

### 1. Async-First Architecture
- v3: Sync-only API with thread-wrapped async methods.
- v4: Native async clients plus sync wrappers. Import via `from kubemq import AsyncPubSubClient` (or `from kubemq.pubsub import AsyncClient`).
- **Action:** Switch to `async with AsyncPubSubClient(...) as client:` for async code paths.

### 2. Client Construction
- v3: `KubeMQ(address="...")` monolithic client.
- v4: Domain-specific clients: `PubSubClient`, `QueuesClient`, `CQClient`.
- **Action:** Replace `KubeMQ(...)` with the appropriate domain client.

### 3. Exception Hierarchy
- v3: Bare `Exception` and `ConnectionError` raised.
- v4: Typed `KubeMQError` hierarchy with error codes.
- **Action:** Update `except` clauses to catch `KubeMQError` subtypes.

### 4. Configuration
- v3: Positional arguments and mixed config styles.
- v4: `ClientConfig` dataclass with explicit keyword arguments.
- **Action:** Use `ClientConfig(address="...", ...)` or pass kwargs to client constructors.

### 5. Python Version Requirement
- v3: Python 3.9+
- v4: Python 3.11+
- **Action:** Upgrade to Python 3.11 or later before upgrading the SDK.

## Deprecated v3 APIs in v4

| v3 API | v4 Replacement | Removal Target |
|--------|---------------|----------------|
| `PubSubClient.ping_async()` | `AsyncPubSubClient.ping()` | v5.0.0 |
| `PubSubClient.send_events_message_async()` | `AsyncPubSubClient.send_event()` | v5.0.0 |
| `PubSubClient.send_events_store_message_async()` | `AsyncPubSubClient.send_event_store()` | v5.0.0 |
| `QueuesClient.ping_async()` | `AsyncQueuesClient.ping()` | v5.0.0 |
| `QueuesClient.send_queues_message_async()` | `AsyncQueuesClient.send_queue_message()` | v5.0.0 |
| `CQClient.ping_async()` | `AsyncCQClient.ping()` | v5.0.0 |
| `CQClient.send_command_request_async()` | `AsyncCQClient.send_command()` | v5.0.0 |
| `CQClient.send_query_request_async()` | `AsyncCQClient.send_query()` | v5.0.0 |
| `CQClient.send_response_message_async()` | `AsyncCQClient.send_response()` | v5.0.0 |

## Step-by-Step Migration

1. Update `kubemq` dependency to `>=4.0.0`.
2. Ensure Python 3.11+ is installed.
3. Replace client construction.
4. Update exception handling.
5. Migrate async code to native async clients.
6. Run with `python -Wd` to see remaining deprecation warnings.
