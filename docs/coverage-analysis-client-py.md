# Coverage Analysis: `src/kubemq/core/client.py`

Analysis of uncovered lines from the coverage report, with description, bug risk, and test recommendations.

---

## 1. `_resolve_logger` — Lines 45, 47

### What the code does
- **Line 45**: Returns `config.logger` when the user provides a custom logger.
- **Line 47**: Returns `StdLibLoggerAdapter` when `config.log_level` is set (legacy path), and `config.logger` is `None`.

### Potential production bug?
**No.** Standard logger resolution branches. Missing coverage means tests only use the default (NOOP_LOGGER).

### How to cover
- Test with `ClientConfig(logger=custom_mock_logger)` and assert `client._logger is custom_mock_logger`.
- Test with `ClientConfig(log_level=logging.DEBUG)` and assert `client._logger` is a `StdLibLoggerAdapter` with the expected name/level.

---

## 2. `BaseClient._initialize` exception path — Lines 144–146

### What the code does
- **144**: Logs `"Failed to connect: {e}"` when `Transport(connection).initialize()` raises.
- **145**: Re-raises via `from_grpc_error(e)` to convert gRPC errors.
- **146**: End of `except` block.

### Potential production bug?
**Low.** Error handling looks correct. Risk is only if `from_grpc_error` or logging behaves unexpectedly for some exception types.

### How to cover
- Mock `Transport` so `initialize()` raises (e.g. `ConnectionError` or a gRPC error).
- Assert `from_grpc_error` is called and the expected exception type is raised.

---

## 3. `BaseClient.ping` exception path — Lines 172–174

### What the code does
- **172**: Logs when `transport.ping()` raises.
- **173**: Re-raises via `from_grpc_error(e)`.
- **174**: End of `except` block.

### Potential production bug?
**Low.** Same pattern as `_initialize`; error handling appears correct.

### How to cover
- Create a connected client with a mock transport.
- Make `transport.ping()` raise (e.g. `Exception("ping failed")`).
- Assert the appropriate mapped exception is raised.

---

## 4. `BaseClient.set_token` when transport exists — Lines 185–186

### What the code does
- **185**: Checks `self._transport is not None`.
- **186**: Calls `self._transport.set_token(token)`.

### Potential production bug?
**No.** Simple delegation. Current tests likely always have `_transport` set.

### How to cover
- Create a connected client (transport present).
- Call `client.set_token("new-token")`.
- Assert `transport.set_token` was called with `"new-token"`.

---

## 5. `BaseClient.close` subscription drain — Lines 226–235

### What the code does
- **226**: Computes deadline from `callback_completion_timeout`.
- **227–234**: Loop over subscription threads; if `remaining <= 0`, logs timeout and breaks; otherwise calls `thread.join(timeout=remaining)`.
- **235**: `thread.join(timeout=remaining)`.

Requires `_subscription_threads` to be non-empty (e.g. via `_register_subscription_thread` from pubsub/cq sync clients).

### Potential production bug?
**Low.** Drain logic is sound. If timeout is misconfigured or callbacks never finish, shutdown may be slow or incomplete; that’s a config/usage concern, not a logic bug.

### How to cover
- Use a sync pubsub or cq client that registers subscription threads.
- Start a subscription, then call `close()` and assert transport is closed.
- For the timeout path: use a very short `callback_completion_timeout`, a slow callback, and assert the timeout warning is logged and `close()` returns.

---

## 6. `AsyncBaseClient.connect` — Lines 363–372

### What the code does
- **363–367**: Imports `Transport`, builds connection, runs `Transport(connection).initialize()` in a thread via `run_in_thread`.
- **368–370**: On exception, logs and re-raises via `from_grpc_error(e)`.

### Potential production bug?
**Low.** Same connect/error handling pattern as sync client.

### How to cover
- Patch `run_in_thread` to raise an exception.
- Call `await client.connect()` and assert the correct mapped exception type is raised.

---

## 7. `AsyncBaseClient.is_connected` with transport — Line 382

### What the code does
- Returns `self._transport is not None and self._transport.is_connected()` when transport exists.

### Potential production bug?
**No.** Simple property; edge case is that tests might not hit this branch if they mock `connect` and never actually connect.

### How to cover
- Use a real `await client.connect()` (or inject a connected transport) and assert `client.is_connected is True` when transport is connected.

---

## 8. `AsyncBaseClient.ping` exception path — Lines 393–400

### What the code does
- **393–394**: Calls `_ensure_connected()` and asserts transport is not `None`.
- **395–398**: `try`: calls `run_in_thread(self._transport.ping)`.
- **396–398**: `except`: logs and re-raises via `from_grpc_error(e)`.

### Potential production bug?
**Low.** Same pattern as sync `ping` error handling.

### How to cover
- Connect client with mock transport.
- Make `transport.ping` raise when called.
- Assert the appropriate exception is raised from `await client.ping()`.

---

## 9. `AsyncBaseClient.__aexit__` — Line 416 (→ `close`)

### What the code does
- `__aexit__` delegates to `await self.close()` when exiting the context manager.

### Potential production bug?
**No.** Standard context-manager exit; coverage is probably affected by tests that mock `close` instead of going through `__aexit__`.

### How to cover
- Use `async with ConcreteAsyncBaseClient(...) as client:` and let it exit normally.
- Ensure the real `close()` path is exercised (i.e. do not replace `close` with a mock).

---

## 10. `AsyncBaseClient.close` transport error — Lines 420–421

### What the code does
- **420**: Catches any exception from `await self._transport.close_async()`.
- **421**: Logs it with `self._logger.warning` and then clears `self._transport` in `finally`.

### Potential production bug?
**Low.** Errors are logged and transport reference is cleared; shutdown continues. Possible issue: some exotic exception type might propagate if not caught correctly, but current code catches `Exception`.

### How to cover
- Set `transport.close_async` to raise (e.g. `AsyncMock(side_effect=RuntimeError("close failed"))`).
- Call `await client.close()`.
- Assert the warning is logged and `client._transport is None`.

---

## 11. `AsyncBaseClient._ensure_connected` — Lines 438–441

### What the code does
- **438**: Raises `KubeMQClientClosedError` when `self._closed`.
- **439**: Raises `KubeMQConnectionError` when not connected (`not self.is_connected`).
- **440–441**: End of method.

### Potential production bug?
**No.** Same semantics as sync `_ensure_connected`.

### How to cover
- Test with `client._closed = True` and assert `KubeMQClientClosedError`.
- Test with client never connected (no transport or `is_connected` False) and assert `KubeMQConnectionError`.

---

## 12. `NativeAsyncBaseClient.connection_state` when transport present — Line 566

*(Line 566 in the original report; numbering may have shifted. This refers to the branch that returns `self._transport.connection_state`.)*

### What the code does
- Returns `self._transport.connection_state` when `self._transport` is not `None`.
- Otherwise returns `ConnectionState.IDLE`.

### Potential production bug?
**No.** Simple property delegation.

### How to cover
- Connect a NativeAsyncBaseClient (mock or real).
- Assert `client.connection_state` equals the transport’s `connection_state` (e.g. `ConnectionState.READY`).

---

## 13. `NativeAsyncBaseClient.set_token` when transport exists — Lines 590–591

### What the code does
- **589**: Checks `self._transport is not None`.
- **591**: Calls `self._transport.set_token(token)`.

### Potential production bug?
**No.** Same pattern as `BaseClient.set_token`.

### How to cover
- Connect a NativeAsyncBaseClient, call `client.set_token("new-token")`, assert `transport.set_token` was called.

---

## 14. `NativeAsyncBaseClient` state/callback registration — Lines 596–600, 604–605, 609–610, 614–615, 619–620

### What the code does
- **596–598**: `connection_state` when transport exists.
- **604–605, 609–610, 614–615, 619–620**: Bodies of `on_connected`, `on_disconnected`, `on_reconnecting`, `on_reconnected` when transport exists (delegate to transport).

### Potential production bug?
**No.** Simple delegation to transport callbacks.

### How to cover
- Connect client.
- Call `client.on_connected(cb)`, `on_disconnected`, `on_reconnecting`, `on_reconnected`.
- Assert the corresponding transport methods were called with the same callbacks.

---

## 15. `NativeAsyncBaseClient.close` drain with pending tasks — Lines 655–671

### What the code does
- **654–658**: If `_subscription_tasks` is non-empty, waits with `asyncio.wait(tasks, timeout=callback_timeout)`.
- **661–671**: If any tasks are still pending after timeout:
  - **662–667**: Logs warning.
  - **668–670**: Cancels pending tasks and awaits them via `asyncio.gather(..., return_exceptions=True)`.

### Potential production bug?
**Yes — design/integration issue.**  
`_subscription_tasks` is never populated. `_register_subscription_task` exists but is not called by pubsub, queues, or cq async clients. As a result:

- On `close()`, `_subscription_tasks` is always empty.
- The drain logic (lines 654–671) is never exercised.
- Token cancellation stops new deliveries, but the asyncio tasks that run callbacks are not tracked.
- Transport may be closed while callbacks are still in progress.

**Recommendation:** Either wire `_register_subscription_task` into async subscribe flows, or document and accept that callback drain is best-effort and not guaranteed.

### How to cover
- Unit test: directly call `client._register_subscription_task(task)` before `close()`.
- Create a task that never completes (or completes slowly).
- Call `await client.close()` with a short `callback_completion_timeout`.
- Assert the timeout warning is logged and the task is cancelled.

---

## 16. `NativeAsyncBaseClient._ensure_connected` closing branch — Line 695

### What the code does
- Raises `KubeMQClientClosedError("Client is shutting down")` when `self._closing` is `True`.

### Potential production bug?
**No.** Ensures operations are rejected during shutdown.

### How to cover
- Set `client._closing = True` (or start `close()` in another task).
- Call an operation that uses `_ensure_connected` (e.g. `ping()`) and assert `KubeMQClientClosedError` with “shutting down”.

---

## 17. `NativeAsyncBaseClient._register_subscription_task` / `_unregister_subscription_task` — Lines 742–743, 747

### What the code does
- **740–743**: `_register_subscription_task`: Adds task to `_subscription_tasks` and registers `_subscription_tasks.discard` as a done callback.
- **745–747**: `_unregister_subscription_task`: Removes task from `_subscription_tasks`.

### Potential production bug?
**Yes — dead code.** These methods are never used. They are the intended way to populate `_subscription_tasks`, so the close drain logic is effectively unused (see section 15).

### How to cover
- `_register_subscription_task`: Create a task, call `client._register_subscription_task(task)`, assert task is in `client._subscription_tasks` and done callback is attached.
- `_unregister_subscription_task`: Register a task, then call `_unregister_subscription_task(task)`, assert task is no longer in `_subscription_tasks`.

---

## 18. `NativeAsyncBaseClient.__aexit__` — Line 747

*(Line 747 in original; refers to `await self.close()` inside `__aexit__`.)*

### What the code does
- `__aexit__` calls `await self.close()` when exiting the async context manager.

### Potential production bug?
**No.** Same pattern as `AsyncBaseClient.__aexit__`.

### How to cover
- Use `async with ConcreteNativeAsyncBaseClient(...) as client:` and allow normal exit.
- Ensure the real `close()` is invoked (avoid mocking `close`).

---

## Summary Table

| Region             | Lines    | Bug risk             | Test type                         |
|--------------------|----------|----------------------|-----------------------------------|
| `_resolve_logger`  | 45, 47   | None                 | Config injection                  |
| BaseClient init    | 144–146  | Low                  | Connection failure mock           |
| BaseClient ping    | 172–174  | Low                  | Ping exception mock               |
| BaseClient set_token | 185–186| None                 | Delegation assertion              |
| BaseClient close drain | 226–235 | Low              | Subscription + timeout            |
| AsyncBaseClient connect | 363–372 | Low            | Connect exception mock            |
| AsyncBaseClient is_connected | 382 | None           | Post-connect assertion            |
| AsyncBaseClient ping | 393–400 | Low              | Ping exception mock               |
| AsyncBaseClient __aexit__ | 416  | None                 | Context manager exit              |
| AsyncBaseClient close error | 420–421 | Low          | Transport close exception         |
| AsyncBaseClient _ensure_connected | 438–441 | None        | Closed / not-connected states     |
| NativeAsync connection_state | 566, 596–598 | None      | Property with transport            |
| NativeAsync set_token | 590–591 | None             | Delegation assertion              |
| NativeAsync callbacks | 604–620 | None             | Callback registration             |
| NativeAsync close drain | 655–671 | **Yes**         | Task registration + drain         |
| NativeAsync _ensure_connected | 695 | None           | Shutting-down state               |
| NativeAsync task registration | 742–743, 747 | **Yes** (dead code) | Direct method calls |
| NativeAsync __aexit__ | 747   | None                 | Context manager exit              |

---

## High-priority follow-up

1. **Subscription task tracking (lines 655–671, 742–743)**  
   Either wire `_register_subscription_task` into async subscribe implementations (pubsub, queues, cq) or clearly document that callback drain is not guaranteed on close.

2. **Coverage gaps for error paths**  
   Add focused tests for:
   - BaseClient/AsyncBaseClient init and ping failure paths.
   - Transport `close`/`close_async` failure paths.
   - Subscription drain and timeout in BaseClient close.
   - NativeAsyncBaseClient close with pending tasks.
