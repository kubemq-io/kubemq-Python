# Implementing Request-Reply with Commands

This tutorial shows you how to build synchronous request-reply interactions using KubeMQ commands in Python. Unlike events (fire-and-forget) and queues (async processing), commands give you a direct response from the handler — similar to an RPC call. By the end, you'll have a working command handler, a client that sends commands, and robust timeout and error handling — using both sync and async approaches.

## What You'll Build

A request-reply system where:
1. A responder subscribes to a command channel and processes incoming requests
2. A client sends a command and blocks until the response arrives
3. Timeouts prevent the client from hanging if the responder is unavailable
4. Errors are handled gracefully on both sides

## Prerequisites

- **Python 3.9 or later** ([download](https://www.python.org/downloads/))
- **KubeMQ server** running on `localhost:50000`:
  ```bash
  docker run -d -p 50000:50000 -p 9090:9090 kubemq/kubemq
  ```
- A virtual environment:
  ```bash
  mkdir kubemq-commands-tutorial && cd kubemq-commands-tutorial
  python -m venv .venv
  source .venv/bin/activate
  pip install kubemq
  ```

## When to Use Commands vs. Events vs. Queues

Choosing the right messaging pattern matters:

| Pattern | Use When |
|---|---|
| **Events** | You want to broadcast information and don't need confirmation (logs, metrics, notifications). |
| **Queues** | Work must be processed reliably but the producer doesn't need an immediate answer (background jobs). |
| **Commands** | You need a synchronous response — the caller waits for the result before continuing (API gateways, orchestration, validation). |

Commands are blocking by nature. The sender waits until the handler responds or the timeout expires. This makes them perfect for operations where the caller needs to know the outcome right away.

## Step 1: Create a Command Handler (Responder)

The responder listens on a channel and processes incoming commands. Think of it as a remote function that other services can call.

Create `commands_tutorial.py`:

```python
from __future__ import annotations

import time

from kubemq import (
    CancellationToken,
    CommandMessage,
    CommandMessageReceived,
    CommandResponseMessage,
    CommandsSubscription,
    CQClient,
)


def main() -> None:
    with CQClient(
        address="localhost:50000",
        client_id="commands-tutorial-client",
    ) as client:

        cancel = CancellationToken()

        def on_receive_command(cmd: CommandMessageReceived) -> None:
            print("[Handler] Received command:")
            print(f"  ID:   {cmd.id}")
            print(f"  Body: {cmd.body.decode('utf-8')}")

            client.send_response_message(
                CommandResponseMessage(
                    command_received=cmd,
                    is_executed=True,
                )
            )
            print("[Handler] Response sent (executed=True)")

        def on_error(err: str) -> None:
            print(f"[Handler] Subscription error: {err}")

        client.subscribe_to_commands(
            subscription=CommandsSubscription(
                channel="tutorial.commands",
                on_receive_command_callback=on_receive_command,
                on_error_callback=on_error,
            ),
            cancel=cancel,
        )

        print("[Handler] Listening for commands on: tutorial.commands")

        # ... sender code will go here (Step 2) ...


if __name__ == "__main__":
    main()
```

**How the handler works:**

- `CQClient` is the client for commands and queries (the "CQ" in CQClient). It uses a separate connection from the pub/sub and queue clients because command/query routing works differently.
- `CommandsSubscription` defines the channel to listen on and two callbacks. The command callback fires for every incoming command; the error callback fires on subscription problems.
- `CommandResponseMessage` is the reply. You pass in the received command object (`command_received=cmd`) — this automatically sets the routing fields (request ID, response-to address) that KubeMQ needs to deliver the response to the correct sender.
- `is_executed=True` tells the sender that the command was processed successfully. Set it to `False` for failures.

## Step 2: Send Commands from Client

Add the sender code after the subscription block:

```python
        time.sleep(1)

        print("\n[Client] Sending command...")
        response = client.send_command(
            CommandMessage(
                channel="tutorial.commands",
                body=b"process-order-123",
                timeout_in_seconds=10,
            )
        )

        print("[Client] Response received:")
        print(f"  Executed:  {response.is_executed}")
        print(f"  Timestamp: {response.timestamp}")
        print(f"  Error:     {response.error}")

        time.sleep(1)
        cancel.cancel()
```

**What's happening:**

- `CommandMessage` takes a channel, body (as `bytes`), and a **timeout**. The timeout is essential — it defines how long the sender will wait for a response before giving up.
- `send_command` is a **blocking call**. It sends the command to the server, which routes it to a handler, and waits for the response. This is the "request-reply" pattern in action.
- The returned `CommandResponseMessage` contains `is_executed` (bool indicating success), `timestamp` (when the response was generated), and `error` (error description, if any).

## Step 3: Handle Timeouts

What happens when no handler is available? The command times out. Your code should handle this gracefully rather than crashing.

```python
from __future__ import annotations

from kubemq import (
    CommandMessage,
    CQClient,
    KubeMQTimeoutError,
)


def main() -> None:
    with CQClient(
        address="localhost:50000",
        client_id="commands-timeout-tutorial",
    ) as client:

        print("[Client] Sending command with 3-second timeout (no handler exists)...")

        try:
            response = client.send_command(
                CommandMessage(
                    channel="tutorial.commands.nobody-home",
                    body=b"hello?",
                    timeout_in_seconds=3,
                )
            )
            print(f"[Client] Unexpected success: executed={response.is_executed}")
        except KubeMQTimeoutError as e:
            print(f"[Client] Command timed out as expected: {e}")
        except Exception as e:
            print(f"[Client] Error: {e}")


if __name__ == "__main__":
    main()
```

**Why timeouts matter:**

- Without a timeout, a missing handler would cause the sender to block indefinitely. Explicit timeouts make your system predictable.
- Choose timeout values based on your expected processing time. A fast lookup might use 2 seconds; a complex computation might need 30.
- `KubeMQTimeoutError` is a specific exception class you can catch separately from connection errors or validation errors.

### Expected Output

```
[Client] Sending command with 3-second timeout (no handler exists)...
[Client] Command timed out as expected: ...
```

## Step 4: Add Error Handling

Let's put it all together in a production-ready example with comprehensive error handling on both sides:

```python
from __future__ import annotations

import time

from kubemq import (
    CancellationToken,
    CommandMessage,
    CommandMessageReceived,
    CommandResponseMessage,
    CommandsSubscription,
    CQClient,
    KubeMQConnectionError,
    KubeMQError,
    KubeMQTimeoutError,
)


def main() -> None:
    try:
        with CQClient(
            address="localhost:50000",
            client_id="commands-error-tutorial",
        ) as client:

            cancel = CancellationToken()

            def on_receive_command(cmd: CommandMessageReceived) -> None:
                body = cmd.body.decode("utf-8")
                print(f"[Handler] Processing: {body}")

                try:
                    if "invalid" in body:
                        client.send_response_message(
                            CommandResponseMessage(
                                command_received=cmd,
                                is_executed=False,
                                error="validation failed: invalid order ID",
                            )
                        )
                    else:
                        client.send_response_message(
                            CommandResponseMessage(
                                command_received=cmd,
                                is_executed=True,
                            )
                        )
                except Exception as e:
                    print(f"[Handler] Failed to send response: {e}")

            def on_error(err: str) -> None:
                print(f"[Handler] Subscription error: {err}")

            client.subscribe_to_commands(
                subscription=CommandsSubscription(
                    channel="tutorial.commands.robust",
                    on_receive_command_callback=on_receive_command,
                    on_error_callback=on_error,
                ),
                cancel=cancel,
            )
            print("[Handler] Ready\n")
            time.sleep(1)

            # Send multiple commands to test both success and failure paths.
            test_commands = [
                ("process-order-100", 10),
                ("invalid-order-xyz", 10),
            ]

            for body, timeout in test_commands:
                print(f"[Client] Sending: {body}")
                try:
                    response = client.send_command(
                        CommandMessage(
                            channel="tutorial.commands.robust",
                            body=body.encode(),
                            timeout_in_seconds=timeout,
                        )
                    )
                    print(f"[Client] Executed: {response.is_executed}")
                    if response.error:
                        print(f"[Client] Error:    {response.error}")
                    print()
                except KubeMQTimeoutError:
                    print("[Client] Timed out — no handler responded\n")

            time.sleep(1)
            cancel.cancel()

    except KubeMQConnectionError as e:
        print(f"Connection error: {e}")
    except KubeMQError as e:
        print(f"KubeMQ error: {e}")


if __name__ == "__main__":
    main()
```

### Expected Output

```
[Handler] Ready

[Client] Sending: process-order-100
[Handler] Processing: process-order-100
[Client] Executed: True

[Client] Sending: invalid-order-xyz
[Handler] Processing: invalid-order-xyz
[Client] Executed: False
[Client] Error:    validation failed: invalid order ID
```

## Async Version

For `asyncio`-based applications, use `AsyncCQClient`:

```python
from __future__ import annotations

import asyncio

from kubemq import (
    AsyncCancellationToken,
    AsyncCQClient,
    CommandMessage,
    CommandResponseMessage,
    CommandsSubscription,
)


async def main() -> None:
    async with AsyncCQClient(
        address="localhost:50000",
        client_id="commands-async-tutorial",
    ) as client:

        token = AsyncCancellationToken()

        async def command_handler() -> None:
            async for cmd in client.subscribe_to_commands(
                subscription=CommandsSubscription(
                    channel="tutorial.commands.async",
                    on_receive_command_callback=lambda c: None,
                    on_error_callback=lambda e: print(f"Error: {e}"),
                ),
                cancellation_token=token,
            ):
                print(f"[Handler] Received: {cmd.body.decode('utf-8')}")
                await client.send_response(
                    CommandResponseMessage(
                        command_received=cmd,
                        is_executed=True,
                    )
                )

        handler_task = asyncio.create_task(command_handler())
        await asyncio.sleep(1)

        print("[Client] Sending async command...")
        response = await client.send_command(
            CommandMessage(
                channel="tutorial.commands.async",
                body=b"async-order-456",
                timeout_in_seconds=10,
            )
        )
        print(f"[Client] Executed: {response.is_executed}")

        token.cancel()
        handler_task.cancel()
        try:
            await handler_task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    asyncio.run(main())
```

**Key differences in the async version:**

- The subscriber uses an `async for` loop that yields commands as they arrive. This integrates naturally with `asyncio`'s concurrency model.
- The handler runs as a background task via `asyncio.create_task`, allowing the main coroutine to continue and send commands.
- `send_response` (not `send_response_message`) is the async client's method for sending responses.
- Cleanup involves cancelling both the `AsyncCancellationToken` and the background task.

## Key Concepts

| Concept | What It Means |
|---|---|
| **Request-reply** | The sender blocks until the handler responds, like a function call across services. |
| **Timeout** | A hard limit on how long the sender waits. Always set one. |
| **CQClient** | The client for commands and queries — separate from PubSubClient and QueuesClient. |
| **CommandResponseMessage** | The reply object. Pass `command_received=cmd` to auto-set routing fields. |
| **KubeMQTimeoutError** | Raised when no handler responds before the timeout. Catch it explicitly. |
| **Consumer group** | Set `group` on the subscription to load-balance commands across multiple handlers. |

## Next Steps

- **Consumer groups**: Distribute command handling across multiple instances for load balancing. See [`examples/commands/consumer_group.py`](../../examples/commands/consumer_group.py).
- **Queries**: If you need to return data (not just success/failure), use queries instead. See [`examples/queries/send_query.py`](../../examples/queries/send_query.py).
- **Events**: For one-way notifications that don't need a response, see [Getting Started with Events](getting-started-events.md).
- **Queues**: For reliable async task processing, see [Building a Task Queue](building-a-task-queue.md).
