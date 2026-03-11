# Commands (RPC)

Request/reply where the sender expects confirmation of execution
(no response payload).

## Overview

- **Delivery:** At-most-once request/reply
- **Pattern:** Command — the responder confirms execution with a boolean
- **Use cases:** Device control, configuration changes, remote actions

## Send a command

```python
from kubemq import CQClient, CommandMessage

with CQClient(address="localhost:50000") as client:
    response = client.send_command(
        CommandMessage(
            channel="device-control",
            body=b"turn_on_lights",
            timeout_in_seconds=10,
        )
    )
    print(f"Executed: {response.is_executed}")
```

## Handle commands

```python
import time
from kubemq import (
    CQClient, CommandsSubscription,
    CommandResponseMessage, CancellationToken,
)

client = CQClient(address="localhost:50000")

def on_command(cmd):
    print(f"Command: {cmd.body.decode('utf-8')}")
    client.send_response_message(
        CommandResponseMessage(command_received=cmd, is_executed=True)
    )

client.subscribe_to_commands(
    subscription=CommandsSubscription(
        channel="device-control",
        on_receive_command_callback=on_command,
        on_error_callback=lambda e: print(f"Error: {e}"),
    ),
    cancel=CancellationToken(),
)
time.sleep(60)
client.close()
```

## See Also

- [Queries](queries.md) — request/reply with data response
- [API Reference](../api/cq.md)
- [Examples](https://github.com/kubemq-io/kubemq-Python/tree/v4/examples/cq)
