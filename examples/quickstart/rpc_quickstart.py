"""Quick start: RPC (Commands) — send a command and handle the response."""

from __future__ import annotations

import time

from kubemq import (
    CancellationToken,
    CommandMessage,
    CommandResponseMessage,
    CommandsSubscription,
    CQClient,
)


def main() -> None:
    client = CQClient(address="localhost:50000", client_id="python-rpc-quickstart-client")

    # Subscribe to commands as a handler
    def on_command(cmd) -> None:  # type: ignore[no-untyped-def]
        print(f"Received command: {cmd.body.decode('utf-8')}")
        client.send_response_message(CommandResponseMessage(command_received=cmd, is_executed=True))

    client.subscribe_to_commands(
        subscription=CommandsSubscription(
            channel="python-quickstart-commands",
            on_receive_command_callback=on_command,
            on_error_callback=lambda e: print(f"Error: {e}"),
        ),
        cancel=CancellationToken(),
    )

    # Give the subscription a moment to connect
    time.sleep(1)

    # Send a command and wait for the response
    response = client.send_command(
        CommandMessage(
            channel="python-quickstart-commands",
            body=b"Turn on lights",
            timeout_in_seconds=10,
        )
    )
    print(f"Command executed: {response.is_executed}")

    client.close()


if __name__ == "__main__":
    main()
