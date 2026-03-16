"""Example: Handle command — subscribe and respond to incoming commands."""

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
        client_id="python-commands-handle-command-client",
    ) as client:
        cancel = CancellationToken()

        def on_receive_command(request: CommandMessageReceived) -> None:
            """Handle incoming command and send response."""
            try:
                body = request.body.decode("utf-8")
                print(f"Handling command: Id={request.id}, Body={body}")

                # Process the command (simulate work)
                success = True

                # Send response back
                client.send_response_message(
                    CommandResponseMessage(
                        command_received=request,
                        is_executed=success,
                    )
                )
                print(f"  Response sent: executed={success}")
            except Exception as e:
                print(f"  Error handling command: {e}")

        def on_error(err: str) -> None:
            print(f"Subscription error: {err}")

        # Subscribe to commands
        client.subscribe_to_commands(
            subscription=CommandsSubscription(
                channel="python-commands.handle-command",
                on_receive_command_callback=on_receive_command,
                on_error_callback=on_error,
            ),
            cancel=cancel,
        )
        print("Listening for commands on 'python-commands.handle-command'...")
        time.sleep(1)

        # Send a test command
        response = client.send_command_request(
            CommandMessage(
                channel="python-commands.handle-command",
                body=b"process this task",
                timeout_in_seconds=10,
            )
        )
        print(f"Command result: executed={response.is_executed}")

        time.sleep(1)
        cancel.cancel()


if __name__ == "__main__":
    main()
