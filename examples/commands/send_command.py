"""Example: Send command — send a command and receive a response."""

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
            client_id="python-commands-send-command-client",
        ) as client:
            cancel = CancellationToken()

            def on_receive_command(request: CommandMessageReceived) -> None:
                print(f"Responder received: {request.body.decode('utf-8')}")
                client.send_response_message(
                    CommandResponseMessage(
                        command_received=request,
                        is_executed=True,
                    )
                )

            def on_error(err: str) -> None:
                print(f"Error: {err}")

            # Set up a command responder
            client.subscribe_to_commands(
                subscription=CommandsSubscription(
                    channel="python-commands.send-command",
                    on_receive_command_callback=on_receive_command,
                    on_error_callback=on_error,
                ),
                cancel=cancel,
            )
            time.sleep(1)

            # Send a command and get the response
            response = client.send_command_request(
                CommandMessage(
                    channel="python-commands.send-command",
                    body=b"hello kubemq, please reply!",
                    timeout_in_seconds=10,
                )
            )
            print(
                f"Response: executed={response.is_executed}, "
                f"timestamp={response.timestamp}, error={response.error}"
            )

            cancel.cancel()
    except KubeMQConnectionError as e:
        print(f"Connection error: {e}")
    except KubeMQTimeoutError as e:
        print(f"Timeout error: {e}")
    except KubeMQError as e:
        print(f"KubeMQ error: {e}")


if __name__ == "__main__":
    main()

# Expected output:
# Responder received: hello kubemq, please reply!
# Response: executed=True, timestamp=<timestamp>, error=
