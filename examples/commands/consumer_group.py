"""Example: Consumer group — load-balance commands across multiple handlers."""

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


def make_handler(name: str, client: CQClient):  # type: ignore[no-untyped-def]
    def handler(request: CommandMessageReceived) -> None:
        print(f"[{name}] Received command: {request.body.decode('utf-8')}")
        client.send_response_message(
            CommandResponseMessage(command_received=request, is_executed=True)
        )

    return handler


def on_error(err: str) -> None:
    print(f"Error: {err}")


def main() -> None:
    with CQClient(
        address="localhost:50000",
        client_id="python-commands-consumer-group-client",
    ) as client:
        cancel = CancellationToken()

        client.subscribe_to_commands(
            subscription=CommandsSubscription(
                channel="python-commands.consumer-group",
                group="handlers",
                on_receive_command_callback=make_handler("Handler-1", client),
                on_error_callback=on_error,
            ),
            cancel=cancel,
        )
        client.subscribe_to_commands(
            subscription=CommandsSubscription(
                channel="python-commands.consumer-group",
                group="handlers",
                on_receive_command_callback=make_handler("Handler-2", client),
                on_error_callback=on_error,
            ),
            cancel=cancel,
        )

        time.sleep(1)
        for i in range(4):
            response = client.send_command_request(
                CommandMessage(
                    channel="python-commands.consumer-group",
                    body=f"Command #{i + 1}".encode(),
                    timeout_in_seconds=10,
                )
            )
            print(f"Command #{i + 1} executed: {response.is_executed}")

        cancel.cancel()


if __name__ == "__main__":
    main()
