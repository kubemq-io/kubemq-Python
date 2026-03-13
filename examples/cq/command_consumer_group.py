import time
from kubemq.cq import *


def make_handler(name: str, client):
    def handler(request: CommandMessageReceived):
        print(f"[{name}] Received command: {request.body.decode('utf-8')}")
        client.send_response_message(
            CommandResponseMessage(command_received=request, is_executed=True)
        )
    return handler


def on_error(err: str):
    print(f"Error: {err}")


def main():
    client = Client(address="localhost:50000")
    cancel = CancellationToken()

    client.subscribe_to_commands(
        subscription=CommandsSubscription(
            channel="cmd-group",
            group="handlers",
            on_receive_command_callback=make_handler("Handler-1", client),
            on_error_callback=on_error,
        ),
        cancel=cancel,
    )
    client.subscribe_to_commands(
        subscription=CommandsSubscription(
            channel="cmd-group",
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
                channel="cmd-group",
                body=f"Command #{i + 1}".encode(),
                timeout_in_seconds=10,
            )
        )
        print(f"Command #{i + 1} executed: {response.is_executed}")

    cancel.cancel()
    client.close()


if __name__ == "__main__":
    main()
