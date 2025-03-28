import time
from kubemq.cq import *


def main():
    try:
        client = Client(address="localhost:50000")

        def on_receive_command(request: CommandMessageReceived):
            try:
                print(f"Id:{request.id}, Body:{request.body.decode('utf-8')}")
                response = CommandResponseMessage(
                    command_received=request,
                    is_executed=True,
                )
                client.send_response_message(response)
            except Exception as e:
                print(e)

        def on_error_handler(err: str):
            print(f"{err}")

        client.subscribe_to_commands(
            subscription=CommandsSubscription(
                channel="c1",
                group="",
                on_receive_command_callback=on_receive_command,
                on_error_callback=on_error_handler,
            ),
            cancel=CancellationToken(),
        )
        time.sleep(1)
        response = client.send_command_request(
            CommandMessage(
                channel="c1",
                body=b"hello kubemq, please reply to me!",
                timeout_in_seconds=10,
            )
        )
        print(
            f"Request Execution: {response.is_executed}, Executed at: {response.timestamp}, Error: {response.error}"
        )
    except Exception as e:
        print(e)
        return


if __name__ == "__main__":
    main()
