import time
from kubemq.cq import *


def main():
    client = Client(address="localhost:50000")

    try:
        response = client.send_command_request(
            CommandMessage(
                channel="no-responder",
                body=b"this will time out",
                timeout_in_seconds=3,
            )
        )
        print(f"Executed: {response.is_executed}, Error: {response.error}")
    except Exception as e:
        print(f"Command timed out as expected: {e}")
    finally:
        client.close()


if __name__ == "__main__":
    main()
