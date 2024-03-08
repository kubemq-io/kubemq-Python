import logging
from kubemq.client import Client
from kubemq.entities import *


def main():
    with Client(address="localhost:50000", client_id="queue_example", log_level=logging.DEBUG) as client:
        for i in range(100):
            result = client.poll(
                channel="q1",
                max_messages=1,
                wait_timeout_in_seconds=10,
                auto_ack=False,
            )
            if result.is_error:
                print(f"{result.error}")
                return
            for message in result.messages:
                print(f"Id:{message.id}, Body:{message.body.decode('utf-8')}")
                message.ack()



if __name__ == "__main__":
    main()
