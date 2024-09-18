import time
import threading
from random import random

from kubemq.queues import *

def example_with_visibility():
    with Client(address="localhost:50000") as client:
        send_result = client.send_queues_message(
            QueueMessage(
                channel="q1",
                body=b"message with visibility",
            )
        )
        print(f"Queue Message Sent: {send_result}")

        result = client.receive_queues_messages(
            channel="q1",
            max_messages=1,
            wait_timeout_in_seconds=10,
            auto_ack=False,
            visibility_seconds=2
        )
        for message in result.messages:
            print(f"Id:{message.id}, Body:{message.body.decode('utf-8')}")
            time.sleep(1)
            message.ack()

def example_with_visibility_expired():
    with Client(address="localhost:50000") as client:
        send_result = client.send_queues_message(
            QueueMessage(
                channel="q1",
                body=b"message with visibility expired",
            )
        )
        print(f"Queue Message Sent: {send_result}")

        result = client.receive_queues_messages(
            channel="q1",
            max_messages=1,
            wait_timeout_in_seconds=10,
            auto_ack=False,
            visibility_seconds=2
        )
        for message in result.messages:
            try:
                print(f"Id:{message.id}, Body:{message.body.decode('utf-8')}")
                time.sleep(3)
                message.ack()
            except Exception as err:
                print(err)

def example_with_visibility_extention():
    with Client(address="localhost:50000") as client:
        send_result = client.send_queues_message(
            QueueMessage(
                channel="q1",
                body=b"message with visibility extention",
            )
        )
        print(f"Queue Message Sent: {send_result}")

        result = client.receive_queues_messages(
            channel="q1",
            max_messages=1,
            wait_timeout_in_seconds=10,
            auto_ack=False,
            visibility_seconds=2

        )
        for message in result.messages:
            try:
                print(f"Id:{message.id}, Body:{message.body.decode('utf-8')}")
                time.sleep(1)
                message.extend_visibility_timer(3)
                time.sleep(2)
                message.ack()
            except Exception as err:
                print(err)

if __name__ == "__main__":
    try:
        example_with_visibility()
        example_with_visibility_expired()
        example_with_visibility_extention()
    except Exception as err:
        print(err)
    finally:
        time.sleep(1)