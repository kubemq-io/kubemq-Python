from kubemq.queues import *


def main():
    with Client(address="localhost:50000") as client:
        send_result = client.send_queues_message(
            QueueMessage(
                channel="q1",
                body=b"some-simple_queue-queue-message",
                metadata="some-metadata",
            )
        )
        print(f"Queue Message Sent: {send_result}")

        result = client.receive_queues_messages(
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
