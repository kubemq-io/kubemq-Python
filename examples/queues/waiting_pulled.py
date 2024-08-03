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

        waitingResult = client.waiting("q1", 1, 10)

        if waitingResult.is_error:
            print(f"{waitingResult.error}")
            return
        for message in waitingResult.messages:
            print(f"Id:{message.id}, Body:{message.body.decode('utf-8')}")

        pullResult = client.pull("q1", 1, 10)
        if pullResult.is_error:
            print(f"{pullResult.error}")
            return
        for message in pullResult.messages:
            print(f"Id:{message.id}, Body:{message.body.decode('utf-8')}")


if __name__ == "__main__":
    main()
