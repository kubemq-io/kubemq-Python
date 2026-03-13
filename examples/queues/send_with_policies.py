"""Send a single message with expiration, delay, and DLQ policy combined."""

from kubemq.queues import *


def main():
    with Client(address="localhost:50000") as client:
        result = client.send_queues_message(
            QueueMessage(
                channel="policy-demo",
                body=b"message with policies",
                metadata="policy-test",
                expiration_in_seconds=60,
                delay_in_seconds=5,
                attempts_before_dead_letter_queue=3,
                dead_letter_queue="policy-demo-dlq",
            )
        )
        print(f"Sent with policies: {result}")
        print(
            "  Expiration: 60s, Delay: 5s, Max attempts: 3, DLQ: policy-demo-dlq"
        )


if __name__ == "__main__":
    main()
