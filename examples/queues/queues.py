from time import sleep

from kubemq.queues import *


def example_send_receive():
    client = Client(address="localhost:50000")
    send_result = client.send_queues_message(
        QueueMessage(
            channel="send_receive",
            body=b"message",
        )
    )
    print(f"Queue Message Sent: {send_result}")

    send_receive_result = client.receive_queues_messages(
        channel="send_receive",
        max_messages=1,
        wait_timeout_in_seconds=10,
    )
    for message in send_receive_result.messages:
        print(f"Id:{message.id}, Body:{message.body.decode('utf-8')}")
        message.ack()
    client.close()


def example_send_receive_with_auto_ack():
    client = Client(address="localhost:50000")
    send_result = client.send_queues_message(
        QueueMessage(
            channel="auto_ack",
            body=b"message",
        )
    )
    print(f"Queue Message Sent: {send_result}")

    auto_ack_result = client.receive_queues_messages(
        channel="auto_ack", max_messages=1, wait_timeout_in_seconds=10, auto_ack=True
    )
    for message in auto_ack_result.messages:
        print(f"Id:{message.id}, Body:{message.body.decode('utf-8')}")
    client.close()


def example_send_receive_with_dlq():
    client = Client(address="localhost:50000")
    send_result = client.send_queues_message(
        QueueMessage(
            channel="python_process_queue",
            body="Message".encode("utf-8"),
            metadata="some-metadata",
            attempts_before_dead_letter_queue=4,
            dead_letter_queue="dlq_python_process_queue",
        )
    )
    print(f"Queue Message Sent: {send_result}")

    for i in range(2):
        receive_result = client.receive_queues_messages(
            channel="python_process_queue",
            max_messages=1,
            wait_timeout_in_seconds=2,
        )
        if len(receive_result.messages) == 0:
            print("No more messages")
            break
        for message in receive_result.messages:
            print(
                f"Id:{message.id}, Body:{message.body.decode('utf-8')}, Receive Count:{message.receive_count}"
            )
            message.reject()
    receive_result = client.receive_queues_messages(
        channel="python_process_queue",
        max_messages=1,
        wait_timeout_in_seconds=2,
    )
    if len(receive_result.messages) == 0:
        print("No more messages")
    for message in receive_result.messages:
        print(
            f"Id:{message.id}, Body:{message.body.decode('utf-8')}, Receive Count:{message.receive_count}"
        )
        message.re_queue("python_process_re_queue")

    client.close()


def example_send_receive_with_delay():
    client = Client(address="localhost:50000")
    send_result = client.send_queues_message(
        QueueMessage(channel="delay", body=b"message with delay", delay_in_seconds=5)
    )
    print(f"Queue Message Sent: {send_result}")

    delay_result = client.receive_queues_messages(
        channel="delay",
        max_messages=1,
        wait_timeout_in_seconds=1,
    )
    print(f"Received {len(delay_result.messages)} messages")
    print("Waiting for 6 seconds")
    sleep(6)
    for message in delay_result.messages:
        print(f"Id:{message.id}, Body:{message.body.decode('utf-8')}")
        message.ack()
    client.close()


def example_send_receive_with_expiration():
    client = Client(address="localhost:50000")
    send_result = client.send_queues_message(
        QueueMessage(
            channel="expiration",
            body=b"message with expiration",
            expiration_in_seconds=5,
        )
    )
    print(f"Queue Message Sent: {send_result}")
    print("Waiting for 6 seconds")
    sleep(6)
    expiration_result = client.receive_queues_messages(
        channel="expiration",
        max_messages=1,
        wait_timeout_in_seconds=1,
    )
    print(f"Received {len(expiration_result.messages)} messages")
    client.close()


def example_with_message_ack():
    client = Client(address="localhost:50000")
    send_result = client.send_queues_message(
        QueueMessage(
            channel="message_ack",
            body=b"message with ack",
        )
    )
    print(f"Queue Message Sent: {send_result}")

    message_ack_result = client.receive_queues_messages(
        channel="message_ack",
        max_messages=1,
        wait_timeout_in_seconds=10,
        auto_ack=False,
    )
    for message in message_ack_result.messages:
        print(f"Id:{message.id}, Body:{message.body.decode('utf-8')}")
        message.ack()
    client.close()


def example_with_message_reject():
    client = Client(address="localhost:50000")
    send_result = client.send_queues_message(
        QueueMessage(
            channel="message_reject",
            body=b"message with reject",
        )
    )
    print(f"Queue Message Sent: {send_result}")

    message_reject_result = client.receive_queues_messages(
        channel="message_reject",
        max_messages=1,
        wait_timeout_in_seconds=10,
        auto_ack=False,
    )
    for message in message_reject_result.messages:
        print(f"Id:{message.id}, Body:{message.body.decode('utf-8')}")
        message.reject()
    message_reject_result = client.receive_queues_messages(
        channel="message_reject",
        max_messages=1,
        wait_timeout_in_seconds=10,
        auto_ack=False,
    )
    for message in message_reject_result.messages:
        print(f"Id:{message.id}, Body:{message.body.decode('utf-8')}")
        message.ack()
    client.close()


def example_with_message_requeue():
    client = Client(address="localhost:50000")
    send_result = client.send_queues_message(
        QueueMessage(
            channel="message_requeue",
            body=b"message with requeue",
        )
    )
    print(f"Queue Message Sent: {send_result}")

    message_requeue_result = client.receive_queues_messages(
        channel="message_requeue",
        max_messages=1,
        wait_timeout_in_seconds=10,
        auto_ack=False,
    )
    for message in message_requeue_result.messages:
        print(f"Id:{message.id}, Body:{message.body.decode('utf-8')}")
        message.re_queue("requeue_channel")
    message_requeue_result = client.receive_queues_messages(
        channel="requeue_channel",
        max_messages=1,
        wait_timeout_in_seconds=10,
        auto_ack=False,
    )
    for message in message_requeue_result.messages:
        print(f"Id:{message.id}, Body:{message.body.decode('utf-8')}")
        message.ack()


def example_with_ack_all():
    client = Client(address="localhost:50000")
    for i in range(10):
        send_result = client.send_queues_message(
            QueueMessage(
                channel="ack_all",
                body=f"Message {i + 1}".encode("utf-8"),
                metadata="some-metadata",
            )
        )
        print(f"Queue Message Sent: {send_result}")

    ack_all_result = client.receive_queues_messages(
        channel="ack_all",
        max_messages=10,
        wait_timeout_in_seconds=10,
    )
    ack_all_result.ack_all()
    for message in ack_all_result.messages:
        print(f"Id:{message.id}, Body:{message.body.decode('utf-8')}")


def example_with_reject_all():
    client = Client(address="localhost:50000")
    for i in range(10):
        send_result = client.send_queues_message(
            QueueMessage(
                channel="reject_all",
                body=f"Message {i + 1}".encode("utf-8"),
            )
        )
        print(f"Queue Message Sent: {send_result}")

    reject_all_result = client.receive_queues_messages(
        channel="q1",
        max_messages=10,
        wait_timeout_in_seconds=10,
    )
    for message in reject_all_result.messages:
        print(f"Id:{message.id}, Body:{message.body.decode('utf-8')}")
    reject_all_result.reject_all()

    reject_all_result = client.receive_queues_messages(
        channel="reject_all",
        max_messages=10,
        wait_timeout_in_seconds=10,
    )
    for message in reject_all_result.messages:
        print(f"Id:{message.id}, Body:{message.body.decode('utf-8')}")
    reject_all_result.ack_all()
    client.close()


def example_with_requeue_all():
    client = Client(address="localhost:50000")
    for i in range(10):
        send_result = client.send_queues_message(
            QueueMessage(
                channel="requeue_all",
                body=f"Message {i + 1}".encode("utf-8"),
            )
        )
        print(f"Queue Message Sent: {send_result}")

    requeue_all_result = client.receive_queues_messages(
        channel="requeue_all",
        max_messages=10,
        wait_timeout_in_seconds=10,
    )
    for message in requeue_all_result.messages:
        print(f"Id:{message.id}, Body:{message.body.decode('utf-8')}")
    requeue_all_result.re_queue_all("requeue_channel")

    requeue_all_result = client.receive_queues_messages(
        channel="requeue_channel",
        max_messages=10,
        wait_timeout_in_seconds=10,
    )
    for message in requeue_all_result.messages:
        print(f"Id:{message.id}, Body:{message.body.decode('utf-8')}")
    requeue_all_result.ack_all()
    client.close()


def example_with_visibility():
    client = Client(address="localhost:50000")
    send_result = client.send_queues_message(
        QueueMessage(
            channel="visibility_channel",
            body=b"message with visibility",
        )
    )
    print(f"Queue Message Sent: {send_result}")

    result = client.receive_queues_messages(
        channel="visibility_channel",
        max_messages=1,
        wait_timeout_in_seconds=10,
        auto_ack=False,
        visibility_seconds=2,
    )
    for message in result.messages:
        print(f"Id:{message.id}, Body:{message.body.decode('utf-8')}")
        time.sleep(1)
        message.ack()
    client.close()


def example_with_visibility_expired():
    client = Client(address="localhost:50000")
    send_result = client.send_queues_message(
        QueueMessage(
            channel="visibility_channel_expired",
            body=b"message with visibility expired",
        )
    )
    print(f"Queue Message Sent: {send_result}")

    result = client.receive_queues_messages(
        channel="visibility_channel_expired",
        max_messages=1,
        wait_timeout_in_seconds=10,
        auto_ack=False,
        visibility_seconds=2,
    )
    for message in result.messages:
        try:
            print(f"Id:{message.id}, Body:{message.body.decode('utf-8')}")
            time.sleep(3)
            message.ack()
        except Exception as err:
            print(err)
    client.close()


def example_with_visibility_extension():
    client = Client(address="localhost:50000")
    send_result = client.send_queues_message(
        QueueMessage(
            channel="visibility_channel_extension",
            body=b"message with visibility extension",
        )
    )
    print(f"Queue Message Sent: {send_result}")

    result = client.receive_queues_messages(
        channel="visibility_channel_extension",
        max_messages=1,
        wait_timeout_in_seconds=10,
        auto_ack=False,
        visibility_seconds=2,
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
    client.close()


def example_with_wait_pull():
    client = Client(address="localhost:50000")
    send_result = client.send_queues_message(
        QueueMessage(
            channel="wait_pull",
            body=b"some-simple_queue-queue-message",
            metadata="some-metadata",
        )
    )
    print(f"Queue Message Sent: {send_result}")

    waitingResult = client.waiting("wait_pull", 1, 10)

    if waitingResult.is_error:
        print(f"{waitingResult.error}")
        return
    for message in waitingResult.messages:
        print(f" Wait Id:{message.id}, Body:{message.body.decode('utf-8')}")

    pullResult = client.pull("wait_pull", 1, 10)
    if pullResult.is_error:
        print(f"{pullResult.error}")
        return
    for message in pullResult.messages:
        print(f"Pull Id:{message.id}, Body:{message.body.decode('utf-8')}")
    client.close()


if __name__ == "__main__":
    try:
        example_send_receive()
        example_send_receive_with_auto_ack()
        example_send_receive_with_dlq()
        example_send_receive_with_delay()
        example_send_receive_with_expiration()
        example_with_message_ack()
        example_with_message_reject()
        example_with_message_requeue()
        example_with_ack_all()
        example_with_reject_all()
        example_with_requeue_all()
        example_with_visibility()
        example_with_visibility_expired()
        example_with_visibility_extension()
        example_with_wait_pull()
    except Exception as err:
        print(err)
    finally:
        sleep(2)
