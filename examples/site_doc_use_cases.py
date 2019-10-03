import time

from kubemq.queue.message_queue import MessageQueue
from kubemq.grpc import QueueMessagePolicy
from kubemq.queue.message import Message
from kubemq.queue.stream_request_type import StreamRequestType

queue_name = "QueueNameee"
client_id = "ClientID"
kube_add = "localhost:50000"
max_number_messages = 32
max_timeout = 1


def send_message_to_queue():
    queue = MessageQueue(queue_name, client_id, kube_add)
    message = create_queue_message("someMeta", "some-simple_queue-queue-message".encode('UTF-8'))
    queue_send_response = queue.send_queue_message(message)
    print("finished sending to queue answer. message_id: %s, body: %s" % (queue_send_response.message_id, message.body))


def send_message_to_a_queue_with_expiration():
    queue = MessageQueue(queue_name, client_id, kube_add, max_number_messages, max_timeout)
    policy = QueueMessagePolicy()
    policy.ExpirationSeconds = 5
    message = create_queue_message("someMeta", "some-simple_queue-queue-message".encode('UTF-8'), policy)
    queue_send_message_to_queue_with_expiration_response = queue.send_queue_message(message)
    print("finished sending message to queue with expiration answer: {} ".format(
        queue_send_message_to_queue_with_expiration_response))


def send_message_to_a_queue_with_delay():
    queue = MessageQueue(queue_name, client_id, kube_add, max_number_messages, max_timeout)
    policy = QueueMessagePolicy()
    policy.DelaySeconds = 5
    message = create_queue_message("someMeta", "some-simple_queue-queue-message".encode('UTF-8'), policy)
    queue_send_message_to_queue_with_delay_response = queue.send_queue_message(message)
    print("finished sending message to queue with delay answer: {} ".format(
        queue_send_message_to_queue_with_delay_response))


def send_message_to_a_queue_with_deadletter_queue():
    queue = MessageQueue(queue_name, client_id, kube_add, max_number_messages, max_timeout)
    policy = QueueMessagePolicy()
    policy.MaxReceiveCount = 3
    policy.MaxReceiveQueue = "DeadLetterQueue"
    message = create_queue_message("someMeta", "some-simple_queue-queue-message".encode('UTF-8'), policy)
    queue_send_message_to_queue_with_deadletter_response = queue.send_queue_message(message)
    print("finished sending message to queue with deadletter answer: {} ".format(
        queue_send_message_to_queue_with_deadletter_response))


def send_batch_message_to_queue():
    queue = MessageQueue(queue_name, client_id, kube_add, max_number_messages, max_timeout)
    mm = []
    for i in range(2):
        message = create_queue_message("queueName {}".format(i), "some-simple_queue-queue-message".encode('UTF-8'))
        mm.append(message)
    queue_send_batch_response = queue.send_queue_messages_batch(mm)
    print("finished sending message to queue with batch answer: {} ".format(queue_send_batch_response))


def receive_message_from_queue():
    queue = MessageQueue(queue_name, client_id, kube_add, max_number_messages, max_timeout)
    queue_receive_response = queue.receive_queue_messages()
    print("finished sending message to receive_queue answer: {} ".format(queue_receive_response))


def peek_message_from_queue():
    queue = MessageQueue(queue_name, client_id, kube_add, max_number_messages, max_timeout)
    queue_receive_response = queue.peek_queue_message(5)
    print("finished sending message to peek answer: {} ".format(queue_receive_response))


def ack_all_messages_in_a_queue():
    queue = MessageQueue(queue_name, client_id, kube_add, max_number_messages, max_timeout)
    queue_ack_response = queue.ack_all_queue_messages()
    print("finished sending message to ack answer: {} ".format(queue_ack_response))


def transactional_queue_ack():
    queue = MessageQueue(queue_name, client_id, kube_add)
    transaction = queue.create_transaction()
    res_rec = transaction.receive(10, 10)

    if res_rec.is_error:
        raise "Message dequeue error, error: %s" % res_rec.is_error

    print("Received message id: %s, body: %s" % (res_rec.message.MessageID, res_rec.message.Body))
    print("tags: %s" % res_rec.message.Tags)

    res_ack = transaction.ack_message(res_rec.message.Attributes.Sequence)
    if res_ack.is_error:
        raise Exception("Ack message error: %s" % res_ack.error)

    print("Received message of type: %s" % StreamRequestType(res_ack.stream_request_type).name)

    time.sleep(1.0)

    # res2 = transaction.receive(10, 1)
    # if res2.is_error:
    #     raise Exception("Message dequeue error, error: %s" % res2.message)

    # print("Received message id: %s, body: %s" % (res2.message.MessageID, res2.message.Body))

    print("bye")


def create_queue_message(meta_data, body, policy=None):
    message = Message()
    message.metadata = meta_data
    message.body = body
    message.tags = [
        ('key', 'value'),
        ('key2', 'value2'),
    ]
    message.attributes = None
    message.policy = policy
    return message


if __name__ == "__main__":
    print("Test Started")
    send_message_to_queue()
    send_message_to_a_queue_with_delay()
    send_message_to_a_queue_with_deadletter_queue()
    send_batch_message_to_queue()
    receive_message_from_queue()
    send_message_to_queue()
    peek_message_from_queue()
    send_message_to_queue()
    ack_all_messages_in_a_queue()
    transactional_queue_ack()
