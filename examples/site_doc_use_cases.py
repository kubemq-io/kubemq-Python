from kubemq.queue.queue import Queue
from kubemq.grpc import QueueMessagePolicy
from kubemq.queue.message import Message
def send_message_to_queue():
    queue= Queue("QueueName","ClientID","localhost:50000",32,1)
    message=create_queue_message("someMeta",("some-simple_queue-queue-message").encode('UTF-8'))
    queue_send_response=queue.send_queue_message(message)
    print("finished sending message to queue answer: {} ".format(queue_send_response))

def send_message_to_a_queue_with_delay():
    queue= Queue("QueueName","ClientID","localhost:50000",32,1)
    policy=QueueMessagePolicy()
    policy.ExpirationSeconds=5
    message=create_queue_message("someMeta",("some-simple_queue-queue-message").encode('UTF-8'),policy)
    queue_send_message_to_queue_with_delay_response=queue.send_queue_message(message)
    print("finished sending message to queue with delay answer: {} ".format(queue_send_message_to_queue_with_delay_response))

def send_message_to_a_queue_with_deadletter_queue():
    queue= Queue("QueueName","ClientID","localhost:50000",32,1)
    policy=QueueMessagePolicy()
    policy.MaxReceiveCount=3
    policy.MaxReceiveQueue="DeadLetterQueue"
    message=create_queue_message("someMeta",("some-simple_queue-queue-message").encode('UTF-8'),policy)
    queue_send_message_to_queue_with_deadletter_response=queue.send_queue_message(message)
    print("finished sending message to queue with deadletter answer: {} ".format(queue_send_message_to_queue_with_deadletter_response))

def send_batch_message_to_queue():
    queue= Queue("QueueName","ClientID","localhost:50000",32,1)
    mm=[]
    for i in range(2):
        message=create_queue_message("queueName {}".format(i) ,("some-simple_queue-queue-message").encode('UTF-8'))
        mm.append(message)
    queue_send_batch_response=queue.send_queue_messages_batch(mm)
    print("finished sending message to queue with batch answer: {} ".format(queue_send_batch_response))
    
def receive_message_from_queue():
    queue= Queue("QueueName","ClientID","localhost:50000",32,1)
    queue_receive_response=queue.receive_queue_messages()
    print("finished sending message to receive_queue answer: {} ".format(queue_receive_response))

def peek_message_from_queue():
    queue= Queue("QueueName","ClientID","localhost:50000",32,1)
    queue_receive_response=queue.peek_queue_message(5)
    print("finished sending message to peek answer: {} ".format(queue_receive_response))

def ack_all_messages_in_a_queue():
    queue= Queue("QueueName","ClientID","localhost:50000",32,1)
    queue_ack_response=queue.ack_all_queue_messages()
    print("finished sending message to ack answer: {} ".format(queue_ack_response))


def create_queue_message(meta_data,body,policy=None):
    message=Message()
    message.metadata=meta_data
    message.body=body
    message.tags=None
    message.attributes=None
    message.policy=policy
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