from kubemq.queue.queue import Queue
from kubemq.grpc import QueueMessagePolicy
from kubemq.queue.message import Message
import time
queue_name="MyQueueName3232"
client_id="ClientID32"
kube_add="localhost:50000"
max_number_messages=32
max_timeout=1
def send_message_to_queue():
    queue= Queue(queue_name,client_id,kube_add)
    message=create_queue_message("someMeta",("some-simple_queue-queue-message").encode('UTF-8'))
    queue_send_response=queue.send_queue_message(message)
    print("finished sending message to queue answer: {} ".format(queue_send_response))

def send_message_to_a_queue_with_expiration():
    queue= Queue(queue_name,client_id,kube_add,max_number_messages,max_timeout)
    policy=QueueMessagePolicy()
    policy.ExpirationSeconds=5
    message=create_queue_message("someMeta",("some-simple_queue-queue-message").encode('UTF-8'),policy)
    queue_send_message_to_queue_with_expiration_response=queue.send_queue_message(message)
    print("finished sending message to queue with expiration answer: {} ".format(queue_send_message_to_queue_with_expiration_response))

def send_message_to_a_queue_with_delay():
    queue= Queue(queue_name,client_id,kube_add,max_number_messages,max_timeout)
    policy=QueueMessagePolicy()
    policy.DelaySeconds=5
    message=create_queue_message("someMeta",("some-simple_queue-queue-message").encode('UTF-8'),policy)
    queue_send_message_to_queue_with_delay_response=queue.send_queue_message(message)
    print("finished sending message to queue with delay answer: {} ".format(queue_send_message_to_queue_with_delay_response))

def send_message_to_a_queue_with_deadletter_queue():
    queue= Queue(queue_name,client_id,kube_add,max_number_messages,max_timeout)
    policy=QueueMessagePolicy()
    policy.MaxReceiveCount=3
    policy.MaxReceiveQueue="DeadLetterQueue"
    message=create_queue_message("someMeta",("some-simple_queue-queue-message").encode('UTF-8'),policy)
    queue_send_message_to_queue_with_deadletter_response=queue.send_queue_message(message)
    print("finished sending message to queue with deadletter answer: {} ".format(queue_send_message_to_queue_with_deadletter_response))

def send_batch_message_to_queue():
    queue= Queue(queue_name,client_id,kube_add,max_number_messages,max_timeout)
    mm=[]
    for i in range(2):
        message=create_queue_message("queueName {}".format(i) ,("some-simple_queue-queue-message").encode('UTF-8'))
        mm.append(message)
    queue_send_batch_response=queue.send_queue_messages_batch(mm)
    print("finished sending message to queue with batch answer: {} ".format(queue_send_batch_response))
    
def receive_message_from_queue():
    queue= Queue(queue_name,client_id,kube_add,max_number_messages,max_timeout)
    queue_receive_response=queue.receive_queue_messages()
    print("finished sending message to receive_queue answer: {} ".format(queue_receive_response))

def peek_message_from_queue():
    queue= Queue(queue_name,client_id,kube_add,max_number_messages,max_timeout)
    queue_receive_response=queue.peek_queue_message(5)
    print("finished sending message to peek answer: {} ".format(queue_receive_response))

def ack_all_messages_in_a_queue():
    queue= Queue(queue_name,client_id,kube_add,max_number_messages,max_timeout)
    queue_ack_response=queue.ack_all_queue_messages()
    print("finished sending message to ack answer: {} ".format(queue_ack_response))

def Transactional_Queue_Ack():
    queue= Queue(queue_name,client_id,kube_add,max_number_messages,max_timeout)
    transaction= queue.create_transaction()
    resRec=transaction.receive(10,10)
    if resRec.is_error:
        print("Message dequeue error, error:{}".format(resRec.is_error))
    else:
        print("recieved resRec{}".format(resRec))
    resAck=transaction.ack_message(resRec.message.Attributes.Sequence)
    if resAck.is_error:
        print("Ack message error:{}".format(resAck.is_error))
    else:
        print(resAck)
        res2=transaction.receive(10,1)
        if res2.is_error:
          print("Message dequeue error, error:{}".format(res2.is_error))
          return


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
    # send_message_to_a_queue_with_delay()
    # send_message_to_a_queue_with_deadletter_queue()
    # send_batch_message_to_queue()
    # receive_message_from_queue()
    # send_message_to_queue()
    # peek_message_from_queue()
    # send_message_to_queue()
    # ack_all_messages_in_a_queue()
    Transactional_Queue_Ack()