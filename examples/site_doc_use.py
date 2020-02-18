import time,datetime
from random import randint
from kubemq.queue.message_queue import MessageQueue
from kubemq.grpc import QueueMessagePolicy
from kubemq.queue.message import Message
from kubemq.queue.stream_request_type import StreamRequestType
from kubemq.events.lowlevel.sender import Sender
from kubemq.subscription.subscribe_type import SubscribeType
from kubemq.subscription.events_store_type import EventsStoreType
from kubemq.events.subscriber import Subscriber
from kubemq.subscription.subscribe_request import SubscribeRequest
from kubemq.tools.listener_cancellation_token import ListenerCancellationToken
from kubemq.events.lowlevel.event import Event
from kubemq.commandquery.responder import Responder
from kubemq.commandquery.response import Response
from kubemq.commandquery.lowlevel.request import Request
from kubemq.commandquery.request_type import RequestType
from kubemq.commandquery.lowlevel.initiator import Initiator


def send_message_to_queue(queue_name,client_id,kube_add):
    queue = MessageQueue(queue_name, client_id, kube_add)
    message = create_queue_message("someMeta", "some-simple_queue-queue-message".encode('UTF-8'))
    queue_send_response = queue.send_queue_message(message)
    print("finished sending to queue answer. message_id: %s, body: %s" % (queue_send_response.message_id, message.body))

def send_message_to_a_queue_with_expiration(queue_name,client_id,kube_add,max_number_messages,max_timeout):
    queue = MessageQueue(queue_name, client_id, kube_add, max_number_messages, max_timeout)
    policy = QueueMessagePolicy()
    policy.ExpirationSeconds = 5
    message = create_queue_message("someMeta", "some-simple_queue-queue-message".encode('UTF-8'), policy)
    queue_send_message_to_queue_with_expiration_response = queue.send_queue_message(message)
    print("finished sending message to queue with expiration answer: {} ".format(
        queue_send_message_to_queue_with_expiration_response))

def send_message_to_a_queue_with_delay(queue_name,client_id,kube_add,max_number_messages,max_timeout):
    queue = MessageQueue(queue_name, client_id, kube_add, max_number_messages, max_timeout)
    policy = QueueMessagePolicy()
    policy.DelaySeconds = 5
    message = create_queue_message("someMeta", "some-simple_queue-queue-message".encode('UTF-8'), policy)
    queue_send_message_to_queue_with_delay_response = queue.send_queue_message(message)
    print("finished sending message to queue with delay answer: {} ".format(
        queue_send_message_to_queue_with_delay_response))

def send_message_to_a_queue_with_deadletter_queue(queue_name,client_id,kube_add,max_number_messages,max_timeout):
    queue = MessageQueue(queue_name, client_id, kube_add, max_number_messages, max_timeout)
    policy = QueueMessagePolicy()
    policy.MaxReceiveCount = 3
    policy.MaxReceiveQueue = "DeadLetterQueue"
    message = create_queue_message("someMeta", "some-simple_queue-queue-message".encode('UTF-8'), policy)
    queue_send_message_to_queue_with_deadletter_response = queue.send_queue_message(message)
    print("finished sending message to queue with deadletter answer: {} ".format(
        queue_send_message_to_queue_with_deadletter_response))


def send_batch_message_to_queue(queue_name,client_id,kube_add,max_number_messages,max_timeout):
    queue = MessageQueue(queue_name, client_id, kube_add, max_number_messages, max_timeout)
    mm = []
    for i in range(2):
        message = create_queue_message("queueName {}".format(i), "some-simple_queue-queue-message".encode('UTF-8'))
        mm.append(message)
    queue_send_batch_response = queue.send_queue_messages_batch(mm)
    print("finished sending message to queue with batch answer: {} ".format(queue_send_batch_response))


def receive_message_from_queue(queue_name,client_id,kube_add,max_number_messages,max_timeout):
    queue = MessageQueue(queue_name, client_id, kube_add, max_number_messages, max_timeout)
    queue_receive_response = queue.receive_queue_messages()
    print("finished sending message to receive_queue answer: {} ".format(queue_receive_response))


def peek_message_from_queue(queue_name,client_id,kube_add,max_number_messages,max_timeout):
    queue = MessageQueue(queue_name, client_id, kube_add, max_number_messages, max_timeout)
    queue_receive_response = queue.peek_queue_message(5)
    print("finished sending message to peek answer: {} ".format(queue_receive_response))


def ack_all_messages_in_a_queue(queue_name,client_id,kube_add,max_number_messages,max_timeout):
    queue = MessageQueue(queue_name, client_id, kube_add, max_number_messages, max_timeout)
    queue_ack_response = queue.ack_all_queue_messages()
    print("finished sending message to ack answer: {} ".format(queue_ack_response))


def transactional_queue_ack(queue_name,client_id,kube_add):
    queue = MessageQueue(queue_name, client_id, kube_add)
    transaction = queue.create_transaction()
    res_rec = transaction.receive(10, 10)

    if res_rec.is_error:
        raise "Message dequeue error, error: %s" % res_rec.is_error

    print("Received message id: {}, body: {} tags:{}".format(res_rec.message.MessageID, res_rec.message.Body,res_rec.message.Tags))

    res_ack = transaction.ack_message(res_rec.message.Attributes.Sequence)
    if res_ack.is_error:
        raise Exception("Ack message error: %s" % res_ack.error)

    print("Received message of type: %s" % StreamRequestType(res_ack.stream_request_type).name)

def transactional_queue_reject(queue_name,client_id,kube_add):
    queue = MessageQueue(queue_name, client_id, kube_add)
    transaction = queue.create_transaction()
    res_rec = transaction.receive(10, 10)

    if res_rec.is_error:
        raise "Message dequeue error, error: %s" % res_rec.is_error

    print("Received message id: {}, body: {} tags:{}".format(res_rec.message.MessageID, res_rec.message.Body,res_rec.message.Tags))

    res_rej = transaction.rejected_message(res_rec.message.Attributes.Sequence)
    if res_rej.is_error:
        raise Exception("Ack message error: %s" % res_rej.error)

    print("rejected message message of type: %s" % StreamRequestType(res_rej.stream_request_type).name)
    

def transactional_queue_extand_visibility(queue_name,client_id,kube_add):
    queue_rej = MessageQueue("reject_test", client_id, kube_add)

    message = create_queue_message("queueName {}".format(0), "my reject".encode('UTF-8'))
    queue_rej.send_queue_message(message)

    queue= MessageQueue("reject_test", client_id, kube_add)
    tran=queue.create_transaction()

    res_rec=tran.receive(5,10)

    if res_rec.is_error:
        raise "Message dequeue error, error: %s" % res_rec.is_error

    print("Received message id: {}, body: {} tags: {}".format(res_rec.message.MessageID, res_rec.message.Body,res_rec.message.Tags))

    print("work for 1 second")

    time.sleep(1)

    print("Need more time to process, extend visibility for more 3 seconds")

    res_ext=tran.extend_visibility(3)

    if res_ext.is_error:
        raise Exception("Ack message error: %s" % res_ext.error)

    print("Approved. work for 2.5 seconds")

    time.sleep(2.5)

    print("Work done... ack the message")


    res_ack=tran.ack_message(res_rec.message.Attributes.Sequence)

    if res_ack.is_error:
        raise Exception("Ack message error: %s" % res_ack.error)

    print("ack done")

def transactional_queue_resend_to_new_queue(queue_name,client_id,kube_add):
    queue_rej = MessageQueue(queue_name, client_id, kube_add)

    message = create_queue_message("resend to new queue {}".format(0), "my resend".encode('UTF-8'))
    queue_rej.send_queue_message(message)

    queue= MessageQueue(queue_name, client_id, kube_add)
    tran=queue.create_transaction()

    res_rec=tran.receive(5,10)

    if res_rec.is_error:
        raise "Message dequeue error, error: %s" % res_rec.is_error

    print("Received message id: {}, body: {} tags:{}".format(res_rec.message.MessageID, res_rec.message.Body, res_rec.message.Tags))

    print("resend to new queue")

    res_resend=tran.resend("new-queue")

    if res_resend.is_error:
        raise "Message resend error, error: %s" % res_resend.is_error

    print("Done")


def transactional_queue_resend_modify_message(queue_name,client_id,kube_add):
    queue_res = MessageQueue(queue_name, client_id, kube_add)

    message = create_queue_message("resend to new queue {}".format(0), "my resend modify".encode('UTF-8'))
    queue_res.send_queue_message(message)

    queue= MessageQueue(queue_name, client_id, kube_add)
    tran=queue.create_transaction()

    res_rec=tran.receive(3,5)

    if res_rec.is_error:
        raise "Message dequeue error, error: %s" % res_rec.is_error

    print("Received message id: {}, body: {} tags:{}".format(res_rec.message.MessageID, res_rec.message.Body,res_rec.message.Tags))

    mod_msg=res_rec.message
    mod_msg.Channel="receiverB"
    
    mod_msg.Metadata="new Metadata"

    res_mod=tran.modify(mod_msg)

    if res_mod.is_error:
        raise "Message modify error, error: %s" % res_mod.is_error

    print("Done")

def event_subscriber(channel_name,p_client_id,kube_add):
    subscriber = Subscriber(kube_add)
    cancel_token=ListenerCancellationToken()
    sub_req= SubscribeRequest(
        channel=channel_name,
        client_id=p_client_id,
        events_store_type=EventsStoreType.Undefined,
        events_store_type_value=0,
        group="",
        subscribe_type=SubscribeType.Events
    )
    subscriber.subscribe_to_events(sub_req, handle_incoming_events,handle_incoming_error,cancel_token)
    print("sub for 2 seconds")
    time.sleep(2.0)
    print("Canceled token")
    cancel_token.cancel()


def handle_incoming_events(event):
    if event:
        print("Subscriber Received Event: Metadata:'%s', Channel:'%s', Body:'%s tags:%s'" % (
            event.metadata,
            event.channel,
            event.body,
            event.tags
        ))

def handle_incoming_error(error_msg):
        print("received error:%s'" % (
            error_msg
        ))

def send_single_event(channel_name,client_id,kube_add):
    sender = Sender(kube_add)
    event = Event(
        metadata="EventMetaData",
        body=("Event Created on time %s" % datetime.datetime.utcnow()).encode('UTF-8'),
        store=False,
        channel=channel_name,
        client_id="EventSender"
    )
    event.tags=[
            ('key', 'value'),
            ('key2', 'value2'),
        ]
    sender.send_event(event)

def send_event_stream(channel_name,client_id,kube_add):
    sender = Sender(kube_add)


    def async_streamer():
        for counter in range(3):
            yield Event(
                metadata="EventMetaData",
                body=("Event %s Created on time %s" % (counter, datetime.datetime.utcnow())).encode('UTF-8'),
                store=False,
                channel=channel_name,
                client_id="EventSenderStream",
            )


    def result_handler(result):
        print(result)


    sender.stream_event(async_streamer(), result_handler)

def send_event_to_store(channel_name,client_id,kube_add):
        sender = Sender(kube_add)
        event = Event(
            metadata="EventMetaData",
            body=("Event Created on time %s" % datetime.datetime.utcnow()).encode('UTF-8'),
            store=True,
            channel=channel_name,
            client_id="EventSenderStore"
        )
        event.tags=[
                ('key', 'value'),
                ('key2', 'value2'),
            ]
        sender.send_event(event)

def stream_to_event_store(channel_name,client_id,kube_add):
    sender = Sender(kube_add)

    def async_streamer():
        for counter in range(3):
            yield Event(
                metadata="EventMetaData",
                body=("Event %s Created on time %s" % (counter, datetime.datetime.utcnow())).encode('UTF-8'),
                store=True,
                channel=channel_name,
                client_id="EventSenderStore",
            )


    def result_handler(result):
        print(result)

    sender.stream_event(async_streamer(), result_handler)

def subcribe_to_event_store(channel_name,p_client_id,kube_add):
    subscriber = Subscriber(kube_add)
    cancel_token=ListenerCancellationToken()
    sub_req= SubscribeRequest(
        channel=channel_name,
        client_id=p_client_id,
        events_store_type=EventsStoreType.StartFromFirst,
        events_store_type_value=0,
        group="",
        subscribe_type=SubscribeType.EventsStore
    )
    subscriber.subscribe_to_events(sub_req, handle_incoming_events,handle_incoming_error,cancel_token)
    print("sub for 2 seconds")
    time.sleep(2.0)
    print("Canceled token")
    cancel_token.cancel()

def subscribe_to_requests(channel_name,p_client_id,kube_add):
    responder = Responder(kube_add)
    cancel_token=ListenerCancellationToken()
    sub_req= SubscribeRequest(
        channel=channel_name,
        client_id=p_client_id,
        events_store_type=EventsStoreType.Undefined,
        events_store_type_value=0,
        group="",
        subscribe_type=SubscribeType.Queries
    )
    responder.subscribe_to_requests(sub_req, handle_incoming_request,handle_incoming_error,cancel_token)
    print("sub for 10 seconds")
    time.sleep(10.0)
    print("Canceled token")
    cancel_token.cancel()

def handle_incoming_request(request):
    if request:
        print("Subscriber Received request: Metadata:'%s', Channel:'%s', Body:'%s' tags:%s" % (
            request.metadata,
            request.channel,
            request.body,
            request.tags
        ))
        
        response = Response(request)
        response.body = "OK".encode('UTF-8')
        response.cache_hit = False
        response.error = "None"
        response.client_id = "My_Client_id"
        response.executed = True
        response.metadata = "OK"
        response.timestamp = datetime.datetime.now()
        response.tags=request.tags
        return response

def handle_request_incoming_error(error_msg):
        print("received error:%s'" % (
            error_msg
        ))

def send_command_request(channel_name,client_id,kube_add):
    request = Request(
        body="Request".encode('UTF-8'),
        metadata="MyMetadata",
        cache_key="",
        cache_ttl=0,
        channel=channel_name,
        client_id="CommandQueryInitiator",
        timeout=1000,
        request_type=RequestType.Command,
        tags=[
            ('key', 'value'),
            ('key2', 'value2'),
        ]
    )
    initiator = Initiator(kube_add)
    response = initiator.send_request(request)

def send_query_request(channel_name,client_id,kube_add):
    request = Request(
        body="Request".encode('UTF-8'),
        metadata="MyMetadata",
        cache_key="",
        cache_ttl=0,
        channel=channel_name,
        client_id="QueryInitiator",
        timeout=1000,
        request_type=RequestType.Query,
        tags=[
            ('key', 'value'),
            ('key2', 'value2'),
        ]
    )
    initiator = Initiator(kube_add)
    response = initiator.send_request(request)

def create_check_connection():
    sender= Sender()
    try:
        result=sender.ping()
    except Exception as identifier:
        print('error {}'.format(identifier))
        exit()
    print(result)

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
    print("test")


