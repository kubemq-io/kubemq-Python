# KubeMQ SDK for Python

The **KubeMQ SDK for Python** enables Python developers to easily work with [KubeMQ](https://kubemq.io/). 

## Getting Started

### Prerequisites

KubeMQ-SDK-Python works with **Python 3.2** or newer.

### Installing
 
The recommended way to use the SDK for Python in your project is to consume it from pip.

```
pip install kubemq
```

This package uses setuptools for the installation if needed please run:
```
python3 -m pip install --upgrade pip setuptools wheel
```

## Generating Documentation

Sphinx is used for documentation. Use the Makefile to build the docs, like so:

```
$ pip install -r requirements-docs.txt
$ cd docs
$ make html
```
(`make latex` or `make linkcheck` supported)

## Building from source

Once you check out the code from GitHub, you can install the package locally with:

```
$ pip install .
```

You can also install the package with a symlink, 
so that changes to the source files will be immediately available:

```
$ pip install -e .
```

Installation:
$ pip install kubemq

### Core Basics
KubeMQ messaging broker has five messaging patterns:

- **Queues**  - FIFO based, exactly one durable queue pattern
- **Events** - real-time pub/sub pattern
- **Events Store** - pub/sub with persistence pattern
- **Commands** - the Command part of CQRS pattern, which sends commands with the response for executed or not (with proper error messaging)
- **Queries** - the Query part of CQRS pattern, which sends a query and gets a response with the relevant query result back
For each one of the patterns, we can distinguish between the senders and the receivers.
- **Group**: Optional parameter when subscribing to a channel. A set of subscribers can define the same group so that only one of the subscribers within the group will receive a specific event. Used mainly for load balancing. Subscribing without the group parameter ensures receiving all the channel messages. (When using Grouping all the programs that are assigned to the group need to have to same channel name)

For events and events store, the KubeMQ supports both RPC and upstream calls.

the data model is almost identical between all the pattern with some data added related to the specific patter.

### The common part of all the patterns are:

- **ID** - the sender can set the ID for each type of message, or the Id is automatically generated a UUID Id for him.
- **Metadata** - a string field that can hold any metadata related to the message
- **Body** - a Bytes array which contains the actual payload to be sent from the sender to the receiver
- **Tags** - a Map of string, string for user define data
- The KubeMQ core transport is based on gRPC, and the library is a wrapper around the client-side of gRPC complied protobuf hence -   leveraging the gRPC benefits and advantages.

Before any transactions to be performed with KubeMQ server, the Client should connect and dial KubeMQ server and obtain Client connection.

With the Client connection object, the user can perform all transactions to and from KubeMQ server.

A Client connection object is thread-safe and can be shared between all process needed to communicate with KubeMQ.

IMPORTANT - it's the user responsibility to close the Client connection when no further communication with KubeMQ is needed.

Connection
Connecting to KubeMQ server can be by creating the type needed:
```
    pub/sub:
        sender:
        sender = Sender("localhost:50000")
        Subscriber:
        subscriber = Subscriber("localhost:50000)
        
    command/query:
        Initiator:
        initiator = Initiator("localhost:50000")
        Responder:
        responder = Responder("localhost:50000")
        
    then to check connection call ping as such:
        #
        def create_check_connection():
        sender= Sender("localhost:50000")
        try:
            result=sender.ping()
        except Exception as identifier:
            print('error {}'.format(identifier))
            exit()
        print(result)
    
```
Examples
Please visit our extensive examples folder Please find usage examples on the examples folders.

### Queues
Core features
- KubeMQ supports distributed durable FIFO based queues with the following core features:

- **Exactly One Delivery** - Only one message guarantee will deliver to the subscriber

- **Single and Batch Messages Send and Receive** - Single and multiple messages in one call

- **RPC and Stream Flows** - RPC flow allows an insert and pulls messages in one call. Stream flow allows single message consuming in a transactional way

- **Message Policy** - Each message can be configured with expiration and delay timers. Also, each message can specify a dead-letter queue for un-processed messages attempts

- **Long Polling** - Consumers can wait until a message available in the queue to consume

- **Peek Messages**- Consumers can peek into a queue without removing them from the queue

- **Ack All Queue Messages** - Any client can mark all the messages in a queue as discarded and will not be available anymore to consume

- **Visibility timers** - Consumers can pull a message from the queue and set a timer which will cause the message not be visible to other consumers. This timer can be extended as needed.

- **Resend Messages** - Consumers can send back a message they pulled to a new queue or send a modified message to the same queue for further processing.

### QueueMessageAttributes.(proto struct)
Timestamp - when the message arrived to queue.

Sequence - the message order in the queue.

MD5OfBody - An MD5 digest non-URL-encoded message body string.

ReceiveCount - how many recieved.

ReRouted - if the message was ReRouted from another point.

ReRoutedFromQueue - from where the message was ReRouted

ExpirationAt - Expiration time of the message.

DelayedTo -if the message was Delayed.

```
  message QueueMessageAttributes {
      int64               Timestamp                   =1;
      uint64              Sequence                    =2;
      string              MD5OfBody                   =3;
      int32               ReceiveCount                =4;
      bool                ReRouted                    =5;
      string              ReRoutedFromQueue           =6;
      int64               ExpirationAt                =7;
      int64               DelayedTo                   =8;

  }
```

Send Message to a Queue:
```
    queue = MessageQueue(queue_name, client_id, kube_add)
    message = create_queue_message("someMeta", "some-simple_queue-queue-message".encode('UTF-8'))
    queue_send_response = queue.send_queue_message(message)
    print("finished sending to queue answer. message_id: %s, body: %s" % (queue_send_response.message_id, message.body))
```
create_queue_message:
```
    def create_queue_message(meta_data, body, policy=None):
        message = Message()
        message.metadata = meta_data
        message.body = body
        message.tags = [
            ('key', 'value'),
            ('key2', 'value2')
        ]
        message.attributes = None
        message.policy = policy
        return message
```
Send Message to a Queue with Expiration:
```
    queue = MessageQueue(queue_name, client_id, kube_add, max_number_messages, max_timeout)
    policy = QueueMessagePolicy()
    policy.ExpirationSeconds = 5
    message = create_queue_message("someMeta", "some-simple_queue-queue-message".encode('UTF-8'), policy)
    queue_send_message_to_queue_with_expiration_response = queue.send_queue_message(message)
    print("finished sending message to queue with expiration answer: {} ".format(
        queue_send_message_to_queue_with_expiration_response))
```

Send Message to a Queue with Delay:
```
    queue = MessageQueue(queue_name, client_id, kube_add, max_number_messages, max_timeout)
    policy = QueueMessagePolicy()
    policy.DelaySeconds = 5
    message = create_queue_message("someMeta", "some-simple_queue-queue-message".encode('UTF-8'), policy)
    queue_send_message_to_queue_with_delay_response = queue.send_queue_message(message)
    print("finished sending message to queue with delay answer: {} ".format(
        queue_send_message_to_queue_with_delay_response))
```

Send Message to a Queue with Dead-letter Queue:
```
    queue = MessageQueue(queue_name, client_id, kube_add, max_number_messages, max_timeout)
    policy = QueueMessagePolicy()
    policy.MaxReceiveCount = 3
    policy.MaxReceiveQueue = "DeadLetterQueue"
    message = create_queue_message("someMeta", "some-simple_queue-queue-message".encode('UTF-8'), policy)
    queue_send_message_to_queue_with_deadletter_response = queue.send_queue_message(message)
    print("finished sending message to queue with deadletter answer: {} ".format(
        queue_send_message_to_queue_with_deadletter_response))
```

Send Batch Messages:
```
    queue = MessageQueue(queue_name, client_id, kube_add, max_number_messages, max_timeout)
    mm = []
    for i in range(2):
        message = create_queue_message("queueName {}".format(i), "some-simple_queue-queue-message".encode('UTF-8'))
        mm.append(message)
    queue_send_batch_response = queue.send_queue_messages_batch(mm)
    print("finished sending message to queue with batch answer: {} ".format(queue_send_batch_response))
```

Receive Messages from a Queue:
```
    queue = MessageQueue(queue_name, client_id, kube_add, max_number_messages, max_timeout)
    queue_receive_response = queue.receive_queue_messages()
    print("finished sending message to receive_queue answer: {} ".format(queue_receive_response))
```

Peak Messages from a Queue:
```
    queue = MessageQueue(queue_name, client_id, kube_add, max_number_messages, max_timeout)
    queue_receive_response = queue.peek_queue_message(5)
    print("finished sending message to peek answer: {} ".format(queue_receive_response))
```

Ack All Messages In a Queue:
```
    queue = MessageQueue(queue_name, client_id, kube_add, max_number_messages, max_timeout)
    queue_ack_response = queue.ack_all_queue_messages()
    print("finished sending message to ack answer: {} ".format(queue_ack_response))
```
Transactional Queue - Ack:
```
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
```
Transactional Queue - Reject:
```
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
```

Transactional Queue - Extend Visibility:
```
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
```

Transactional Queue - Resend to New Queue:
```
    queue_rej = MessageQueue("resend_to_new_queue", client_id, kube_add)

    message = create_queue_message("resend to new queue {}".format(0), "my resend".encode('UTF-8'))
    queue_rej.send_queue_message(message)

    queue= MessageQueue("resend_to_new_queue", client_id, kube_add)
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
```
Transactional Queue - Resend Modified Message:
```
    queue_res = MessageQueue("resend_modify_message", client_id, kube_add)

    message = create_queue_message("resend to new queue {}".format(0), "my resend modify".encode('UTF-8'))
    queue_res.send_queue_message(message)

    queue= MessageQueue("resend_modify_message", client_id, kube_add)
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
```
Events
Sending Events
Single Event:
```
    def send_single_event():
        sender = Sender(kube_add)
        event = Event(
            metadata="EventMetaData",
            body=("Event Created on time %s" % datetime.datetime.utcnow()).encode('UTF-8'),
            store=False,
            channel="MyTestChannelName",
            client_id="EventSender"
        )
        event.tags=[
                ('key', 'value'),
                ('key2', 'value2'),
            ]
        sender.send_event(event)
```
Stream Events:
```
    sender = Sender(kube_add)


    def async_streamer():
        for counter in range(3):
            yield Event(
                metadata="EventMetaData",
                body=("Event %s Created on time %s" % (counter, datetime.datetime.utcnow())).encode('UTF-8'),
                store=False,
                channel="MyTestChannelName",
                client_id="EventSenderStream",
            )


    def result_handler(result):
        print(result)


    sender.stream_event(async_streamer(), result_handler)
```

Receiving Events
First you should subscribe to Events:
```
        def event_subscriber():
            subscriber = Subscriber(kube_add)
            cancel_token=ListenerCancellationToken()
            sub_req= SubscribeRequest(
                channel="MyTestChannelName",
                client_id=str(randint(9, 19999)),
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
```
Events Store
Sending Events Store
Single Event to Store:
```
        sender = Sender(kube_add)
        event = Event(
            metadata="EventMetaData",
            body=("Event Created on time %s" % datetime.datetime.utcnow()).encode('UTF-8'),
            store=True,
            channel="MyTestChannelNameStore",
            client_id="EventSenderStore"
        )
        event.tags=[
                ('key', 'value'),
                ('key2', 'value2'),
            ]
        sender.send_event(event)
```
Stream Events Store:
```
        sender = Sender(kube_add)


        def async_streamer():
            for counter in range(3):
                yield Event(
                    metadata="EventMetaData",
                    body=("Event %s Created on time %s" % (counter, datetime.datetime.utcnow())).encode('UTF-8'),
                    store=True,
                    channel="MyTestChannelNameStore",
                    client_id="EventSenderStore",
                )


        def result_handler(result):
            print(result)


        sender.stream_event(async_streamer(), result_handler)

```
Receiving Events Store
First you should subscribe to Events Store and get a channel:
```
    subscriber = Subscriber(kube_add)
    cancel_token=ListenerCancellationToken()
    sub_req= SubscribeRequest(
        channel="MyTestChannelNameStore",
        client_id=str(randint(9, 19999)),
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
```
### Subscription Options
KubeMQ supports six types of subscriptions:

- **StartFromNewEvents** - start event store subscription with only new events

- **StartFromFirstEvent** - replay all the stored events from the first available sequence and continue stream new events from this point

- **StartFromLastEvent** - replay the last event and continue stream new events from this point

- **StartFromSequence** - replay events from specific event sequence number and continue stream new events from this point

- **StartFromTime** - replay events from specific time continue stream new events from this point

- **StartFromTimeDelta** - replay events from specific current time - delta duration in seconds, continue stream new events from this point

## Commands
Concept
Commands implement synchronous messaging pattern which the sender send a request and wait for a specific amount of time to get a response.

The response can be successful or not. This is the responsibility of the responder to return with the result of the command within the time the sender set in the request.

Sending Command Requests:
In this example, the responder should send his response withing one second. Otherwise, an error will be return as a timeout:
```
    request = Request(
        body="Request".encode('UTF-8'),
        metadata="MyMetadata",
        cache_key="",
        cache_ttl=0,
        channel="MyTestChannelName",
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

```
### Queries
Concept
Queries implement synchronous messaging pattern which the sender send a request and wait for a specific amount of time to get a response.

The response must include metadata or body together with an indication of successful or not operation. This is the responsibility of the responder to return with the result of the query within the time the sender set in the request.

Sending Query Requests:
```
    request = Request(
        body="Request".encode('UTF-8'),
        metadata="MyMetadata",
        cache_key="",
        cache_ttl=0,
        channel="MyTestChannelName",
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
```
Receiving Query Requests and sending response
First get a channel of queries:
```
    responder = Responder(kube_add)
    cancel_token=ListenerCancellationToken()
    sub_req= SubscribeRequest(
        channel="MyTestRequestChannelName",
        client_id=str(randint(9, 19999)),
        events_store_type=EventsStoreType.Undefined,
        events_store_type_value=0,
        group="",
        subscribe_type=SubscribeType.Queries
    )
    responder.subscribe_to_requests(sub_req, handle_incoming_events,handle_incoming_error,cancel_token)
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
        response.client_id = client_id
        response.executed = True
        response.metadata = "OK"
        response.timestamp = datetime.datetime.now()
        response.tags=request.tags
        #Return response
        return response

def handle_request_incoming_error(error_msg):
        print("received error:%s'" % (
            error_msg
        ))
```

License
This project is licensed under the MIT License - see the LICENSE file for details
