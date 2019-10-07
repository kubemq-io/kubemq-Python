import unittest
from kubemq.queue.message_queue import MessageQueue
from kubemq.queue.message import Message
from kubemq.grpc import QueueMessagePolicy
import time


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

class TestStringMethods(unittest.TestCase):

    def test_delayed_message_pass(self):
        queue_name = "message_pass"
        client_id = "message_pass"
        kube_add = "localhost:50000"
        queue=MessageQueue(queue_name, client_id, kube_add)
        mm = []

        
        queuePolicy=QueueMessagePolicy(
            DelaySeconds =3
        )
        message = create_queue_message("queueName {}".format(0), "some-simple_queue-queue-message".encode('UTF-8'),queuePolicy)
        mm.append(message)

        queuePolicy2=QueueMessagePolicy(
            DelaySeconds =5
        )
        message2 = create_queue_message("queueName {}".format(1), "some-simple_queue-queue-message".encode('UTF-8'),queuePolicy2)
        mm.append(message2)
        queue.send_queue_messages_batch(mm)
        

        res=queue.receive_queue_messages()
        self.assertFalse(res.is_error)
        self.assertEqual(0,len(res.messages))
        time.sleep(3.0)
        res=queue.receive_queue_messages()
        self.assertFalse(res.is_error)
        self.assertEqual(1,len(res.messages))
        time.sleep(2.0)
        res=queue.receive_queue_messages()
        self.assertFalse(res.is_error)
        self.assertEqual(1,len(res.messages))


    def test_delayed_transaction_message_pass(self):
        queue_name = "transaction_message_pass"
        client_id = "transaction_message_pass"
        kube_add = "localhost:50000"
        queue=MessageQueue(queue_name, client_id, kube_add)
        mm = []

        
        queuePolicy=QueueMessagePolicy(
            DelaySeconds =3
        )
        message = create_queue_message("queueName {}".format(0), "first test Ack".encode('UTF-8'),queuePolicy)
        mm.append(message)

        queuePolicy2=QueueMessagePolicy(
            DelaySeconds =5
        )
        message2 = create_queue_message("queueName {}".format(1), "first test Ack".encode('UTF-8'),queuePolicy2)
        mm.append(message2)
        queue.send_queue_messages_batch(mm)

        tr=queue.create_transaction()
        recm=tr.receive()
        self.assertEqual(recm.error,"Error 138: no new message in queue, wait time expired")
        tr.close_stream()
        time.sleep(0.1)

        recm=tr.receive(2,3)
        self.assertFalse(recm.is_error)
        tr.close_stream()
        time.sleep(0.01)

        recm=tr.receive(2,5)
        self.assertFalse(recm.is_error)


if __name__ == '__main__':
    unittest.main()