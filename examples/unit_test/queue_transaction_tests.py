import unittest, random, string
from kubemq.queue.message_queue import MessageQueue
from kubemq.queue.message import Message
from kubemq.grpc import QueueMessagePolicy
import time
import datetime


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



    def test_get_message_pass(self):
        client_id = "message_pass"
        kube_add = "localhost:50000"
        queue=MessageQueue("Get_Messages{}".format(randomString(10)), client_id, kube_add)
        mm = []

        

        message = create_queue_message("first test Ack", "hi there".encode('UTF-8'))
        mm.append(message)

        message2 = create_queue_message("sec test Ack", "hi again".encode('UTF-8'))
        mm.append(message2)
        queue.send_queue_messages_batch(mm)
        

        tr=queue.create_transaction()
        recm=tr.receive(3)
        self.assertFalse(recm.error)
        self.assertFalse(tr.ack_message(recm.message.Attributes.Sequence).is_error)
        self.assertEqual(1,len(queue.peek_queue_message().messages))
        self.assertFalse(tr.receive().is_error)
        tr.close_stream()

    def test_send_receive_tran_ack_pass(self):
        client_id = "tran_ack_pass"
        kube_add = "localhost:50000"
        queue=MessageQueue("SendReciveTranAck_Pass", client_id, kube_add)


        message = create_queue_message("first test Ack", "hi there".encode('UTF-8'))

        queue.send_queue_message(message)
        
        tr=queue.create_transaction()
        recm=tr.receive(5)
        self.assertFalse(tr.ack_message(recm.message.Attributes.Sequence).is_error)


    def test_send_receive_tran_ack_fail(self):
        client_id = "tran_ack_fail"
        kube_add = "localhost:50000"
        queue=MessageQueue("SendReciveTranAck_Fail", client_id, kube_add)


        message = create_queue_message("first test Ack", "hi there".encode('UTF-8'))

        queue.send_queue_message(message)
        
        tr=queue.create_transaction()
        recm=tr.receive(5)
        ackms=tr.ack_message(recm.message.Attributes.Sequence)
        time.sleep(0.1)
        recm2=tr.receive(5)
        self.assertFalse(ackms.is_error)
        self.assertTrue(recm2.is_error)
        tr.close_stream()

    def test_send_receive_tran_visability_expired_fail(self):
        client_id = "expired_fail"
        kube_add = "localhost:50000"
        queue=MessageQueue("send_receive_tran_visability_expired_fail", client_id, kube_add)


        message = create_queue_message("first test Ack", "hi there".encode('UTF-8'))

        queue.send_queue_message(message)
        
        tr=queue.create_transaction()
        recm=tr.receive(4)
        time.sleep(5)
        ackms = tr.ack_message(recm.message.Attributes.Sequence)
        self.assertEqual(ackms.error,"Error 129: current visibility timer expired")

    def test_send_receive_tran_visability_expired_pass(self):
        client_id = "expired_pass"
        kube_add = "localhost:50000"
        queue=MessageQueue("send_receive_tran_visability_expired_pass", client_id, kube_add)


        message = create_queue_message("first test Ack", "hi there".encode('UTF-8'))

        queue.send_queue_message(message)
        
        tr=queue.create_transaction()
        recm=tr.receive()
        tr.extend_visibility(5)
        time.sleep(4)
        ackms = tr.ack_message(recm.message.Attributes.Sequence)
        self.assertEqual(ackms.is_error,False)

    def test_modify_new_message_pass(self):
        client_id = "message_pass"
        kube_add = "localhost:50000"
        queue=MessageQueue("send_modify_new_message_pass", client_id, kube_add)


        message = create_queue_message("first test Ack", "hi there".encode('UTF-8'))

        queue.send_queue_message(message)
        
        tr=queue.create_transaction()
        recm=tr.receive(10)
        self.assertFalse(recm.is_error)
        modMsg = create_queue_message("hi there", "well hello".encode('UTF-8'))
        modMsg.queue=queue.queue_name

        resMod=tr.modify(modMsg)
        self.assertFalse(resMod.is_error)
        tr.close_stream()
        time.sleep(0.01)


        recm2 = tr.receive(3,5)
        self.assertEqual("well hello",recm2.message.Body.decode("utf-8") )
        tr.close_stream()

    def test_modify_after_ack_fail(self):
        client_id = "ack_fail"
        kube_add = "localhost:50000"
        queue=MessageQueue("test_modify_after_ack_fail", client_id, kube_add)
        mm = []

        

        message = create_queue_message("first test Ack", "hi there".encode('UTF-8'))
        mm.append(message)

        message2 = create_queue_message("sec test Ack", "hi again".encode('UTF-8'))
        mm.append(message2)
        queue.send_queue_messages_batch(mm)

        
        tr=queue.create_transaction()
        recm=tr.receive(5)
        
        tr.ack_message(recm.message.Attributes.Sequence)
        time.sleep(0.1)
        recMod=tr.extend_visibility(5)
        self.assertTrue(recMod.is_error)

    




def randomString(stringLength=10):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))

if __name__ == '__main__':
    unittest.main()