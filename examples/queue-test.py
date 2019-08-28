from kubemq.queue.queue import Queue
from kubemq.queue.message import Message
if __name__ == "__main__":
    print("Test Started")
    queue= Queue("Eitam3","eitam_client","localhost:50000",32,1)
    M= Message()
    M.client_id= "EitamcIDs"
    M.metadata="metaEitamdata"
    M.body=("Event Created on time ").encode('UTF-8')
    M.tags=None
    M.attributes=None
    M.policy=None

    r= Message()
    r.client_id= "EitamcID2"
    r.metadata="metaEitamdat2"
    r.body=("Event Created on time ").encode('UTF-8')
    r.tags=None
    r.attributes=None
    r.policy=None
    mm=[]
    mm.append(r)
    mm.append(M)
    ping= queue.ping()
    queue_send_response= queue.send_queue_message(r)
    queue_send_response= queue.send_queue_messages_batch(mm)
    rec_send_response=queue.receive_queue_messages()
    peak_send_response= queue.peek_queue_message(1)
    queue_send_response= queue.send_queue_message(r)
    ack_send_response=queue.ack_all_queue_messages()
    print(queue)
    print("Test ended")
    