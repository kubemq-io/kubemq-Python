#  MIT License
# Copyright (c) 2018 KubeMQ
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE. 

import jwt
from kubemq.queue.message_queue import MessageQueue

from kubemq.queue.message import Message


if __name__ == "__main__":

    queue = MessageQueue("hello-world-queue", "test-queue-client-id2", "localhost:50000",encryptionHeader=jwt.encode({},algorithm="HS256",key="some-key"))
    message = Message()
    message.metadata = 'metadata'
    message.body = "some-simple_queue-queue-message".encode('UTF-8')
    message.attributes = None
    try:
        sent  = queue.send_queue_message(message)
        if sent.error:
            print('message enqueue error, error:' + sent.error)
        else:
            print('message sent at: %d' % (
                sent.sent_at
                        ))
    except Exception as err:
        print('message enqueue error, error:%s'  % (
                err
                        ))