


# MIT License
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

from kubemq.queue.message_queue import MessageQueue




if __name__ == "__main__":
    queue = MessageQueue("hello-world-queue", "test-queue-client-id2", "localhost:50000", 2, 1)
    try:
        res = queue.receive_queue_messages()
        if res.error:
            print(
                "'Received:'%s'" % (
                    res.error
                            )
            )
        else:
            for message in res.messages:
                print(
                        "'MessageID :%s ,Body: sending:'%s'" % (
                            message.MessageID,
                            message.Body
                                    )
                    )
    except Exception as err:
      print(
            "'error sending:'%s'" % (
                err
                        )
        )
    input("Press 'Enter' to stop the application...\n")

    
