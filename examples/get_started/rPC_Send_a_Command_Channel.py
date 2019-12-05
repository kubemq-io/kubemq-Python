
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


from kubemq.commandquery.lowlevel.initiator import Initiator
from kubemq.commandquery.lowlevel.request import Request
from kubemq.commandquery.request_type import RequestType




if __name__ == "__main__":

    initiator = Initiator("localhost:50000")
    request  = Request(
        body="hello kubemq - sending a command, please reply'".encode('UTF-8'),
        metadata="",
        cache_key="",
        cache_ttl=0,
        channel="testing_Command_channel",
        client_id="hello-world-sender",
        timeout=10000,
        request_type=RequestType.Command,
    )
    try:
        response = initiator.send_request(request)
        print('Response Received:%s Executed at::%s'  % (
            response.request_id,
            response.timestamp
                    ))
    except Exception as err :
        print('command error::%s'  % (
            err
                    ))

