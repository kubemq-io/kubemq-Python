# MIT License
#
# Copyright (c) 2018 KubeMQ
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
import logging
from kubemq.grpc import Empty
from kubemq.basic.grpc_client import GrpcClient
from kubemq.events.lowlevel.event import Event
from kubemq.events.result import Result


class Sender(GrpcClient):
    """Represents the instance that is responsible to send events to the kubemq."""

    def __init__(self, kubemq_address=None, encryptionHeader=None):
        """
        Initialize a new Sender under the requested KubeMQ Server Address.

        :param str kubemq_address: KubeMQ server address. if None will be parsed from Config or environment parameter.
        :param byte[] encryptionHeader: the encrypted header requested by kubemq authentication.
        """
        GrpcClient.__init__(self, encryptionHeader)
        if kubemq_address:
            self._kubemq_address = kubemq_address

    def send_event(self, event):
        # type: (Event) -> Result
        """Publish a single event using the KubeMQ."""
        try:
            result = self.get_kubemq_client().SendEvent(event.to_inner_event(), metadata=self._metadata)
            if result:
                return Result(inner_result=result)
        except Exception as e:
            logging.exception(
                "Sender received 'Result': Error:'%s'" % (
                    e
                ))
            raise

    def stream_event(self, events_stream, response_handler=None):
        def callback():
            for event in events_stream:
                yield event.to_inner_event()

        try:
            responses = self.get_kubemq_client().SendEventsStream(callback(), metadata=self._metadata)
            for response in responses:
                if response_handler:
                    result = Result(response)
                    logging.debug(
                        "Sender received 'Result': EventID:'%s', Sent:'%s', Error:'%s'" % (
                            result.event_id,
                            result.sent,
                            result.error
                        ))
                    response_handler(result)
        except Exception as e:
            logging.exception(
                "Sender received 'Result': Error:'%s'" % (
                    e
                ))
            raise

    def ping(self):
        """ping check connection to the kubemq"""
        ping_result = self.get_kubemq_client().Ping(Empty())
        logging.debug("event sender KubeMQ address:%s ping result:%s'" % (self._kubemq_address, ping_result))
        return ping_result
