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
from kubemq.events.lowlevel.event import Event
from kubemq.events.lowlevel.sender import Sender


class Channel:
    """Sender with a set of predefined parameters"""
    sender = None

    def __init__(self, params=None, channel_name=None, client_id=None, store=None, kubemq_address=None,
                 encryptionHeader=None):
        """
        Initializes a new instance of the Events.Channel class using params OR "Manual" Parameters.

        :param params: ChannelParameters to use instead of the other params
        :param channel_name: Represents The channel name to send to using the KubeMQ.
        :param client_id: Represents the sender ID that the messages will be send under.
        :param store: Represents if the events should be send to persistence.
        :param kubemq_address: The address the of the KubeMQ including the GRPC Port ,Example: "LocalHost:50000".
        :param byte[] encryptionHeader: the encrypted header requested by kubemq authentication.
        """

        if params:
            self.channel_name = params.channel_name
            self.client_id = params.client_id
            self.store = params.store
            self.kubemq_address = params.kubemq_address
            self.encryptionHeader = params.encryptionHeader
        else:
            self.channel_name = channel_name
            self.client_id = client_id
            self.store = store
            self.kubemq_address = kubemq_address
            self.encryptionHeader = encryptionHeader

        if not self.channel_name:
            raise ValueError("channel_name parameter is mandatory")

        self.sender = Sender(self.kubemq_address, self.encryptionHeader)

    def __repr__(self):
        return "<Channel channel_name:%s client_id:%s store:%s kubemq_address:%s encryptionHeader:%s>" % (
            self.channel_name,
            self.client_id,
            self.store,
            self.kubemq_address,
            self.encryptionHeader
        )

    def send_event(self, event):
        """Sending a new Event using KubeMQ."""
        low_level_event = self.create_low_level_event(event)
        return self.sender.send_event(low_level_event)

    def stream_event(self, stream, handler=None):
        """Sending a new Event using KubeMQ."""

        def stream_handler():
            for event in stream:
                yield self.create_low_level_event(event)

        return self.sender.stream_event(stream_handler(), handler)

    def create_low_level_event(self, notification):
        return Event(
            channel=self.channel_name,
            client_id=self.client_id,
            store=self.store,
            event_id=notification.event_id,
            body=notification.body,
            metadata=notification.metadata,
        )
