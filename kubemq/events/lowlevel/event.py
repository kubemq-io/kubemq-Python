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
import threading

from kubemq.grpc import Event as kubeEvent

_lock = threading.Lock()
_counter = 0


def get_next_id():
    """Get an unique thread safety ID between 1 to 65535"""
    global _lock, _counter

    with _lock:
        if _counter == 65535:
            _counter = 1
        else:
            _counter += 1

        return str(_counter)


class Event:

    def __init__(self, channel=None, client_id=None, store=None, event_id=None, body=None, metadata=None, tags=None):
        self.channel = channel
        """Represents The channel name to send to using the KubeMQ ."""

        self.client_id = client_id
        """Represents the sender ID that the events will be send under."""

        self.store = store
        """Represents if the events should be send to persistence."""

        self.event_id = event_id
        """Represents a Event identifier."""

        self.body = body
        """Represents The content of the LowLevel.Event."""

        self.metadata = metadata
        """Represents text as str."""

        self.tags = tags
        """Represents key value pairs that help distinguish the message"""

    def from_inner_event(self, inner_event):
        self.channel = inner_event.Channel
        self.metadata = inner_event.Metadata
        self.body = inner_event.Body
        self.event_id = inner_event.EventID or get_next_id()
        self.client_id = inner_event.ClientID
        self.store = inner_event.Store
        self.tags = inner_event.Tags

    def to_inner_event(self):
        return kubeEvent(
            Channel=self.channel,
            Metadata=self.metadata or "",
            Body=self.body,
            EventID=self.event_id or get_next_id(),
            ClientID=self.client_id,
            Store=self.store,
            Tags=self.tags or ""
        )

    def __repr__(self):
        return "<LowLevel.Event channel:%s client_id:%s store:%s event_id:%s body:%s metadata:%s tags:%s>" % (
            self.channel,
            self.client_id,
            self.store,
            self.event_id,
            self.body,
            self.metadata,
            self.tags
        )
