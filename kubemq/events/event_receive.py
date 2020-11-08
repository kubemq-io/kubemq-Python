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


class EventReceive:
    def __init__(self, inner_event_receive=None):
        if inner_event_receive:
            self.event_id = inner_event_receive.EventID
            """Represents a event identifier."""

            self.channel = inner_event_receive.Channel
            """Represents The channel name to send to using the KubeMQ."""

            self.metadata = inner_event_receive.Metadata
            """Represents text as str"""

            self.body = inner_event_receive.Body
            """Represents The content of the KubeMQRequestReceive."""

            self.timestamp = inner_event_receive.Timestamp
            """Represents the timestamp the message arrived """

            self.sequence = inner_event_receive.Sequence
            """"Represents the sequnce of the event (set by Kubemq)"""

            self.tags = inner_event_receive.Tags
            """Represents key value pairs that help distinguish the message"""
