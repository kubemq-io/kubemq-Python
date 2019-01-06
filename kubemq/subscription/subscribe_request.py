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
from kubemq.grpc import Subscribe
from kubemq.subscription.subscribe_type import SubscribeType


class SubscribeRequest:
    """Represents a set of parameters which the Subscriber uses to subscribe to the KubeMQ."""

    def __init__(self, subscribe_type=None, client_id=None, channel=None, events_store_type=None,
                 events_store_type_value=None, group=""):
        self.subscribe_type = subscribe_type
        """Represents the type of Subscriber operation."""

        self.client_id = client_id
        """Represents an identifier that will subscribe to kubeMQ under."""

        self.channel = channel
        """Represents the channel name that will subscribe to under kubeMQ."""

        self.events_store_type = events_store_type
        """Represents the type of subscription to persistence"""

        self.events_store_type_value = events_store_type_value
        """Represents the value of subscription to persistence queue."""

        self.group = group
        """Represents the group the channel is assign to , if not filled will be empty string(no group)."""

    def from_inner_subscribe_request(self, inner):
        self.subscribe_type = SubscribeType(inner.SubscribeTypeData)
        self.client_id = inner.ClientID
        self.channel = inner.Channel
        self.group = inner.Group or ""
        self.events_store_type_value = inner.EventsStoreTypeValue

    def to_inner_subscribe_request(self):
        request = Subscribe()
        request.SubscribeTypeData = self.subscribe_type.value
        request.ClientID = self.client_id
        request.Channel = self.channel
        request.Group = self.group or ""
        request.EventsStoreTypeData = self.events_store_type.value
        request.EventsStoreTypeValue = self.events_store_type_value
        return request

    def is_valid_type(self, subscriber):
        if subscriber == "CommandQuery":
            return self.subscribe_type == SubscribeType.Commands or self.subscribe_type == SubscribeType.Queries
        else:  # subscriber == "Events"
            return self.subscribe_type == SubscribeType.Events or self.subscribe_type == SubscribeType.EventsStore
