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


class ChannelParameters:
    """Configuration parameters for a Channel."""

    def __init__(self, channel_name=None, client_id=None, store=None, kubemq_address=None, return_result=False,
                 encryptionHeader=None):
        """
        Initializes a new instance of the ChannelParameters class with set parameters.

        :param channel_name: Represents The channel name to send to using the KubeMQ.
        :param client_id: Represents the sender ID that the messages will be send under.
        :param store: Represents the channel persistence property.
        :param kubemq_address: Represents The address of the KubeMQ server.
        :param byte[] encryptionHeader: the encrypted header requested by kubemq authentication.
        """
        self.channel_name = channel_name
        self.client_id = client_id
        self.store = store
        self.kubemq_address = kubemq_address
        self.encryptionHeader = encryptionHeader
        """Represents the header for authentication using kubemq."""

    def __repr__(self):
        return "<ChannelParameters channel_name:%s client_id:%s store:%s kubemq_address:%s encryptionHeader:%s>" % (
            self.channel_name,
            self.client_id,
            self.store,
            self.kubemq_address,
            self.encryptionHeader
        )
