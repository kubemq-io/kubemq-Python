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

    def __init__(self, channel_name=None, client_id=None, timeout=None, cache_key=None, cache_ttl=None,
                 request_type=None, kubemq_address=None, encryptionHeader=None):
        """
        Initializes a new instance of the ChannelParameters

        :param str channel_name: Represents The channel name to send to using the KubeMQ.
        :param str client_id: Represents the sender ID that the messages will be send under.
        :param int timeout: Represents the limit for waiting for response (Milliseconds).
        :param str cache_key: Represents if the request should be saved from Cache and under what "Key"(Str) to save it.
        :param int cache_ttl: Cache time to live : for how long does the request should be saved in Cache.
        :param RequestType request_type: Represents the type of request operation.
        :param str kubemq_address: Represents The address of the KubeMQ server.
        :param byte[] encryptionHeader: the encrypted header requested by kubemq authentication.
        """

        self.channel_name = channel_name
        """Represents The channel name to send to using the KubeMQ."""

        self.client_id = client_id
        """Represents the sender ID that the messages will be send under."""

        self.timeout = timeout
        """Represents the limit for waiting for response (Milliseconds)."""

        self.cache_key = cache_key
        """Represents if the request should be saved from Cache and under what "Key"(str) to save it."""

        self.cache_ttl = cache_ttl
        """Cache time to live : for how long does the request should be saved in Cache."""

        self.request_type = request_type
        """Represents the type of request operation."""

        self.kubemq_address = kubemq_address
        """Represents The address of the KubeMQ server."""

        self.encryptionHeader = encryptionHeader
        """the header for authentication using kubemq."""
