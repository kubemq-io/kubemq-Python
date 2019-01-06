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


class RequestParameters:
    def __init__(self, timeout=None, cache_key=None, cache_ttl=None):
        """
        Initializes a new instance of RequestParameters
        :param int timeout: Represents the limit for waiting for response (Milliseconds).
        :param str cache_key: Represents if the request should be saved from Cache and under what "Key"(str) to save it.
        :param int cache_ttl: Cache time to live : for how long does the request should be saved in Cache.
        """
        self.timeout = timeout
        """Represents the limit for waiting for response (Milliseconds)."""

        self.cache_key = cache_key
        """Represents if the request should be saved from Cache and under what "Key"(str) to save it."""

        self.cache_ttl = cache_ttl
        """Cache time to live : for how long does the request should be saved in Cache."""
