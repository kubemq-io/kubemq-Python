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
import grpc
from kubemq.basic import configuration_loader
from kubemq.grpc import kubemq_pb2_grpc


class ServerAddressNotSuppliedException(Exception):
    def __init__(self):
        self.message = "Server Address was not supplied"

    def __str__(self):
        return str(self.message)


class GrpcClient:
    _kubemq_address = None
    _metadata = None

    _channel = None
    _client = None

    def __init__(self, encryptionHeader):
        self._init_registration(encryptionHeader)

    def get_kubemq_client(self):
        if not self._client:
            if not self._channel:
                kubemq_address = self.get_kubemq_address()
                client_cert_file = configuration_loader.get_certificate_file()
                if client_cert_file:
                    # Open SSL/TLS connection
                    with open(client_cert_file, 'rb') as f:
                        credentials = grpc.ssl_channel_credentials(f.read())
                    self._channel = grpc.secure_channel(kubemq_address, credentials)
                else:
                    # Open Insecure connection
                    self._channel = grpc.insecure_channel(kubemq_address)
            self._client = kubemq_pb2_grpc.kubemqStub(self._channel)

        return self._client

    def get_kubemq_address(self):
        if self._kubemq_address:
            return self._kubemq_address

        self._kubemq_address = configuration_loader.get_server_address()

        if not self._kubemq_address:
            raise ServerAddressNotSuppliedException()

        return self._kubemq_address

    def _init_registration(self, encryptionHeader):
        if encryptionHeader:
            self._metadata = [("authorization", encryptionHeader)]
