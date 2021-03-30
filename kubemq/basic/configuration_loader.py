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
import os

_path = None  # ServerAddress.
_key = None  # RegistrationKey.
_cert = None  # Certificate file.


def get_server_address():
    global _path

    if _path:
        return _path

    _path = get_from_environment_variable("KubeMQServerAddress")
    if _path:
        return _path
    else:
        _path = get_from_environment_variable("KUBEMQSERVERADDRESS")
        if _path:
            return _path

    return _path


def get_registration_key():
    global _key

    if _key:
        return _key

    _key = get_from_environment_variable("KubeMQRegistrationKey")
    if _key:
        return _key
    else:
        _key = get_from_environment_variable("KUBEMQREGISTRATIONKEY")
        if _key:
            return _key
        else:
            _key = ""

    return _key


def get_certificate_file():
    global _cert

    if _cert:
        return _cert

    _cert = get_from_environment_variable("KubeMQCertificateFile")
    if _cert:
        return _cert
    else:
        _cert = get_from_environment_variable("KUBEMQCERTIFICATEFILE")
        if _cert:
            return _cert
        else:
            _cert = ""

    return _cert


def get_from_environment_variable(key):
    return os.environ.get(key)
