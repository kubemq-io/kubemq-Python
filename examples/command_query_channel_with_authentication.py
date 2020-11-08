import jwt
from kubemq.commandquery.channel import Channel
from kubemq.commandquery.channel_parameters import ChannelParameters
from kubemq.commandquery.request import Request
from kubemq.commandquery.request_type import RequestType


def create_request_channel_parameters(request_type):
    return ChannelParameters(
        channel_name="MyTestChannelName",
        client_id="CommandQueryChannel",
        timeout=111000,
        cache_key="",
        cache_ttl=0,
        request_type=request_type,
        kubemq_address="localhost:50000",
        encryptionHeader=jwt.encode({}, algorithm="HS256", key="some-key")
    )


def send_query_request():
    request_channel_parameters = create_request_channel_parameters(RequestType.Query)
    request_channel = Channel(channel_parameters=request_channel_parameters)

    request = Request(
        metadata="CommandQueryChannel",
        body="Request".encode('UTF-8'),
        tags=[
            ('key', 'value'),
            ('key2', 'value2'),
        ]
    )

    try:
        response = request_channel.send_request(request)
    except Exception as err:
        print('error, error:%s' % (
            err
        ))


def send_command_request():
    request_channel_parameters = create_request_channel_parameters(RequestType.Command)
    request_channel = Channel(channel_parameters=request_channel_parameters)

    request = Request(
        metadata="CommandQueryChannel",
        body="Request".encode('UTF-8'),
        tags=[
            ('key', 'value'),
            ('key2', 'value2'),
        ]
    )

    try:
        request_channel.send_request(request)
    except Exception as err:
        print('message send error, error:%s' % (
            err
        ))


if __name__ == "__main__":
    print("Starting CommandQueryChannel example...\n")

    send_query_request()
    send_command_request()
