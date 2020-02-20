from kubemq.commandquery.lowlevel.initiator import Initiator
from kubemq.commandquery.lowlevel.request import Request
from kubemq.commandquery.request_type import RequestType


def create_low_level_request(request_type):
    return Request(
        body="Request".encode('UTF-8'),
        metadata="MyMetadata",
        cache_key="",
        cache_ttl=0,
        channel="MyTestChannelName",
        client_id="CommandQueryInitiator",
        timeout=111000,
        request_type=request_type,
        tags=[
            ('key', 'value'),
            ('key2', 'value2'),
        ]
    )


if __name__ == "__main__":
    print("Starting CommandQueryInitiator example...\n")

    initiator = Initiator("localhost:50000")
    response = initiator.send_request(create_low_level_request(RequestType.Query))
    print("Recieved response")
    initiator.send_request(create_low_level_request(RequestType.Command))
