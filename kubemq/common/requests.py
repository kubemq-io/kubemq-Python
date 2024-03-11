import uuid
from kubemq.transport import transport
from kubemq.common.exceptions import *
from kubemq.common.helpers import decode_grpc_error
from kubemq.grpc import (Request,Response)

requests_channel="kubemq.cluster.internal.requests"
def create_channel_request(transport,client_id, channel_name, channel_type)->[bool,None]:
    try:
        request = Request(
            RequestID=str(uuid.uuid4()),
            RequestTypeData=2,
            Metadata="create-channel",
            Channel=requests_channel,
            ClientID=client_id,
            Tags={"channel_type": channel_type, "channel": channel_name, "client_id": client_id},
            Timeout=10 * 1000
        )
        response = transport.kubemq_client().SendRequest(request)
        if response:
            if response.Executed:
                return True
            else:
                raise CreateChannelError(response.Error)
    except grpc.RpcError as e:
        raise GRPCError(decode_grpc_error(e))

def delete_channel_request(transport,client_id, channel_name, channel_type)->[bool,None]:
    try:
        request = Request(
            RequestID=str(uuid.uuid4()),
            RequestTypeData=2,
            Metadata="delete-channel",
            Channel=requests_channel,
            ClientID=client_id,
            Tags={"channel_type": channel_type, "channel": channel_name, "client_id": client_id},
            Timeout=10 * 1000
        )
        response = transport.kubemq_client().SendRequest(request)
        if response:
            if response.Executed:
                return True
            else:
                raise DeleteChannelError(response.Error)
    except grpc.RpcError as e:
        raise GRPCError(decode_grpc_error(e))