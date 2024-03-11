import uuid
import grpc
from kubemq.transport import transport
from kubemq.common.exceptions import *
from kubemq.common.helpers import decode_grpc_error
from kubemq.grpc import (Request,Response)
from kubemq.common.channel_stats import *

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

def list_queues_channels(transport,client_id,channel_search)->List[QueuesChannel]:
    try:
        request = Request(
            RequestID=str(uuid.uuid4()),
            RequestTypeData=2,
            Metadata="list-channels",
            Channel=requests_channel,
            ClientID=client_id,
            Tags={"channel_type": "queues", "channel_search": channel_search},
            Timeout=10 * 1000
        )
        response = transport.kubemq_client().SendRequest(request)
        if response:
            if response.Executed:
                return decode_queues_channel_list(response.Body)
            else:
                self.logger.error(f"Client failed to list {channel_type} channels, error: {response.Error}")
                raise ListChannelsError(response.Error)
    except grpc.RpcError as e:
        raise GRPCError(decode_grpc_error(e))
def list_pubsub_channels(transport,client_id,channel_type: str, channel_search)->List[PubSubChannel]:
    try:
        request = Request(
            RequestID=str(uuid.uuid4()),
            RequestTypeData=2,
            Metadata="list-channels",
            Channel=requests_channel,
            ClientID=client_id,
            Tags={"channel_type": channel_type, "channel_search": channel_search},
            Timeout=10 * 1000
            )
        response = transport.kubemq_client().SendRequest(request)
        if response:
            if response.Executed:
                return decode_pub_sub_channel_list(response.Body)
            else:
                self.logger.error(f"Client failed to list {channel_type} channels, error: {response.Error}")
                raise ListChannelsError(response.Error)
    except grpc.RpcError as e:
        raise GRPCError(decode_grpc_error(e))

def list_cq_channels(transport,client_id,channel_type: str, channel_search)->List[CQChannel]:
    try:
        request = Request(
            RequestID=str(uuid.uuid4()),
            RequestTypeData=2,
            Metadata="list-channels",
            Channel=requests_channel,
            ClientID=client_id,
            Tags={"channel_type": channel_type, "channel_search": channel_search},
            Timeout=10 * 1000
            )
        response = transport.kubemq_client().SendRequest(request)
        if response:
            if response.Executed:
                return decode_cq_channel_list(response.Body)
            else:
                self.logger.error(f"Client failed to list {channel_type} channels, error: {response.Error}")
                raise ListChannelsError(response.Error)
    except grpc.RpcError as e:
        raise GRPCError(decode_grpc_error(e))