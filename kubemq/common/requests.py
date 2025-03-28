import uuid
import grpc
from kubemq.common.exceptions import *
from kubemq.common.helpers import decode_grpc_error
from kubemq.grpc import Request
from kubemq.common.channel_stats import *

requests_channel = "kubemq.cluster.internal.requests"


def create_channel_request(
    transport, client_id, channel_name, channel_type
) -> [bool, None]:
    """

    This method creates a request to create a channel in the Kubemq server.

    Parameters:
    - transport: The transport object that handles the communication with the Kubemq server.
    - client_id: The ID of the client.
    - channel_name: The name of the channel to create.
    - channel_type: The type of the channel.

    Returns:
    - bool: True if the channel creation was successful.
    - None: If an error occurred during the channel creation.

    Raises:
    - CreateChannelError: If the channel creation request was executed but with an error response from the server.
    - GRPCError: If there was an error with the gRPC communication with the Kubemq server.

    """
    try:
        request = Request(
            RequestID=str(uuid.uuid4()),
            RequestTypeData=2,
            Metadata="create-channel",
            Channel=requests_channel,
            ClientID=client_id,
            Tags={
                "channel_type": channel_type,
                "channel": channel_name,
                "client_id": client_id,
            },
            Timeout=10 * 1000,
        )
        response = transport.kubemq_client().SendRequest(request)
        if response:
            if response.Executed:
                return True
            else:
                raise CreateChannelError(response.Error)
    except grpc.RpcError as e:
        raise GRPCError(decode_grpc_error(e))


def delete_channel_request(
    transport, client_id, channel_name, channel_type
) -> [bool, None]:
    """

    This method is used to send a delete channel request to the Kubemq server. It deletes a channel with the specified name and type.

    Parameters:
    - transport: The transport object used for communication with the Kubemq server.
    - client_id: The client ID associated with the request.
    - channel_name: The name of the channel to be deleted.
    - channel_type: The type of the channel to be deleted.

    Returns:
    - If the delete channel request is executed successfully, it returns True.
    - If there is an error during the execution of the delete channel request, it raises a DeleteChannelError with the corresponding error message.

    Raises:
    - GRPCError: If there is a GRPC error during the process.

    """
    try:
        request = Request(
            RequestID=str(uuid.uuid4()),
            RequestTypeData=2,
            Metadata="delete-channel",
            Channel=requests_channel,
            ClientID=client_id,
            Tags={
                "channel_type": channel_type,
                "channel": channel_name,
                "client_id": client_id,
            },
            Timeout=10 * 1000,
        )
        response = transport.kubemq_client().SendRequest(request)
        if response:
            if response.Executed:
                return True
            else:
                raise DeleteChannelError(response.Error)
    except grpc.RpcError as e:
        raise GRPCError(decode_grpc_error(e))


def list_queues_channels(transport, client_id, channel_search) -> List[QueuesChannel]:
    """

    List Queues Channels

    This method is used to list the queues channels from the Kubemq server. It takes the following parameters:

    Parameters:
    - `transport` : The transport object used to communicate with the Kubemq server.
    - `client_id` : The client ID of the client making the request.
    - `channel_search` : The search query string to filter the channels. (optional)

    Returns:
    - `List[QueuesChannel]` : A list of QueuesChannel objects representing the queues channels.

    Raises:
    - `ListChannelsError` : If the client fails to list channels, this error will be raised.
    - `GRPCError` : If there is an error while communicating with the Kubemq server, this error will be raised.

    Example Usage:

    ```python
    transport = Transport()
    client_id = "my_client_id"

    # List all queues channels
    queues_channels = list_queues_channels(transport, client_id)

    # List queues channels with a specific search query
    search_query = "search query"
    queues_channels = list_queues_channels(transport, client_id, search_query)
    ```
    """
    try:
        request = Request(
            RequestID=str(uuid.uuid4()),
            RequestTypeData=2,
            Metadata="list-channels",
            Channel=requests_channel,
            ClientID=client_id,
            Tags={"channel_type": "queues", "channel_search": channel_search},
            Timeout=10 * 1000,
        )
        response = transport.kubemq_client().SendRequest(request)
        if response:
            if response.Executed:
                return decode_queues_channel_list(response.Body)
            else:
                self.logger.error(
                    f"Client failed to list {channel_type} channels, error: {response.Error}"
                )
                raise ListChannelsError(response.Error)
    except grpc.RpcError as e:
        raise GRPCError(decode_grpc_error(e))


def list_pubsub_channels(
    transport, client_id, channel_type: str, channel_search
) -> List[PubSubChannel]:
    """

    This method is used to retrieve a list of PubSub channels based on the specified parameters.

    Parameters:
    - transport: The transport object used for communication.
    - client_id: The ID of the client making the request.
    - channel_type (str): The type of PubSub channel to filter the list by.
    - channel_search: A search parameter to further filter the list of channels.

    Returns:
    - List[PubSubChannel]: A list of PubSubChannel objects representing the channels matching the specified parameters.

    Raises:
    - ListChannelsError: If the request fails or is not executed successfully.
    - GRPCError: If a gRPC error occurs during the execution of the request.

    """
    try:
        request = Request(
            RequestID=str(uuid.uuid4()),
            RequestTypeData=2,
            Metadata="list-channels",
            Channel=requests_channel,
            ClientID=client_id,
            Tags={"channel_type": channel_type, "channel_search": channel_search},
            Timeout=10 * 1000,
        )
        response = transport.kubemq_client().SendRequest(request)
        if response:
            if response.Executed:
                return decode_pub_sub_channel_list(response.Body)
            else:
                self.logger.error(
                    f"Client failed to list {channel_type} channels, error: {response.Error}"
                )
                raise ListChannelsError(response.Error)
    except grpc.RpcError as e:
        raise GRPCError(decode_grpc_error(e))


def list_cq_channels(
    transport, client_id, channel_type: str, channel_search
) -> List[CQChannel]:
    """

    Method: list_cq_channels

    Parameters:
    - transport: The transport object used for communication with the Kubemq server.
    - client_id: The ID of the client making the request.
    - channel_type (str): The type of channel to list.
    - channel_search: The search keyword for filtering the channels.

    Returns:
    - List[CQChannel]: A list of CQChannel objects representing the channels that match the search criteria.

    Raises:
    - ListChannelsError: If the client fails to list the channels.
    - GRPCError: If a gRPC error occurs during the request.

    """
    try:
        request = Request(
            RequestID=str(uuid.uuid4()),
            RequestTypeData=2,
            Metadata="list-channels",
            Channel=requests_channel,
            ClientID=client_id,
            Tags={"channel_type": channel_type, "channel_search": channel_search},
            Timeout=10 * 1000,
        )
        response = transport.kubemq_client().SendRequest(request)
        if response:
            if response.Executed:
                return decode_cq_channel_list(response.Body)
            else:
                self.logger.error(
                    f"Client failed to list {channel_type} channels, error: {response.Error}"
                )
                raise ListChannelsError(response.Error)
    except grpc.RpcError as e:
        raise GRPCError(decode_grpc_error(e))
