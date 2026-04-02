import logging
import time
import uuid
from collections.abc import Callable
from typing import Any

import grpc

from kubemq.common.channel_stats import (
    CQChannel,
    PubSubChannel,
    QueuesChannel,
    decode_cq_channel_list,
    decode_pub_sub_channel_list,
    decode_queues_channel_list,
)
from kubemq.core.exceptions import KubeMQChannelError, from_grpc_error
from kubemq.grpc import Request

requests_channel = "kubemq.cluster.internal.requests"


def create_channel_request(
    transport: Any, client_id: str | None, channel_name: str, channel_type: str
) -> bool:
    """This method creates a request to create a channel in the Kubemq server.

    Parameters:
    - transport: The transport object that handles the communication with the Kubemq server.
    - client_id: The ID of the client.
    - channel_name: The name of the channel to create.
    - channel_type: The type of the channel.

    Returns:
    - bool: True if the channel creation was successful.
    - None: If an error occurred during the channel creation.

    Raises:
    - KubeMQChannelError: If the channel creation request was executed but with an error response from the server.
    - KubeMQError: If there was an error with the gRPC communication with the Kubemq server.

    """
    try:
        request = Request(
            RequestID=str(uuid.uuid4()),
            RequestTypeData=2,  # type: ignore[arg-type]
            Metadata="create-channel",
            Channel=requests_channel,
            ClientID=client_id,
            Tags={
                "channel_type": channel_type,
                "channel": channel_name,
                "client_id": client_id or "",
            },
            Timeout=10 * 1000,
        )
        response = transport.kubemq_client().SendRequest(request)
        if response:
            if response.Executed:
                return True
            else:
                raise KubeMQChannelError(f"Create Channel Error: {response.Error}")
        return False
    except grpc.RpcError as e:
        raise from_grpc_error(e) from e


def delete_channel_request(
    transport: Any, client_id: str | None, channel_name: str, channel_type: str
) -> bool:
    """This method is used to send a delete channel request to the Kubemq server. It deletes a channel with the specified name and type.

    Parameters:
    - transport: The transport object used for communication with the Kubemq server.
    - client_id: The client ID associated with the request.
    - channel_name: The name of the channel to be deleted.
    - channel_type: The type of the channel to be deleted.

    Returns:
    - If the delete channel request is executed successfully, it returns True.
    - If there is an error during the execution of the delete channel request, it raises a KubeMQChannelError with the corresponding error message.

    Raises:
    - KubeMQError: If there is a GRPC error during the process.

    """
    try:
        request = Request(
            RequestID=str(uuid.uuid4()),
            RequestTypeData=2,  # type: ignore[arg-type]
            Metadata="delete-channel",
            Channel=requests_channel,
            ClientID=client_id,
            Tags={
                "channel_type": channel_type,
                "channel": channel_name,
                "client_id": client_id or "",
            },
            Timeout=10 * 1000,
        )
        response = transport.kubemq_client().SendRequest(request)
        if response:
            if response.Executed:
                return True
            else:
                raise KubeMQChannelError(f"Delete Channel Error: {response.Error}")
        return False
    except grpc.RpcError as e:
        raise from_grpc_error(e) from e


_LIST_MAX_RETRIES = 3
_LIST_RETRY_DELAY = 2.0
_SNAPSHOT_NOT_READY = "cluster snapshot not ready yet"

_logger = logging.getLogger("kubemq.common.requests")


def _is_retryable_list_error(response_error: str | None, rpc_error: grpc.RpcError | None) -> bool:
    if response_error and _SNAPSHOT_NOT_READY in response_error:
        return True
    return bool(
        rpc_error
        and hasattr(rpc_error, "code")
        and rpc_error.code() == grpc.StatusCode.DEADLINE_EXCEEDED
    )


def _list_channels_with_retry(
    transport: Any,
    client_id: str | None,
    channel_type: str,
    channel_search: str,
    decode_fn: Callable[[bytes], list[Any]],
) -> list[Any]:
    """Shared retry wrapper for all list-channels operations.

    Retries on transient errors such as 'cluster snapshot not ready yet'
    and DEADLINE_EXCEEDED, which occur when the request misses the master node.
    """
    last_error: Exception | None = None
    for attempt in range(_LIST_MAX_RETRIES):
        try:
            request = Request(
                RequestID=str(uuid.uuid4()),
                RequestTypeData=2,  # type: ignore[arg-type]
                Metadata="list-channels",
                Channel=requests_channel,
                ClientID=client_id,
                Tags={"channel_type": channel_type, "channel_search": channel_search},
                Timeout=10 * 1000,
            )
            response = transport.kubemq_client().SendRequest(request)
            if response:
                if response.Executed:
                    return decode_fn(response.Body)
                else:
                    if (
                        _is_retryable_list_error(response.Error, None)
                        and attempt < _LIST_MAX_RETRIES - 1
                    ):
                        _logger.warning(
                            "List channels attempt %d failed: %s, retrying...",
                            attempt + 1,
                            response.Error,
                        )
                        time.sleep(_LIST_RETRY_DELAY)
                        continue
                    raise KubeMQChannelError(f"List Channels Error: {response.Error}")
            return []
        except grpc.RpcError as e:
            if _is_retryable_list_error(None, e) and attempt < _LIST_MAX_RETRIES - 1:
                _logger.warning("List channels attempt %d gRPC error, retrying...", attempt + 1)
                last_error = e
                time.sleep(_LIST_RETRY_DELAY)
                continue
            raise from_grpc_error(e) from e
    if last_error:
        raise from_grpc_error(last_error) from last_error
    return []


def list_queues_channels(
    transport: Any, client_id: str | None, channel_search: str
) -> list[QueuesChannel]:
    """List queues channels with retry on transient errors."""
    return _list_channels_with_retry(
        transport, client_id, "queues", channel_search, decode_queues_channel_list
    )


def list_pubsub_channels(
    transport: Any, client_id: str | None, channel_type: str, channel_search: str
) -> list[PubSubChannel]:
    """List pub/sub channels with retry on transient errors."""
    return _list_channels_with_retry(
        transport, client_id, channel_type, channel_search, decode_pub_sub_channel_list
    )


def list_cq_channels(
    transport: Any, client_id: str | None, channel_type: str, channel_search: str
) -> list[CQChannel]:
    """List CQ channels with retry on transient errors."""
    return _list_channels_with_retry(
        transport, client_id, channel_type, channel_search, decode_cq_channel_list
    )
