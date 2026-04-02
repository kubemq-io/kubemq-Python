"""Legacy common module — import from kubemq package directly.

All public types are available from ``import kubemq`` or ``from kubemq import ...``.
Direct imports from ``kubemq.common.*`` are deprecated and may be removed in v5.0.
"""

from .async_cancellation_token import AsyncCancellationToken, CancellationTokenBridge
from .cancellation_token import CancellationToken
from .channel_stats import (
    CQChannel,
    CQStats,
    PubSubChannel,
    PubSubStats,
    QueuesChannel,
    QueuesStats,
    decode_cq_channel_list,
    decode_pub_sub_channel_list,
    decode_queues_channel_list,
)
from .exceptions import (
    BaseError,
    KubeMQChannelError,
    KubeMQConnectionError,
    KubeMQMessageError,
    KubeMQTransactionError,
    KubeMQValidationError,
    from_grpc_error,
)
from .helpers import decode_grpc_error, is_channel_error
from .requests import create_channel_request
from .subscribe_type import SubscribeType

__all__ = [
    "CancellationToken",
    "AsyncCancellationToken",
    "CancellationTokenBridge",
    "SubscribeType",
    "create_channel_request",
    # Channel stats
    "CQChannel",
    "CQStats",
    "PubSubChannel",
    "PubSubStats",
    "QueuesChannel",
    "QueuesStats",
    "decode_cq_channel_list",
    "decode_pub_sub_channel_list",
    "decode_queues_channel_list",
    # Exceptions
    "BaseError",
    "KubeMQChannelError",
    "KubeMQConnectionError",
    "KubeMQMessageError",
    "KubeMQTransactionError",
    "KubeMQValidationError",
    "from_grpc_error",
    # Helpers
    "decode_grpc_error",
    "is_channel_error",
]
