"""Legacy exception module — re-exports from kubemq.core.exceptions.

Import from ``kubemq.core.exceptions`` directly for new code.
"""

from kubemq.core.exceptions import (
    KubeMQChannelError,
    KubeMQConnectionError,
    KubeMQError as BaseError,
    KubeMQMessageError,
    KubeMQTransactionError,
    KubeMQValidationError,
    from_grpc_error,
)

__all__ = [
    "BaseError",
    "KubeMQValidationError",
    "KubeMQConnectionError",
    "KubeMQMessageError",
    "KubeMQChannelError",
    "KubeMQTransactionError",
    "from_grpc_error",
]
