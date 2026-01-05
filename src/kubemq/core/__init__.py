"""Core module for KubeMQ Python SDK."""

from __future__ import annotations

from kubemq.core.client import (
    AsyncBaseClient,
    BaseClient,
    NativeAsyncBaseClient,
)
from kubemq.core.config import (
    ClientConfig,
    KeepAliveConfig,
    TLSConfig,
)
from kubemq.core.exceptions import (
    KubeMQAuthenticationError,
    KubeMQChannelError,
    KubeMQCircuitOpenError,
    KubeMQConnectionError,
    KubeMQError,
    KubeMQMessageError,
    KubeMQTimeoutError,
    KubeMQTransactionError,
    KubeMQValidationError,
    from_grpc_error,
)
from kubemq.core.health import (
    AsyncHealthChecker,
    HealthCheck,
    HealthChecker,
    HealthReport,
    HealthStatus,
)
from kubemq.core.messages import (
    BaseMessage,
    BaseReceivedMessage,
    BaseResponse,
)
from kubemq.core.types import (
    AnyErrorCallback,
    AsyncCallback,
    AsyncCloseable,
    AsyncErrorCallback,
    AsyncPingable,
    Callback,
    Closeable,
    ErrorCallback,
    Pingable,
    ServerInfo,
    StartPosition,
    SubscribeType,
    SyncCallback,
    SyncErrorCallback,
)

__all__ = [
    # Exceptions
    "KubeMQError",
    "KubeMQConnectionError",
    "KubeMQAuthenticationError",
    "KubeMQTimeoutError",
    "KubeMQValidationError",
    "KubeMQChannelError",
    "KubeMQMessageError",
    "KubeMQTransactionError",
    "KubeMQCircuitOpenError",
    "from_grpc_error",
    # Types
    "SubscribeType",
    "StartPosition",
    "ServerInfo",
    "SyncCallback",
    "AsyncCallback",
    "Callback",
    "ErrorCallback",
    "SyncErrorCallback",
    "AsyncErrorCallback",
    "AnyErrorCallback",
    "Closeable",
    "AsyncCloseable",
    "Pingable",
    "AsyncPingable",
    # Config
    "TLSConfig",
    "KeepAliveConfig",
    "ClientConfig",
    # Client
    "BaseClient",
    "AsyncBaseClient",
    "NativeAsyncBaseClient",
    # Messages
    "BaseMessage",
    "BaseResponse",
    "BaseReceivedMessage",
    # Health
    "HealthStatus",
    "HealthCheck",
    "HealthReport",
    "HealthChecker",
    "AsyncHealthChecker",
]
