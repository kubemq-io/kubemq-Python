"""KubeMQ Python SDK.

A Python client library for KubeMQ message broker.
Supports PubSub (events/events_store), Queues, and Commands/Queries patterns.

Public API Contract:
    All types listed in ``__all__`` are the stable public API.
    Import directly: ``from kubemq import ClientConfig, PubSubClient``

    Importing from subpackages (e.g., ``from kubemq.core.config import ClientConfig``)
    is NOT part of the public API contract and may break in future versions.

    Modules under ``kubemq._internal`` are private implementation details
    and must not be imported by user code.

Sync Example:
    # PubSub client (sync)
    from kubemq import PubSubClient, EventMessage

    with PubSubClient(address="localhost:50000") as client:
        client.publish_event(EventMessage(
            channel="my-channel",
            body=b"Hello, World!"
        ))

Async Example (recommended for async applications):
    # PubSub client (native async)
    from kubemq import AsyncPubSubClient, EventMessage

    async with AsyncPubSubClient(address="localhost:50000") as client:
        await client.publish_event(EventMessage(
            channel="my-channel",
            body=b"Hello, World!"
        ))

    # Queues client (native async)
    from kubemq import AsyncQueuesClient, QueueMessage

    async with AsyncQueuesClient(address="localhost:50000") as client:
        result = await client.send_queue_message(QueueMessage(
            channel="my-queue",
            body=b"Hello, World!"
        ))

    # CQ client (native async)
    from kubemq import AsyncCQClient, CommandMessage

    async with AsyncCQClient(address="localhost:50000") as client:
        response = await client.send_command(CommandMessage(
            channel="my-commands",
            body=b"do something",
            timeout_in_seconds=30,
        ))
"""

from __future__ import annotations

from importlib.metadata import version as _metadata_version

# Logging implementations
from kubemq._internal.logging import NoOpLogger, StdLibLoggerAdapter


# Async cancellation token
from kubemq.common.async_cancellation_token import AsyncCancellationToken

# Common utilities
from kubemq.common.cancellation_token import CancellationToken

# Channel statistics
from kubemq.common.channel_stats import (
    CQChannel,
    PubSubChannel,
    QueuesChannel,
)

# Core configuration
from kubemq.core.config import (
    ClientConfig,
    JitterType,
    KeepAliveConfig,
    OperationTimeouts,
    RetryPolicy,
    TLSConfig,
    resolve_timeout,
)

# Core exceptions
from kubemq.core.exceptions import (
    ERROR_CLASSIFICATION,
    ErrorCategory,
    ErrorCode,
    KubeMQAuthenticationError,
    KubeMQBufferFullError,
    KubeMQCancellationError,
    KubeMQChannelError,
    KubeMQCircuitOpenError,
    KubeMQClientClosedError,
    KubeMQConfigurationError,
    KubeMQConnectionError,
    KubeMQConnectionNotReadyError,
    KubeMQError,
    KubeMQHandlerError,
    KubeMQMessageError,
    KubeMQStreamBrokenError,
    KubeMQTimeoutError,
    KubeMQTransactionError,
    KubeMQTransportError,
    KubeMQValidationError,
    classify_error,
    classify_tls_error,
    from_grpc_error,
)

# Health checking
from kubemq.core.health import (
    AsyncHealthChecker,
    HealthCheck,
    HealthChecker,
    HealthReport,
    HealthStatus,
)

# Core types
from kubemq.core.types import (
    AsyncCredentialProvider,
    ConnectionState,
    CredentialProvider,
    Logger,
    ServerInfo,
    StartPosition,
    SubscribeType,
    TransportProtocol,
)
from kubemq.cq import Client as CQClient
from kubemq.cq.async_client import AsyncClient as AsyncCQClient

# CQ messages and types
from kubemq.cq.command_message import CommandMessage
from kubemq.cq.command_message_received import CommandReceived
from kubemq.cq.command_response_message import CommandResponse
from kubemq.cq.commands_subscription import CommandsSubscription
from kubemq.cq.queries_subscription import QueriesSubscription
from kubemq.cq.query_message import QueryMessage
from kubemq.cq.query_message_received import QueryReceived
from kubemq.cq.query_response_message import QueryResponse

# Sync domain clients with convenient aliases
from kubemq.pubsub import Client as PubSubClient

# Native async domain clients (Phase 4)
from kubemq.pubsub.async_client import AsyncClient as AsyncPubSubClient

# PubSub messages and types
from kubemq.pubsub.event_message import EventMessage
from kubemq.pubsub.event_message_received import EventReceived
from kubemq.pubsub.event_send_result import EventStoreResult
from kubemq.pubsub.event_store_message import EventStoreMessage
from kubemq.pubsub.event_store_message_received import EventStoreReceived
from kubemq.pubsub.events_store_subscription import EventsStoreSubscription
from kubemq.pubsub.events_subscription import EventsSubscription
from kubemq.queues import Client as QueuesClient
from kubemq.queues.async_client import AsyncClient as AsyncQueuesClient, AsyncQueuesPollResponse

# Queues messages and types
from kubemq.queues.queues_message import QueueMessage
from kubemq.queues.queues_message_received import QueueMessageReceived
from kubemq.queues.queues_poll_response import QueuesPollResponse
from kubemq.queues.queues_send_result import QueueSendResult

# Version — single source of truth from pyproject.toml via installed metadata
__version__: str
try:
    __version__ = _metadata_version("kubemq")
except Exception:
    __version__ = "0.0.0.dev0"

__all__ = [
    # Version
    "__version__",
    # Core exceptions and error types
    "ErrorCode",
    "ErrorCategory",
    "ERROR_CLASSIFICATION",
    "classify_error",
    "KubeMQError",
    "KubeMQConnectionError",
    "KubeMQAuthenticationError",
    "KubeMQTimeoutError",
    "KubeMQValidationError",
    "KubeMQChannelError",
    "KubeMQMessageError",
    "KubeMQTransactionError",
    "KubeMQConfigurationError",
    "KubeMQCircuitOpenError",
    "KubeMQClientClosedError",
    "KubeMQConnectionNotReadyError",
    "KubeMQBufferFullError",
    "KubeMQCancellationError",
    "KubeMQTransportError",
    "KubeMQHandlerError",
    "KubeMQStreamBrokenError",
    "classify_tls_error",
    "from_grpc_error",
    # Core configuration
    "TLSConfig",
    "KeepAliveConfig",
    "ClientConfig",
    "JitterType",
    "RetryPolicy",
    "OperationTimeouts",
    "resolve_timeout",
    # Core types
    "ConnectionState",
    "SubscribeType",
    "StartPosition",
    "ServerInfo",
    "CredentialProvider",
    "AsyncCredentialProvider",
    "Logger",
    "TransportProtocol",
    # Logging implementations
    "NoOpLogger",
    "StdLibLoggerAdapter",
    # Sync domain clients
    "PubSubClient",
    "QueuesClient",
    "CQClient",
    # Native async domain clients (Phase 4)
    "AsyncPubSubClient",
    "AsyncQueuesClient",
    "AsyncQueuesPollResponse",
    "AsyncCQClient",
    # Cancellation tokens
    "CancellationToken",
    "AsyncCancellationToken",
    # Health checking
    "HealthStatus",
    "HealthCheck",
    "HealthReport",
    "HealthChecker",
    "AsyncHealthChecker",
    # PubSub messages
    "EventMessage",
    "EventReceived",
    "EventStoreResult",
    "EventStoreMessage",
    "EventStoreReceived",
    "EventsSubscription",
    "EventsStoreSubscription",
    # Queues messages
    "QueueMessage",
    "QueueMessageReceived",
    "QueueSendResult",
    "QueuesPollResponse",
    # CQ messages
    "CommandMessage",
    "CommandReceived",
    "CommandResponse",
    "CommandsSubscription",
    "QueryMessage",
    "QueryReceived",
    "QueryResponse",
    "QueriesSubscription",
    # Channel statistics
    "PubSubChannel",
    "QueuesChannel",
    "CQChannel",
]
