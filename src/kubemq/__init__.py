"""KubeMQ Python SDK.

A Python client library for KubeMQ message broker.
Supports PubSub (events/events_store), Queues, and Commands/Queries patterns.

Sync Example:
    # PubSub client (sync)
    from kubemq import PubSubClient, EventMessage

    with PubSubClient(address="localhost:50000") as client:
        client.send_events_message(EventMessage(
            channel="my-channel",
            body=b"Hello, World!"
        ))

Async Example (recommended for async applications):
    # PubSub client (native async)
    from kubemq import AsyncPubSubClient, EventMessage

    async with AsyncPubSubClient(address="localhost:50000") as client:
        await client.send_event(EventMessage(
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
    KeepAliveConfig,
    TLSConfig,
)

# Core exceptions
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
    ServerInfo,
    StartPosition,
    SubscribeType,
)
from kubemq.cq import Client as CQClient
from kubemq.cq.async_client import AsyncClient as AsyncCQClient

# CQ messages and types
from kubemq.cq.command_message import CommandMessage
from kubemq.cq.command_message_received import CommandMessageReceived
from kubemq.cq.command_response_message import CommandResponseMessage
from kubemq.cq.commands_subscription import CommandsSubscription
from kubemq.cq.queries_subscription import QueriesSubscription
from kubemq.cq.query_message import QueryMessage
from kubemq.cq.query_message_received import QueryMessageReceived
from kubemq.cq.query_response_message import QueryResponseMessage

# Sync domain clients with convenient aliases
from kubemq.pubsub import Client as PubSubClient

# Native async domain clients (Phase 4)
from kubemq.pubsub.async_client import AsyncClient as AsyncPubSubClient

# PubSub messages and types
from kubemq.pubsub.event_message import EventMessage
from kubemq.pubsub.event_message_received import EventMessageReceived
from kubemq.pubsub.event_send_result import EventSendResult
from kubemq.pubsub.event_store_message import EventStoreMessage
from kubemq.pubsub.event_store_message_received import EventStoreMessageReceived
from kubemq.pubsub.events_store_subscription import EventsStoreSubscription
from kubemq.pubsub.events_subscription import EventsSubscription
from kubemq.queues import Client as QueuesClient
from kubemq.queues.async_client import AsyncClient as AsyncQueuesClient, AsyncQueuesPollResponse

# Queues messages and types
from kubemq.queues.queues_message import QueueMessage
from kubemq.queues.queues_message_received import QueueMessageReceived
from kubemq.queues.queues_poll_response import QueuesPollResponse
from kubemq.queues.queues_send_result import QueueSendResult

# Version
__version__ = "4.0.0"

__all__ = [
    # Version
    "__version__",
    # Core exceptions
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
    # Core configuration
    "TLSConfig",
    "KeepAliveConfig",
    "ClientConfig",
    # Core types
    "SubscribeType",
    "StartPosition",
    "ServerInfo",
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
    "EventMessageReceived",
    "EventSendResult",
    "EventStoreMessage",
    "EventStoreMessageReceived",
    "EventsSubscription",
    "EventsStoreSubscription",
    # Queues messages
    "QueueMessage",
    "QueueMessageReceived",
    "QueueSendResult",
    "QueuesPollResponse",
    # CQ messages
    "CommandMessage",
    "CommandMessageReceived",
    "CommandResponseMessage",
    "CommandsSubscription",
    "QueryMessage",
    "QueryMessageReceived",
    "QueryResponseMessage",
    "QueriesSubscription",
    # Channel statistics
    "PubSubChannel",
    "QueuesChannel",
    "CQChannel",
]
