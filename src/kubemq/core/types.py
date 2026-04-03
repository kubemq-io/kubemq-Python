"""Type definitions and protocols for KubeMQ Python SDK."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import datetime
from enum import Enum, auto
from typing import (
    TYPE_CHECKING,
    Any,
    Protocol,
    TypeVar,
    Union,
    runtime_checkable,
)

# Generic type variables
T = TypeVar("T")
MessageT = TypeVar("MessageT", bound="BaseMessage")

# Callback type aliases
SyncCallback = Callable[[T], None]
AsyncCallback = Callable[[T], Awaitable[None]]
Callback = Union[SyncCallback[T], AsyncCallback[T]]

# Error callback types
ErrorCallback = Callable[[Exception], None]
SyncErrorCallback = ErrorCallback  # Alias for consistency
AsyncErrorCallback = Callable[[Exception], Awaitable[None]]

# Combined error callback type (accepts both sync and async)
AnyErrorCallback = Union[ErrorCallback, AsyncErrorCallback]


class ConnectionState(Enum):
    """Connection lifecycle state.

    State transitions:
        IDLE -> CONNECTING -> READY
                  |            |
              RECONNECTING -> READY
                  |
               CLOSED (terminal)
    """

    IDLE = "IDLE"
    CONNECTING = "CONNECTING"
    READY = "READY"
    RECONNECTING = "RECONNECTING"
    CLOSED = "CLOSED"


class SubscribeType(Enum):
    """Subscription types for KubeMQ channels.

    Values match existing kubemq.common.subscribe_type.SubscribeType
    for backward compatibility.
    """

    UNDEFINED = 0
    EVENTS = 1
    EVENTS_STORE = 2
    COMMANDS = 3
    QUERIES = 4


class StartPosition(Enum):
    """Start position for events store subscriptions."""

    START_NEW_ONLY = auto()
    START_FROM_FIRST = auto()
    START_FROM_LAST = auto()
    START_FROM_SEQUENCE = auto()
    START_FROM_TIME = auto()
    START_FROM_TIME_DELTA = auto()


# Re-export ServerInfo from existing transport module
# The existing ServerInfo is a Pydantic model - we re-export it for consistency
from kubemq.transport.server_info import ServerInfo as ServerInfo  # noqa: E402


@runtime_checkable
class CredentialProvider(Protocol):
    """Pluggable sync credential provider for token-based authentication.

    Implementations supply tokens to the SDK. Used by sync clients
    (PubSubClient, CQClient, QueuesClient). For simple use cases like
    static tokens, file reads, or environment variables.

    Calls to get_token() are serialized by the SDK — at most one outstanding
    call at a time. Providers do NOT need internal synchronization.

    Error classification:
    - Raise KubeMQAuthenticationError for credential errors (non-retryable)
    - Raise any other exception for infrastructure errors (treated as transient)
    """

    def get_token(self) -> tuple[str, datetime | None]:
        """Acquire an authentication token (synchronous).

        Returns:
            A tuple of (token, expires_at) where:
            - token: The bearer token string
            - expires_at: Optional expiry hint. None means no expiry.
              When provided, the SDK schedules proactive refresh.
        """
        ...


@runtime_checkable
class AsyncCredentialProvider(Protocol):
    """Pluggable async credential provider for token-based authentication.

    Implementations supply tokens to the SDK. Used by async clients
    (AsyncPubSubClient, AsyncCQClient, AsyncQueuesClient). Suitable for
    async token sources (e.g., OIDC, Vault with async HTTP client).

    Calls to get_token() are serialized by the SDK — at most one outstanding
    call at a time. Providers do NOT need internal synchronization.

    The provider is invoked during both CONNECTING and RECONNECTING states,
    subject to the connection/reconnection timeout (not a separate timeout).

    Error classification:
    - Raise KubeMQAuthenticationError for credential errors (non-retryable)
    - Raise any other exception for infrastructure errors (treated as transient)
    """

    async def get_token(self) -> tuple[str, datetime | None]:
        """Acquire an authentication token (asynchronous).

        Returns:
            A tuple of (token, expires_at) where:
            - token: The bearer token string
            - expires_at: Optional expiry hint. None means no expiry.
              When provided, the SDK schedules proactive refresh.
        """
        ...


@runtime_checkable
class Logger(Protocol):
    """Structured logging interface for KubeMQ SDK.

    Uses ``warning`` (not ``warn``) per Python stdlib convention —
    ``logging.warn()`` has been deprecated since Python 3.3.

    Structured fields are passed as **kwargs::

        logger.info("connection established", client_id="abc", address="host:50000")
    """

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log a debug message."""
        ...

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log an informational message."""
        ...

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log a warning message."""
        ...

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log an error message."""
        ...


@runtime_checkable
class Closeable(Protocol):
    """Protocol for resources that can be closed synchronously."""

    def close(self) -> None:
        """Close the resource and release any held resources."""
        ...


@runtime_checkable
class AsyncCloseable(Protocol):
    """Protocol for resources that can be closed asynchronously."""

    async def close(self) -> None:
        """Close the resource and release any held resources."""
        ...


@runtime_checkable
class Pingable(Protocol):
    """Protocol for resources that support ping/health check."""

    def ping(self) -> ServerInfo:
        """Ping the server and return server information."""
        ...


@runtime_checkable
class AsyncPingable(Protocol):
    """Protocol for resources that support async ping/health check."""

    async def ping(self) -> ServerInfo:
        """Ping the server and return server information."""
        ...


@runtime_checkable
class TransportProtocol(Protocol):
    """Interface for transport layer operations.

    Both SyncTransport and AsyncTransport implement this protocol.
    Used for mock injection in unit tests and for decoupling the
    Public API layer from concrete transport implementations.

    Uses SDK-defined domain types (not gRPC protobuf types) at the
    interface boundary. Concrete transport implementations handle
    encode()/decode() conversion between domain types and protobuf
    internally.

    Defined in 07-code-quality-spec.md (primary owner).
    Referenced by 04-testing-spec.md (mock injection).
    """

    async def connect(self) -> None:
        """Establish connection to the KubeMQ server."""
        ...

    async def close(self) -> None:
        """Close the transport connection and release resources."""
        ...

    async def ping(self) -> ServerInfo:
        """Ping the server and return server information."""
        ...

    async def send_event(self, event: Any) -> Any:
        """Send a single event message."""
        ...

    async def send_event_store(self, event: Any) -> Any:
        """Send a single event store message."""
        ...

    async def subscribe(
        self,
        request: Any,
        cancel_token: Any = None,
    ) -> AsyncIterator[Any]:
        """Subscribe to messages on a channel.

        Note: request and return types vary by subscription type
        (events, events_store, commands, queries). ``Any`` is used
        here because a single Protocol cannot express union variants
        cleanly. Concrete implementations narrow the types.
        """
        ...

    async def send_command(self, request: Any) -> Any:
        """Send a command (RPC)."""
        ...

    async def send_query(self, request: Any) -> Any:
        """Send a query (RPC)."""
        ...

    async def send_response(self, response: Any) -> None:
        """Send a command/query response."""
        ...

    async def send_queue_message(self, message: Any) -> Any:
        """Send a single queue message."""
        ...

    async def send_queue_messages_batch(self, messages: Any) -> Any:
        """Send a batch of queue messages."""
        ...

    async def receive_queue_messages(self, request: Any) -> Any:
        """Receive queue messages (pull)."""
        ...

    async def create_channel(self, channel_type: str, channel: str) -> None:
        """Create a channel."""
        ...

    async def delete_channel(self, channel_type: str, channel: str) -> None:
        """Delete a channel."""
        ...

    async def list_channels(self, channel_type: str, search: str) -> list[Any]:
        """List channels."""
        ...


# Re-export BaseMessage for forward reference (actual implementation in messages.py)
if TYPE_CHECKING:
    from kubemq.core.messages import BaseMessage
