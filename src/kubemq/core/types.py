"""Type definitions and protocols for KubeMQ Python SDK."""

from __future__ import annotations

from collections.abc import Awaitable
from enum import Enum, auto
from typing import (
    TYPE_CHECKING,
    Callable,
    Protocol,
    TypeVar,
    Union,
    runtime_checkable,
)

if TYPE_CHECKING:
    pass

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
from kubemq.transport.server_info import ServerInfo  # noqa: E402


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


# Re-export BaseMessage for forward reference (actual implementation in messages.py)
if TYPE_CHECKING:
    from kubemq.core.messages import BaseMessage
