"""Base message classes for KubeMQ Python SDK."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar, TypeVar
from uuid import uuid4

from kubemq.core.exceptions import KubeMQMessageError, KubeMQValidationError

if TYPE_CHECKING:
    pass

T = TypeVar("T", bound="BaseMessage")
R = TypeVar("R", bound="BaseResponse")


@dataclass
class BaseMessage(ABC):
    """Abstract base class for all KubeMQ messages.

    Provides common attributes and validation for messages across
    PubSub, Queues, and CQ domains.

    Attributes:
        id: Unique message identifier (auto-generated if not provided)
        channel: Target channel for the message
        metadata: Optional string metadata
        body: Message body as bytes
        tags: Key-value tags for the message

    Class Attributes:
        MAX_BODY_SIZE: Maximum allowed body size (can be overridden)
        REQUIRE_CONTENT: If True, require at least one of metadata/body/tags
            Set to False in subclasses to allow empty signaling messages
    """

    # Class variables for validation (can be overridden by subclasses)
    MAX_BODY_SIZE: ClassVar[int] = 100 * 1024 * 1024  # 100MB
    REQUIRE_CONTENT: ClassVar[bool] = True

    channel: str = ""
    metadata: str | None = None
    body: bytes = b""
    tags: dict[str, str] = field(default_factory=dict)
    id: str = field(default_factory=lambda: str(uuid4()))

    def __post_init__(self) -> None:
        """Validate message after initialization."""
        self.validate()

    def validate(self) -> None:
        """Validate the message fields.

        Override in subclasses to customize validation behavior.

        Raises:
            KubeMQValidationError: If validation fails
        """
        if not self.channel:
            raise KubeMQValidationError("channel is required")

        if self.REQUIRE_CONTENT and not self.metadata and not self.body and not self.tags:
            raise KubeMQValidationError(
                "message must have at least one of: metadata, body, or tags"
            )

        if len(self.body) > self.MAX_BODY_SIZE:
            raise KubeMQValidationError(
                f"body size ({len(self.body)}) exceeds maximum ({self.MAX_BODY_SIZE})"
            )

    @abstractmethod
    def encode(self, client_id: str) -> Any:
        """Encode the message to protobuf format for transmission.

        Args:
            client_id: The client ID to include in the message

        Returns:
            The protobuf message object
        """
        pass

    @classmethod
    @abstractmethod
    def decode(cls: type[T], pb_message: Any) -> T:
        """Decode a message from protobuf format.

        Args:
            pb_message: The protobuf message object

        Returns:
            A new instance of the message class
        """
        pass


@dataclass
class BaseResponse(ABC):
    """Abstract base class for operation responses.

    Provides common attributes for responses from KubeMQ operations.

    Attributes:
        id: Message/request identifier
        is_error: Whether the operation failed
        error: Error message if is_error is True
    """

    id: str = ""
    is_error: bool = False
    error: str = ""

    def raise_for_error(self) -> None:
        """Raise an exception if the response indicates an error.

        Raises:
            KubeMQMessageError: If is_error is True
        """
        if self.is_error:
            raise KubeMQMessageError(
                self.error or "Unknown error",
                details={"message_id": self.id},
            )

    @classmethod
    @abstractmethod
    def decode(cls: type[R], pb_response: Any) -> R:
        """Decode a response from protobuf format.

        Args:
            pb_response: The protobuf response object

        Returns:
            A new instance of the response class
        """
        pass


@dataclass
class BaseReceivedMessage(ABC):
    """Abstract base class for received messages.

    Provides common attributes for messages received from subscriptions
    or queue polling.

    Attributes:
        id: Message identifier
        channel: Channel the message was received from
        metadata: Message metadata
        body: Message body
        tags: Message tags
        from_client_id: ID of the client that sent the message
        timestamp: Unix timestamp when the message was sent
    """

    id: str = ""
    channel: str = ""
    metadata: str = ""
    body: bytes = b""
    tags: dict[str, str] = field(default_factory=dict)
    from_client_id: str = ""
    timestamp: int = 0

    @classmethod
    @abstractmethod
    def decode(cls: type[T], pb_message: Any) -> T:
        """Decode a received message from protobuf format.

        Args:
            pb_message: The protobuf message object

        Returns:
            A new instance of the received message class
        """
        pass
