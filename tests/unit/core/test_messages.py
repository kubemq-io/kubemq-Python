"""Tests for kubemq.core.messages module."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from kubemq.core.exceptions import KubeMQValidationError
from kubemq.core.messages import (
    BaseMessage,
    BaseReceivedMessage,
    BaseResponse,
)


@dataclass
class ConcreteMessage(BaseMessage):
    """Concrete implementation of BaseMessage for testing."""

    @classmethod
    def decode(cls, pb_message: Any) -> ConcreteMessage:
        return ConcreteMessage(
            channel=pb_message.get("channel", ""),
            body=pb_message.get("body", b""),
            metadata=pb_message.get("metadata"),
            tags=pb_message.get("tags", {}),
        )

    def encode(self, client_id: str) -> Any:
        return {
            "client_id": client_id,
            "channel": self.channel,
            "body": self.body,
            "metadata": self.metadata,
            "tags": self.tags,
            "id": self.id,
        }


@dataclass
class OptionalContentMessage(BaseMessage):
    """Message that doesn't require content."""

    REQUIRE_CONTENT = False

    @classmethod
    def decode(cls, pb_message: Any) -> OptionalContentMessage:
        return OptionalContentMessage(channel=pb_message.get("channel", ""))

    def encode(self, client_id: str) -> Any:
        return {"channel": self.channel}


@dataclass
class SmallBodyMessage(BaseMessage):
    """Message with small max body size."""

    MAX_BODY_SIZE = 10

    @classmethod
    def decode(cls, pb_message: Any) -> SmallBodyMessage:
        return SmallBodyMessage(
            channel=pb_message.get("channel", ""),
            body=pb_message.get("body", b""),
        )

    def encode(self, client_id: str) -> Any:
        return {"channel": self.channel, "body": self.body}


@dataclass
class ConcreteResponse(BaseResponse):
    """Concrete implementation of BaseResponse for testing."""

    @classmethod
    def decode(cls, pb_response: Any) -> ConcreteResponse:
        return ConcreteResponse(
            id=pb_response.get("id", ""),
            is_error=pb_response.get("is_error", False),
            error=pb_response.get("error", ""),
        )


@dataclass
class ConcreteReceivedMessage(BaseReceivedMessage):
    """Concrete implementation of BaseReceivedMessage for testing."""

    @classmethod
    def decode(cls, pb_message: Any) -> ConcreteReceivedMessage:
        return ConcreteReceivedMessage(
            id=pb_message.get("id", ""),
            channel=pb_message.get("channel", ""),
            body=pb_message.get("body", b""),
            metadata=pb_message.get("metadata", ""),
            tags=pb_message.get("tags", {}),
            from_client_id=pb_message.get("from_client_id", ""),
            timestamp=pb_message.get("timestamp", 0),
        )


class TestBaseMessage:
    """Tests for BaseMessage abstract class."""

    def test_concrete_implementation(self):
        """Test that concrete implementation works."""
        msg = ConcreteMessage(
            channel="test-channel",
            body=b"test body",
            metadata="test metadata",
            tags={"key": "value"},
        )

        assert msg.channel == "test-channel"
        assert msg.body == b"test body"
        assert msg.metadata == "test metadata"
        assert msg.tags == {"key": "value"}
        assert msg.id is not None  # Auto-generated

    def test_encode(self):
        """Test encoding a message."""
        msg = ConcreteMessage(
            channel="test-channel",
            body=b"test body",
        )

        encoded = msg.encode("client-123")

        assert encoded["client_id"] == "client-123"
        assert encoded["channel"] == "test-channel"
        assert encoded["body"] == b"test body"

    def test_validate_empty_channel(self):
        """Test validation fails on empty channel."""
        with pytest.raises(KubeMQValidationError, match="channel is required"):
            ConcreteMessage(channel="", body=b"test body")

    def test_validate_empty_content_when_required(self):
        """Test validation fails on empty content when required."""
        with pytest.raises(KubeMQValidationError, match="must have at least one"):
            ConcreteMessage(channel="test-channel")

    def test_validate_empty_body_when_not_required(self):
        """Test validation passes on empty body when not required."""
        # Should not raise
        msg = OptionalContentMessage(channel="test-channel")
        assert msg.body == b""
        assert msg.metadata is None
        assert msg.tags == {}

    def test_validate_body_too_large(self):
        """Test validation fails on oversized body."""
        with pytest.raises(KubeMQValidationError, match="body size"):
            SmallBodyMessage(
                channel="test-channel",
                body=b"this body is too large",
            )

    def test_default_max_body_size(self):
        """Test default MAX_BODY_SIZE value."""
        assert BaseMessage.MAX_BODY_SIZE == 100 * 1024 * 1024  # 100MB

    def test_default_require_content(self):
        """Test default REQUIRE_CONTENT value."""
        assert BaseMessage.REQUIRE_CONTENT is True

    def test_auto_generated_id(self):
        """Test that id is auto-generated."""
        msg = ConcreteMessage(channel="test", body=b"data")
        assert msg.id is not None
        assert len(msg.id) > 0


class TestBaseResponse:
    """Tests for BaseResponse abstract class."""

    def test_successful_response(self):
        """Test successful response properties."""
        resp = ConcreteResponse(
            id="req-123",
            is_error=False,
            error="",
        )

        assert resp.is_error is False
        assert resp.error == ""
        assert resp.id == "req-123"

    def test_error_response(self):
        """Test error response properties."""
        resp = ConcreteResponse(
            id="req-456",
            is_error=True,
            error="Something went wrong",
        )

        assert resp.is_error is True
        assert resp.error == "Something went wrong"

    def test_decode(self):
        """Test decoding a response."""
        raw = {
            "id": "req-789",
            "is_error": True,
            "error": "Decode error",
        }

        decoded = ConcreteResponse.decode(raw)

        assert decoded.is_error is True
        assert decoded.error == "Decode error"
        assert decoded.id == "req-789"

    def test_raise_for_error_when_error(self):
        """Test raise_for_error raises when is_error is True."""
        resp = ConcreteResponse(
            id="req-123",
            is_error=True,
            error="Test error message",
        )

        from kubemq.core.exceptions import KubeMQMessageError

        with pytest.raises(KubeMQMessageError, match="Test error message"):
            resp.raise_for_error()

    def test_raise_for_error_when_success(self):
        """Test raise_for_error does nothing when is_error is False."""
        resp = ConcreteResponse(
            id="req-123",
            is_error=False,
            error="",
        )

        # Should not raise
        resp.raise_for_error()


class TestBaseReceivedMessage:
    """Tests for BaseReceivedMessage abstract class."""

    def test_received_message_properties(self):
        """Test received message properties."""
        msg = ConcreteReceivedMessage(
            id="msg-123",
            channel="test-channel",
            body=b"received body",
            metadata="received metadata",
            tags={"received": "true"},
            from_client_id="sender-client",
            timestamp=1234567890,
        )

        assert msg.id == "msg-123"
        assert msg.channel == "test-channel"
        assert msg.body == b"received body"
        assert msg.metadata == "received metadata"
        assert msg.tags == {"received": "true"}
        assert msg.from_client_id == "sender-client"
        assert msg.timestamp == 1234567890

    def test_decode(self):
        """Test decoding a received message."""
        raw = {
            "id": "decoded-msg",
            "channel": "decoded-channel",
            "body": b"decoded body",
            "metadata": "decoded metadata",
            "tags": {"decoded": "yes"},
            "from_client_id": "other-client",
            "timestamp": 9876543210,
        }

        decoded = ConcreteReceivedMessage.decode(raw)

        assert decoded.id == "decoded-msg"
        assert decoded.channel == "decoded-channel"
        assert decoded.body == b"decoded body"
        assert decoded.metadata == "decoded metadata"
        assert decoded.tags == {"decoded": "yes"}
        assert decoded.from_client_id == "other-client"
        assert decoded.timestamp == 9876543210


class TestAbstractMethods:
    """Tests for abstract method enforcement."""

    def test_cannot_instantiate_base_message(self):
        """Test that BaseMessage cannot be instantiated directly."""
        with pytest.raises(TypeError):
            BaseMessage()

    def test_cannot_instantiate_base_response(self):
        """Test that BaseResponse cannot be instantiated directly."""
        with pytest.raises(TypeError):
            BaseResponse()

    def test_cannot_instantiate_base_received_message(self):
        """Test that BaseReceivedMessage cannot be instantiated directly."""
        with pytest.raises(TypeError):
            BaseReceivedMessage()


class TestMessageTags:
    """Tests for message tags functionality."""

    def test_empty_tags(self):
        """Test message with empty tags."""
        msg = ConcreteMessage(channel="test", body=b"body")
        assert msg.tags == {}

    def test_multiple_tags(self):
        """Test message with multiple tags."""
        tags = {
            "env": "production",
            "version": "1.0",
            "source": "api",
        }
        msg = ConcreteMessage(channel="test", body=b"body", tags=tags)

        assert msg.tags["env"] == "production"
        assert msg.tags["version"] == "1.0"
        assert msg.tags["source"] == "api"

    def test_tags_in_encode(self):
        """Test that tags are included in encode."""
        msg = ConcreteMessage(
            channel="test",
            body=b"body",
            tags={"key": "value"},
        )

        encoded = msg.encode("client")
        assert encoded["tags"] == {"key": "value"}
