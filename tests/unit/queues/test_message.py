"""Unit tests for kubemq.queues.queues_message module.

Tests for QueueMessage class - validation, encoding, and decoding.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from kubemq.queues.queues_message import QueueMessage


class TestQueueMessageValidation:
    """Tests for QueueMessage validation."""

    def test_requires_channel(self):
        """Test that channel is required and cannot be empty."""
        with pytest.raises(ValueError, match="Channel cannot be empty"):
            QueueMessage(channel="", body=b"test")

    def test_requires_body_or_metadata_or_tags(self):
        """Test that message must have body, metadata, or tags."""
        with pytest.raises(ValueError, match="must have at least one"):
            QueueMessage(channel="test-queue", body=b"", metadata=None, tags={})

    def test_channel_must_not_be_empty(self):
        """Test Pydantic validation for empty channel."""
        with pytest.raises(ValueError):
            QueueMessage(channel="", body=b"data")

    def test_validates_delay_max(self):
        """Test that delay cannot exceed maximum."""
        with pytest.raises(ValueError, match="Delay cannot exceed"):
            QueueMessage(
                channel="test-queue",
                body=b"test",
                delay_in_seconds=90000,  # More than the 24-hour (86400s) max
            )

    def test_validates_expiration_max(self):
        """Test that expiration cannot exceed maximum."""
        with pytest.raises(ValueError, match="Expiration cannot exceed"):
            QueueMessage(
                channel="test-queue",
                body=b"test",
                expiration_in_seconds=90000,  # More than the 24-hour (86400s) max
            )

    def test_validates_max_receive_queue_configuration(self):
        """Test that dead letter queue requires both attempts and queue name."""
        with pytest.raises(ValueError, match="must also provide a max_receive_queue"):
            QueueMessage(
                channel="test-queue",
                body=b"test",
                max_receive_count=3,
                max_receive_queue="",  # Empty DLQ when attempts > 0
            )

    def test_valid_message_with_body(self):
        """Test that a message with only body is valid."""
        msg = QueueMessage(channel="test-queue", body=b"test body")
        assert msg.channel == "test-queue"
        assert msg.body == b"test body"

    def test_valid_message_with_metadata(self):
        """Test that a message with only metadata is valid."""
        msg = QueueMessage(channel="test-queue", metadata="test metadata")
        assert msg.metadata == "test metadata"

    def test_valid_message_with_tags(self):
        """Test that a message with only tags is valid."""
        msg = QueueMessage(channel="test-queue", tags={"key": "value"})
        assert msg.tags == {"key": "value"}


class TestQueueMessageEncode:
    """Tests for QueueMessage encoding."""

    def test_encode_with_all_fields(self):
        """Test encoding message with all fields set."""
        msg = QueueMessage(
            id="msg-123",
            channel="test-queue",
            metadata="test metadata",
            body=b"test body",
            tags={"key1": "value1", "key2": "value2"},
            delay_in_seconds=60,
            expiration_in_seconds=3600,
            max_receive_count=3,
            max_receive_queue="dlq-queue",
        )

        pb_request = msg.encode("test-client")

        assert pb_request.RequestID  # Should have a UUID
        assert len(pb_request.Messages) == 1

        pb_message = pb_request.Messages[0]
        assert pb_message.MessageID == "msg-123"
        assert pb_message.ClientID == "test-client"
        assert pb_message.Channel == "test-queue"
        assert pb_message.Metadata == "test metadata"
        assert pb_message.Body == b"test body"
        assert dict(pb_message.Tags) == {"key1": "value1", "key2": "value2"}

    def test_encode_message_with_policy(self):
        """Test encoding message with policy fields."""
        msg = QueueMessage(
            channel="test-queue",
            body=b"test body",
            delay_in_seconds=30,
            expiration_in_seconds=120,
            max_receive_count=5,
            max_receive_queue="my-dlq",
        )

        pb_message = msg.encode_message("client-1")

        assert pb_message.Policy.DelaySeconds == 30
        assert pb_message.Policy.ExpirationSeconds == 120
        assert pb_message.Policy.MaxReceiveCount == 5
        assert pb_message.Policy.MaxReceiveQueue == "my-dlq"

    def test_encode_generates_id_if_not_provided(self):
        """Test that encoding generates ID if not provided."""
        msg = QueueMessage(channel="test-queue", body=b"test")

        pb_message = msg.encode_message("client-1")

        assert pb_message.MessageID  # Should have a generated UUID

    def test_encode_with_tags(self):
        """Test encoding message with tags."""
        msg = QueueMessage(
            channel="test-queue",
            body=b"test",
            tags={"env": "production", "priority": "high"},
        )

        pb_message = msg.encode_message("client-1")

        assert "env" in pb_message.Tags
        assert pb_message.Tags["env"] == "production"
        assert pb_message.Tags["priority"] == "high"


class TestQueueMessageDecode:
    """Tests for QueueMessage decoding."""

    def test_decode_from_protobuf(self):
        """Test decoding message from protobuf."""
        pb_message = MagicMock()
        pb_message.MessageID = "msg-456"
        pb_message.Channel = "decoded-queue"
        pb_message.Metadata = "decoded metadata"
        pb_message.Body = b"decoded body"
        pb_message.Tags = {"decoded": "tag"}
        pb_message.Policy.DelaySeconds = 10
        pb_message.Policy.ExpirationSeconds = 60
        pb_message.Policy.MaxReceiveCount = 2
        pb_message.Policy.MaxReceiveQueue = "decoded-dlq"

        msg = QueueMessage.decode(pb_message)

        assert msg.id == "msg-456"
        assert msg.channel == "decoded-queue"
        assert msg.metadata == "decoded metadata"
        assert msg.body == b"decoded body"
        assert msg.tags == {"decoded": "tag"}
        assert msg.delay_in_seconds == 10
        assert msg.expiration_in_seconds == 60
        assert msg.max_receive_count == 2
        assert msg.max_receive_queue == "decoded-dlq"

    def test_decode_with_empty_metadata(self):
        """Test decoding message with empty metadata."""
        pb_message = MagicMock()
        pb_message.MessageID = "msg-789"
        pb_message.Channel = "test-queue"
        pb_message.Metadata = ""
        pb_message.Body = b"body"
        pb_message.Tags = {}
        pb_message.Policy.DelaySeconds = 0
        pb_message.Policy.ExpirationSeconds = 0
        pb_message.Policy.MaxReceiveCount = 0
        pb_message.Policy.MaxReceiveQueue = ""

        msg = QueueMessage.decode(pb_message)

        assert msg.metadata is None  # Empty string becomes None


class TestQueueMessageUtility:
    """Tests for QueueMessage utility methods."""

    def test_with_updates_creates_new_instance(self):
        """Test with_updates creates a new message with updates."""
        original = QueueMessage(channel="original-queue", body=b"original")

        updated = original.with_updates(channel="updated-queue")

        assert original.channel == "original-queue"
        assert updated.channel == "updated-queue"
        assert updated.body == b"original"

    def test_string_representation(self):
        """Test string representation of message."""
        msg = QueueMessage(
            id="msg-str",
            channel="str-queue",
            body=b"test body content",
            delay_in_seconds=5,
        )

        str_repr = str(msg)

        assert "msg-str" in str_repr
        assert "str-queue" in str_repr

    def test_string_representation_truncates_long_body(self):
        """Test that string representation truncates long body."""
        long_body = b"x" * 100
        msg = QueueMessage(channel="test-queue", body=long_body)

        str_repr = str(msg)

        assert "..." in str_repr

    def test_repr_representation(self):
        """Test repr representation of message."""
        msg = QueueMessage(
            id="msg-repr",
            channel="repr-queue",
            body=b"test",
        )

        repr_str = repr(msg)

        assert "QueueMessage(" in repr_str
        assert "msg-repr" in repr_str


class TestQueueMessageConstants:
    """Tests for QueueMessage class constants."""

    def test_max_delay_seconds_constant(self):
        """Test MAX_DELAY_SECONDS is 24 hours."""
        assert QueueMessage.MAX_DELAY_SECONDS == 86400

    def test_max_expiration_seconds_constant(self):
        """Test MAX_EXPIRATION_SECONDS is 24 hours."""
        assert QueueMessage.MAX_EXPIRATION_SECONDS == 86400


class TestQueueMessageChannelValidation:
    """GAP-H1/H2/H3: Channel validation for QueueMessage."""

    def test_wildcard_star_raises(self):
        with pytest.raises(ValueError, match="wildcard"):
            QueueMessage(channel="q.*", body=b"x")

    def test_wildcard_gt_raises(self):
        with pytest.raises(ValueError, match="wildcard"):
            QueueMessage(channel="q.>", body=b"x")

    def test_whitespace_raises(self):
        with pytest.raises(ValueError, match="whitespace"):
            QueueMessage(channel="q channel", body=b"x")

    def test_trailing_dot_raises(self):
        with pytest.raises(ValueError, match="end with"):
            QueueMessage(channel="q.", body=b"x")
