"""Unit tests for kubemq.queues.queues_poll_request module.

Tests for QueuesPollRequest class - validation and encoding.
"""

from __future__ import annotations

import pytest

from kubemq.queues.queues_poll_request import QueuesPollRequest


class TestQueuesPollRequestValidation:
    """Tests for QueuesPollRequest validation."""

    def test_requires_channel(self):
        """Test that channel is required."""
        with pytest.raises(ValueError, match="must have a channel"):
            QueuesPollRequest(channel=None)

    def test_requires_channel_not_empty(self):
        """Test that channel cannot be empty."""
        with pytest.raises(ValueError, match="must have a channel"):
            QueuesPollRequest(channel="")

    def test_default_values(self):
        """Test default values are set correctly."""
        request = QueuesPollRequest(channel="test-queue")

        assert request.poll_max_messages == 1
        assert request.poll_wait_timeout_in_seconds == 60
        assert request.auto_ack_messages is False

    def test_custom_values(self):
        """Test custom values are set correctly."""
        request = QueuesPollRequest(
            channel="test-queue",
            poll_max_messages=10,
            poll_wait_timeout_in_seconds=30,
            auto_ack_messages=True,
        )

        assert request.channel == "test-queue"
        assert request.poll_max_messages == 10
        assert request.poll_wait_timeout_in_seconds == 30
        assert request.auto_ack_messages is True

    def test_poll_max_messages_must_be_positive(self):
        """Test poll_max_messages must be >= 1."""
        with pytest.raises(ValueError):
            QueuesPollRequest(channel="test-queue", poll_max_messages=0)  # type: ignore[arg-type]

    def test_poll_wait_timeout_must_be_positive(self):
        """Test poll_wait_timeout_in_seconds must be >= 1."""
        with pytest.raises(ValueError):
            QueuesPollRequest(channel="test-queue", poll_wait_timeout_in_seconds=0)  # type: ignore[arg-type]


class TestQueuesPollRequestEncode:
    """Tests for QueuesPollRequest encoding."""

    def test_encode_to_protobuf(self):
        """Test encoding request to protobuf."""
        request = QueuesPollRequest(
            channel="encode-queue",
            poll_max_messages=5,
            poll_wait_timeout_in_seconds=10,
        )

        pb_request = request.encode("test-client")

        assert pb_request.RequestID  # Should have a UUID
        assert pb_request.ClientID == "test-client"
        assert pb_request.Channel == "encode-queue"
        assert pb_request.MaxItems == 5
        assert pb_request.WaitTimeout == 10000  # Converted to ms

    def test_encode_with_auto_ack(self):
        """Test encoding request with auto_ack enabled."""
        request = QueuesPollRequest(
            channel="test-queue",
            auto_ack_messages=True,
        )

        pb_request = request.encode("client-1")

        assert pb_request.AutoAck is True

    def test_encode_without_auto_ack(self):
        """Test encoding request with auto_ack disabled."""
        request = QueuesPollRequest(
            channel="test-queue",
            auto_ack_messages=False,
        )

        pb_request = request.encode("client-1")

        assert pb_request.AutoAck is False


class TestQueuesPollRequestUtility:
    """Tests for QueuesPollRequest utility methods."""

    def test_with_updates_creates_new_instance(self):
        """Test with_updates creates a new request with updates."""
        original = QueuesPollRequest(
            channel="original-queue",
            poll_max_messages=5,
        )

        updated = original.with_updates(channel="updated-queue", poll_max_messages=10)

        assert original.channel == "original-queue"
        assert original.poll_max_messages == 5
        assert updated.channel == "updated-queue"
        assert updated.poll_max_messages == 10


class TestQueuesPollRequestStr:
    """Tests for QueuesPollRequest string representations."""

    def test_string_representation(self):
        """Test string representation of poll request."""
        request = QueuesPollRequest(
            channel="str-queue",
            poll_max_messages=3,
            poll_wait_timeout_in_seconds=15,
            auto_ack_messages=True,
        )

        str_repr = str(request)

        assert "str-queue" in str_repr
        assert "3" in str_repr
        assert "15" in str_repr
        assert "True" in str_repr

    def test_repr_representation(self):
        """Test repr representation of poll request."""
        request = QueuesPollRequest(channel="repr-queue")

        repr_str = repr(request)

        assert "QueuesPollRequest(" in repr_str
        assert "repr-queue" in repr_str


class TestQueuesPollRequestFrozenModel:
    """Test frozen model behavior."""

    def test_frozen_model_prevents_mutation(self):
        request = QueuesPollRequest(channel="q")
        with pytest.raises(Exception):
            request.channel = "changed"


class TestQueuesPollRequestUpperBounds:
    """GAP-M7: Upper bounds validation for poll request fields."""

    def test_max_messages_exceeds_1024_raises(self):
        with pytest.raises(ValueError):
            QueuesPollRequest(channel="q", poll_max_messages=1025)

    def test_wait_timeout_exceeds_3600_raises(self):
        with pytest.raises(ValueError):
            QueuesPollRequest(channel="q", poll_wait_timeout_in_seconds=3601)

    def test_max_messages_1024_passes(self):
        req = QueuesPollRequest(channel="q", poll_max_messages=1024)
        assert req.poll_max_messages == 1024

    def test_wait_timeout_3600_passes(self):
        req = QueuesPollRequest(channel="q", poll_wait_timeout_in_seconds=3600)
        assert req.poll_wait_timeout_in_seconds == 3600


class TestQueuesPollRequestChannelValidation:
    """GAP-H1/H2/H3: Channel validation for QueuesPollRequest."""

    def test_wildcard_star_raises(self):
        with pytest.raises(ValueError, match="wildcard"):
            QueuesPollRequest(channel="q.*")

    def test_whitespace_raises(self):
        with pytest.raises(ValueError, match="whitespace"):
            QueuesPollRequest(channel="q channel")

    def test_trailing_dot_raises(self):
        with pytest.raises(ValueError, match="end with"):
            QueuesPollRequest(channel="q.")


class TestQueuesPollRequestEncodeWithMetadata:
    """Test encode() with non-empty metadata dict."""

    def test_encode_metadata_set_on_protobuf(self):
        request = QueuesPollRequest(
            channel="test-queue",
            metadata={"trace-id": "abc123", "env": "test"},
        )

        pb_request = request.encode("client-1")

        assert pb_request.Metadata["trace-id"] == "abc123"
        assert pb_request.Metadata["env"] == "test"

    def test_encode_empty_metadata_no_keys(self):
        request = QueuesPollRequest(channel="test-queue")

        pb_request = request.encode("client-1")

        assert len(pb_request.Metadata) == 0
