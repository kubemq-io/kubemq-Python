"""Unit tests for kubemq.queues.queues_send_result module.

Tests for QueueSendResult class - decoding and utility methods.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest

from kubemq.queues.queues_send_result import QueueSendResult


class TestQueueSendResultDecode:
    """Tests for QueueSendResult decoding."""

    def test_decode_success(self):
        """Test decoding a successful send result."""
        pb_result = MagicMock()
        pb_result.MessageID = "msg-123"
        pb_result.SentAt = int(datetime.now().timestamp() * 1e9)
        pb_result.ExpirationAt = 0
        pb_result.DelayedTo = 0
        pb_result.IsError = False
        pb_result.Error = ""

        result = QueueSendResult.decode(pb_result)

        assert result.id == "msg-123"
        assert result.is_error is False
        assert result.error is None
        assert result.sent_at is not None

    def test_decode_error(self):
        """Test decoding an error result."""
        pb_result = MagicMock()
        pb_result.MessageID = ""
        pb_result.SentAt = 0
        pb_result.ExpirationAt = 0
        pb_result.DelayedTo = 0
        pb_result.IsError = True
        pb_result.Error = "Queue not found"

        result = QueueSendResult.decode(pb_result)

        assert result.is_error is True
        assert result.error == "Queue not found"
        assert result.id is None

    def test_decode_with_delayed_to(self):
        """Test decoding result with delayed delivery."""
        future_time = datetime.now() + timedelta(minutes=5)
        pb_result = MagicMock()
        pb_result.MessageID = "msg-delayed"
        pb_result.SentAt = int(datetime.now().timestamp() * 1e9)
        pb_result.ExpirationAt = 0
        pb_result.DelayedTo = int(future_time.timestamp() * 1e9)
        pb_result.IsError = False
        pb_result.Error = ""

        result = QueueSendResult.decode(pb_result)

        assert result.delayed_to is not None
        assert result.delayed_to > datetime.now()

    def test_decode_with_expiration(self):
        """Test decoding result with expiration time."""
        future_time = datetime.now() + timedelta(hours=1)
        pb_result = MagicMock()
        pb_result.MessageID = "msg-expires"
        pb_result.SentAt = int(datetime.now().timestamp() * 1e9)
        pb_result.ExpirationAt = int(future_time.timestamp() * 1e9)
        pb_result.DelayedTo = 0
        pb_result.IsError = False
        pb_result.Error = ""

        result = QueueSendResult.decode(pb_result)

        assert result.expired_at is not None
        assert result.has_expiration() is True

    def test_decode_none_raises_error(self):
        """Test that decoding None raises ValueError."""
        with pytest.raises(ValueError, match="Cannot decode None"):
            QueueSendResult.decode(None)  # type: ignore[arg-type]


class TestQueueSendResultProperties:
    """Tests for QueueSendResult property methods."""

    def test_is_successful_returns_true_for_success(self):
        """Test is_successful returns True for successful sends."""
        result = QueueSendResult(
            id="msg-success",
            is_error=False,
        )

        assert result.is_successful() is True

    def test_is_successful_returns_false_for_error(self):
        """Test is_successful returns False when there's an error."""
        result = QueueSendResult(
            id="msg-error",
            is_error=True,
            error="Some error",
        )

        assert result.is_successful() is False

    def test_is_successful_returns_false_when_no_id(self):
        """Test is_successful returns False when no ID."""
        result = QueueSendResult(
            id=None,
            is_error=False,
        )

        assert result.is_successful() is False

    def test_is_delayed_returns_true_for_future_delay(self):
        """Test is_delayed returns True for future delayed_to."""
        future_time = datetime.now() + timedelta(minutes=10)
        result = QueueSendResult(
            id="msg-delayed",
            delayed_to=future_time,
        )

        assert result.is_delayed() is True

    def test_is_delayed_returns_false_for_past_delay(self):
        """Test is_delayed returns False for past delayed_to."""
        past_time = datetime.now() - timedelta(minutes=10)
        result = QueueSendResult(
            id="msg-delivered",
            delayed_to=past_time,
        )

        assert result.is_delayed() is False

    def test_is_delayed_returns_false_when_no_delay(self):
        """Test is_delayed returns False when delayed_to is None."""
        result = QueueSendResult(
            id="msg-nodelay",
            delayed_to=None,
        )

        assert result.is_delayed() is False

    def test_has_expiration_returns_true(self):
        """Test has_expiration returns True when expired_at is set."""
        result = QueueSendResult(
            id="msg-expires",
            expired_at=datetime.now() + timedelta(hours=1),
        )

        assert result.has_expiration() is True

    def test_has_expiration_returns_false(self):
        """Test has_expiration returns False when expired_at is None."""
        result = QueueSendResult(
            id="msg-noexpire",
            expired_at=None,
        )

        assert result.has_expiration() is False

    def test_get_delay_seconds_returns_positive(self):
        """Test get_delay_seconds returns positive seconds."""
        future_time = datetime.now() + timedelta(seconds=30)
        result = QueueSendResult(
            id="msg-delay",
            delayed_to=future_time,
        )

        delay = result.get_delay_seconds()

        assert delay > 0
        assert delay <= 30

    def test_get_delay_seconds_returns_zero_when_not_delayed(self):
        """Test get_delay_seconds returns 0 when not delayed."""
        result = QueueSendResult(
            id="msg-nodelay",
            delayed_to=None,
        )

        assert result.get_delay_seconds() == 0


class TestQueueSendResultUtility:
    """Tests for QueueSendResult utility methods."""

    def test_with_updates_creates_new_instance(self):
        """Test with_updates creates a new result with updates."""
        original = QueueSendResult(id="original-id", is_error=False)

        updated = original.with_updates(id="updated-id")

        assert original.id == "original-id"
        assert updated.id == "updated-id"
        assert updated.is_error is False


class TestQueueSendResultStr:
    """Tests for QueueSendResult string representations."""

    def test_string_representation_success(self):
        """Test string representation for success."""
        result = QueueSendResult(
            id="msg-str",
            is_error=False,
            sent_at=datetime.now(),
        )

        str_repr = str(result)

        assert "SUCCESS" in str_repr
        assert "msg-str" in str_repr

    def test_string_representation_error(self):
        """Test string representation for error."""
        result = QueueSendResult(
            id=None,
            is_error=True,
            error="Test error",
        )

        str_repr = str(result)

        assert "ERROR" in str_repr
        assert "Test error" in str_repr

    def test_repr_representation(self):
        """Test repr representation."""
        result = QueueSendResult(id="msg-repr", is_error=False)

        repr_str = repr(result)

        assert "QueueSendResult(" in repr_str
        assert "msg-repr" in repr_str


class TestQueueSendResultDecodeException:
    """Tests for decode exception handling (lines 162-163)."""

    def test_decode_with_invalid_result_raises(self):
        pb_result = MagicMock()
        pb_result.MessageID = "msg-1"
        pb_result.SentAt = "not_a_number"  # Will cause exception
        pb_result.ExpirationAt = 0
        pb_result.DelayedTo = 0
        pb_result.IsError = False
        pb_result.Error = ""
        with pytest.raises(ValueError, match="Failed to decode"):
            QueueSendResult.decode(pb_result)


class TestQueueSendResultStrException:
    """Tests for __str__ and __repr__ exception handling (lines 182-183)."""

    def test_str_error_path(self):
        result = QueueSendResult(
            id="msg-err",
            is_error=True,
            error="Test error message",
        )
        s = str(result)
        assert "ERROR" in s
        assert "Test error message" in s

    def test_repr_with_all_fields(self):
        result = QueueSendResult(
            id="r1",
            sent_at=datetime.now(),
            expired_at=datetime.now() + timedelta(hours=1),
            delayed_to=datetime.now() + timedelta(minutes=5),
            is_error=False,
        )
        r = repr(result)
        assert "QueueSendResult(" in r
        assert "r1" in r


