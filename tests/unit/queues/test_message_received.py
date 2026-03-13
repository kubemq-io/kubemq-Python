"""Unit tests for kubemq.queues.queues_message_received module.

Tests for QueueMessageReceived class - decoding, operations, and visibility.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest

from kubemq.grpc import QueuesDownstreamRequestType
from kubemq.queues.queues_message_received import QueueMessageReceived


class TestQueueMessageReceivedCreation:
    """Tests for QueueMessageReceived creation and defaults."""

    def test_default_values(self):
        """Test default values are set correctly."""
        msg = QueueMessageReceived()

        assert msg.id == ""
        assert msg.channel == ""
        assert msg.metadata == ""
        assert msg.body == b""
        assert msg.from_client_id == ""
        assert msg.tags == {}
        assert msg.sequence == 0
        assert msg.receive_count == 0
        assert msg.is_re_routed is False
        assert msg.re_route_from_queue == ""
        assert msg.transaction_id == ""
        assert msg.is_transaction_completed is False
        assert msg.receiver_client_id == ""
        assert msg.visibility_seconds == 0
        assert msg.is_auto_acked is False

    def test_custom_values(self):
        """Test custom values are set correctly."""
        msg = QueueMessageReceived(
            id="msg-123",
            channel="test-queue",
            metadata="test metadata",
            body=b"test body",
            from_client_id="sender-client",
            tags={"key": "value"},
            sequence=42,
            receive_count=3,
            is_re_routed=True,
            re_route_from_queue="original-queue",
            transaction_id="txn-456",
            is_transaction_completed=False,
            receiver_client_id="receiver-client",
            visibility_seconds=60,
            is_auto_acked=False,
        )

        assert msg.id == "msg-123"
        assert msg.channel == "test-queue"
        assert msg.metadata == "test metadata"
        assert msg.body == b"test body"
        assert msg.from_client_id == "sender-client"
        assert msg.tags == {"key": "value"}
        assert msg.sequence == 42
        assert msg.receive_count == 3
        assert msg.is_re_routed is True
        assert msg.re_route_from_queue == "original-queue"
        assert msg.transaction_id == "txn-456"
        assert msg.receiver_client_id == "receiver-client"
        assert msg.visibility_seconds == 60


class TestQueueMessageReceivedDecode:
    """Tests for QueueMessageReceived decoding from protobuf."""

    def test_decode_from_protobuf(self):
        """Test decoding message from protobuf."""
        pb_message = MagicMock()
        pb_message.MessageID = "decoded-msg-123"
        pb_message.Channel = "decoded-queue"
        pb_message.Metadata = "decoded metadata"
        pb_message.Body = b"decoded body"
        pb_message.ClientID = "sender-client"
        pb_message.Tags = {"tag1": "value1"}
        pb_message.Attributes.Timestamp = int(datetime.now().timestamp() * 1e9)
        pb_message.Attributes.Sequence = 10
        pb_message.Attributes.ReceiveCount = 2
        pb_message.Attributes.ReRouted = False
        pb_message.Attributes.ReRoutedFromQueue = ""
        pb_message.Attributes.ExpirationAt = 0
        pb_message.Attributes.DelayedTo = 0
        pb_message.Attributes.MD5OfBody = ""

        response_handler = MagicMock()

        msg = QueueMessageReceived.decode(
            message=pb_message,
            transaction_id="txn-decode",
            transaction_is_completed=False,
            receiver_client_id="receiver-1",
            response_handler=response_handler,
            visibility_seconds=0,
            is_auto_acked=False,
        )

        assert msg.id == "decoded-msg-123"
        assert msg.channel == "decoded-queue"
        assert msg.metadata == "decoded metadata"
        assert msg.body == b"decoded body"
        assert msg.from_client_id == "sender-client"
        assert msg.tags == {"tag1": "value1"}
        assert msg.sequence == 10
        assert msg.receive_count == 2
        assert msg.transaction_id == "txn-decode"
        assert msg.receiver_client_id == "receiver-1"

    def test_decode_with_no_attributes(self):
        """Test decoding message when Attributes is None."""
        pb_message = MagicMock()
        pb_message.MessageID = "msg-no-attrs"
        pb_message.Channel = "queue"
        pb_message.Metadata = ""
        pb_message.Body = b"body"
        pb_message.ClientID = "client"
        pb_message.Tags = {}
        pb_message.Attributes = None

        response_handler = MagicMock()

        msg = QueueMessageReceived.decode(
            message=pb_message,
            transaction_id="txn-1",
            transaction_is_completed=False,
            receiver_client_id="receiver",
            response_handler=response_handler,
            visibility_seconds=0,
            is_auto_acked=False,
        )

        assert msg.sequence == 0
        assert msg.receive_count == 0
        assert msg.is_re_routed is False

    def test_decode_with_visibility_starts_timer(self):
        """Test that decode with visibility_seconds > 0 starts timer."""
        pb_message = MagicMock()
        pb_message.MessageID = "msg-visibility"
        pb_message.Channel = "queue"
        pb_message.Metadata = ""
        pb_message.Body = b"body"
        pb_message.ClientID = "client"
        pb_message.Tags = {}
        pb_message.Attributes.Timestamp = int(datetime.now().timestamp() * 1e9)
        pb_message.Attributes.Sequence = 1
        pb_message.Attributes.ReceiveCount = 1
        pb_message.Attributes.ReRouted = False
        pb_message.Attributes.ReRoutedFromQueue = ""
        pb_message.Attributes.ExpirationAt = 0
        pb_message.Attributes.DelayedTo = 0
        pb_message.Attributes.MD5OfBody = ""

        response_handler = MagicMock()

        msg = QueueMessageReceived.decode(
            message=pb_message,
            transaction_id="txn-1",
            transaction_is_completed=False,
            receiver_client_id="receiver",
            response_handler=response_handler,
            visibility_seconds=60,
            is_auto_acked=False,
        )

        try:
            assert msg.visibility_seconds == 60
            assert msg._visibility_timer is not None
        finally:
            # Clean up timer
            if msg._visibility_timer:
                msg._visibility_timer.cancel()


class TestQueueMessageReceivedOperations:
    """Tests for QueueMessageReceived operations (ack, reject, re_queue)."""

    def test_ack_calls_response_handler(self):
        """Test ack calls response handler with correct request type."""
        response_handler = MagicMock()

        msg = QueueMessageReceived(
            id="msg-ack",
            channel="test-queue",
            transaction_id="txn-1",
            receiver_client_id="receiver",
            response_handler=response_handler,
            is_auto_acked=False,
            is_transaction_completed=False,
        )

        msg.ack()

        response_handler.assert_called_once()
        request = response_handler.call_args[0][0]
        assert request.RequestTypeData == QueuesDownstreamRequestType.AckRange

    def test_reject_calls_response_handler(self):
        """Test reject calls response handler with correct request type."""
        response_handler = MagicMock()

        msg = QueueMessageReceived(
            id="msg-reject",
            channel="test-queue",
            transaction_id="txn-1",
            receiver_client_id="receiver",
            response_handler=response_handler,
            is_auto_acked=False,
            is_transaction_completed=False,
        )

        msg.reject()

        response_handler.assert_called_once()
        request = response_handler.call_args[0][0]
        assert request.RequestTypeData == QueuesDownstreamRequestType.NAckRange

    def test_re_queue_calls_response_handler(self):
        """Test re_queue calls response handler with correct request type and channel."""
        response_handler = MagicMock()

        msg = QueueMessageReceived(
            id="msg-requeue",
            channel="test-queue",
            transaction_id="txn-1",
            receiver_client_id="receiver",
            response_handler=response_handler,
            is_auto_acked=False,
            is_transaction_completed=False,
        )

        msg.re_queue("new-queue")

        response_handler.assert_called_once()
        request = response_handler.call_args[0][0]
        assert request.RequestTypeData == QueuesDownstreamRequestType.ReQueueRange
        assert request.ReQueueChannel == "new-queue"

    def test_ack_raises_error_when_auto_acked(self):
        """Test ack raises error when message is auto-acknowledged."""
        msg = QueueMessageReceived(
            id="msg-auto",
            channel="test-queue",
            transaction_id="txn-1",
            receiver_client_id="receiver",
            response_handler=MagicMock(),
            is_auto_acked=True,
        )

        with pytest.raises(ValueError, match="auto ack"):
            msg.ack()

    def test_ack_raises_error_when_transaction_completed(self):
        """Test ack raises error when transaction is completed."""
        msg = QueueMessageReceived(
            id="msg-completed",
            channel="test-queue",
            transaction_id="txn-1",
            receiver_client_id="receiver",
            response_handler=MagicMock(),
            is_transaction_completed=True,
        )

        with pytest.raises(ValueError, match="already completed"):
            msg.ack()

    def test_ack_raises_error_when_no_response_handler(self):
        """Test ack raises error when response handler is not set."""
        msg = QueueMessageReceived(
            id="msg-no-handler",
            channel="test-queue",
            transaction_id="txn-1",
            receiver_client_id="receiver",
            response_handler=None,
        )

        with pytest.raises(ValueError, match="response_handler is not set"):
            msg.ack()

    def test_operation_marks_message_completed(self):
        """Test that performing an operation marks message as completed."""
        response_handler = MagicMock()

        msg = QueueMessageReceived(
            id="msg-complete",
            channel="test-queue",
            transaction_id="txn-1",
            receiver_client_id="receiver",
            response_handler=response_handler,
            is_auto_acked=False,
            is_transaction_completed=False,
        )

        msg.ack()

        assert msg.is_completed is True


class TestQueueMessageReceivedVisibility:
    """Tests for QueueMessageReceived visibility timer."""

    def test_extend_visibility_timer_raises_error_when_not_set(self):
        """Test extend_visibility_timer raises error when timer not set."""
        msg = QueueMessageReceived(
            id="msg-no-timer",
            channel="test-queue",
            visibility_seconds=0,
        )

        with pytest.raises(ValueError, match="visibility was not set"):
            msg.extend_visibility_timer(30)

    def test_extend_visibility_timer_raises_error_for_negative(self):
        """Test extend_visibility_timer raises error for non-positive seconds."""
        msg = QueueMessageReceived(
            id="msg-neg",
            channel="test-queue",
            visibility_seconds=60,
        )

        with pytest.raises(ValueError, match="must be greater than 0"):
            msg.extend_visibility_timer(0)

        with pytest.raises(ValueError, match="must be greater than 0"):
            msg.extend_visibility_timer(-10)

    def test_is_visible_returns_true_initially(self):
        """Test is_visible returns True when message is new."""
        msg = QueueMessageReceived(
            id="msg-visible",
            channel="test-queue",
        )

        assert msg.is_visible is True

    def test_is_visible_returns_false_after_completion(self):
        """Test is_visible returns False after message is completed."""
        response_handler = MagicMock()

        msg = QueueMessageReceived(
            id="msg-completed",
            channel="test-queue",
            transaction_id="txn-1",
            receiver_client_id="receiver",
            response_handler=response_handler,
        )

        msg.ack()

        assert msg.is_visible is False

    def test_get_remaining_visibility_seconds_returns_zero_without_timer(self):
        """Test get_remaining_visibility_seconds returns 0 without timer."""
        msg = QueueMessageReceived(
            id="msg-no-timer",
            channel="test-queue",
        )

        assert msg.get_remaining_visibility_seconds() == 0


class TestQueueMessageReceivedProperties:
    """Tests for QueueMessageReceived property methods."""

    def test_get_message_age(self):
        """Test get_message_age returns positive age."""
        past_time = datetime.now() - timedelta(seconds=30)
        msg = QueueMessageReceived(
            id="msg-age",
            channel="test-queue",
            timestamp=past_time,
        )

        age = msg.get_message_age()

        assert age >= 30
        assert age < 35  # Allow some tolerance

    def test_is_expired_returns_true_for_past_expiration(self):
        """Test is_expired returns True when expired_at is in the past."""
        past_time = datetime.now() - timedelta(minutes=5)
        msg = QueueMessageReceived(
            id="msg-expired",
            channel="test-queue",
            expired_at=past_time,
        )

        assert msg.is_expired() is True

    def test_is_expired_returns_false_for_future_expiration(self):
        """Test is_expired returns False when expired_at is in the future."""
        future_time = datetime.now() + timedelta(minutes=5)
        msg = QueueMessageReceived(
            id="msg-not-expired",
            channel="test-queue",
            expired_at=future_time,
        )

        assert msg.is_expired() is False

    def test_is_expired_returns_false_for_no_expiration(self):
        """Test is_expired returns False when expired_at is epoch."""
        msg = QueueMessageReceived(
            id="msg-no-expire",
            channel="test-queue",
        )

        assert msg.is_expired() is False


class TestQueueMessageReceivedStr:
    """Tests for QueueMessageReceived string representations."""

    def test_string_representation(self):
        """Test string representation of received message."""
        msg = QueueMessageReceived(
            id="msg-str",
            channel="str-queue",
            from_client_id="sender",
            sequence=5,
            receive_count=2,
        )

        str_repr = str(msg)

        assert "msg-str" in str_repr
        assert "str-queue" in str_repr
        assert "sender" in str_repr
        assert "5" in str_repr
        assert "2" in str_repr


class TestQueueMessageReceivedExtendVisibility:
    """Tests for extend_visibility_timer (lines 152-166)."""

    def test_extend_raises_when_timer_expired(self):
        msg = QueueMessageReceived(
            id="msg-exp",
            channel="q",
            response_handler=MagicMock(),
        )
        msg._timer_expired = True
        msg._visibility_timer = MagicMock()
        with pytest.raises(ValueError, match="expired"):
            msg.extend_visibility_timer(10)

    def test_extend_raises_when_completed(self):
        msg = QueueMessageReceived(
            id="msg-comp",
            channel="q",
            response_handler=MagicMock(),
        )
        msg._message_completed = True
        msg._visibility_timer = MagicMock()
        with pytest.raises(ValueError, match="already completed"):
            msg.extend_visibility_timer(10)

    def test_extend_recalculates_timer(self):
        import time
        msg = QueueMessageReceived(
            id="msg-ext",
            channel="q",
            response_handler=MagicMock(),
            visibility_seconds=60,
        )
        msg._start_visibility_timer()
        try:
            msg.extend_visibility_timer(30)
            assert msg._visibility_timer is not None
            assert msg._visibility_timer.is_alive()
        finally:
            if msg._visibility_timer:
                msg._visibility_timer.cancel()


class TestQueueMessageReceivedRemainingVisibility:
    """Tests for get_remaining_visibility_seconds (lines 207-209)."""

    def test_returns_zero_when_expired(self):
        msg = QueueMessageReceived(id="m1", channel="q")
        msg._timer_expired = True
        msg._visibility_timer = MagicMock()
        assert msg.get_remaining_visibility_seconds() == 0

    def test_returns_zero_when_completed(self):
        msg = QueueMessageReceived(id="m1", channel="q")
        msg._message_completed = True
        msg._visibility_timer = MagicMock()
        assert msg.get_remaining_visibility_seconds() == 0

    def test_returns_positive_when_active(self):
        import time
        msg = QueueMessageReceived(
            id="m1",
            channel="q",
            visibility_seconds=60,
        )
        msg._start_visibility_timer()
        try:
            remaining = msg.get_remaining_visibility_seconds()
            assert remaining > 50
        finally:
            if msg._visibility_timer:
                msg._visibility_timer.cancel()


class TestQueueMessageReceivedMarkTransaction:
    """Tests for _mark_transaction_completed (line 326)."""

    def test_mark_transaction_completed(self):
        msg = QueueMessageReceived(
            id="m1",
            channel="q",
        )
        msg._mark_transaction_completed()
        assert msg._message_completed is True
        assert msg.is_transaction_completed is True

    def test_mark_completed_cancels_timer(self):
        msg = QueueMessageReceived(
            id="m1",
            channel="q",
            visibility_seconds=60,
        )
        msg._start_visibility_timer()
        assert msg._visibility_timer is not None
        msg._mark_transaction_completed()
        assert msg._visibility_timer is None


class TestQueueMessageReceivedCancelVisibilityTimer:
    """Tests for _cancel_visibility_timer (lines 378-379)."""

    def test_cancel_timer_when_exists(self):
        msg = QueueMessageReceived(
            id="m1",
            channel="q",
            visibility_seconds=60,
        )
        msg._start_visibility_timer()
        assert msg._visibility_timer is not None
        msg._cancel_visibility_timer()
        assert msg._visibility_timer is None

    def test_cancel_timer_when_not_set(self):
        msg = QueueMessageReceived(id="m1", channel="q")
        msg._cancel_visibility_timer()


class TestQueueMessageReceivedOnVisibilityExpired:
    """Tests for _on_visibility_expired (lines 391-399)."""

    def test_on_visibility_expired_marks_expired_and_rejects(self):
        response_handler = MagicMock()
        msg = QueueMessageReceived(
            id="m1",
            channel="q",
            transaction_id="txn-1",
            receiver_client_id="receiver",
            response_handler=response_handler,
            is_auto_acked=False,
            is_transaction_completed=False,
        )
        msg._on_visibility_expired()
        assert msg._timer_expired is True
        response_handler.assert_called_once()

    def test_on_visibility_expired_logs_error_on_reject_failure(self):
        msg = QueueMessageReceived(
            id="m1",
            channel="q",
            response_handler=None,
        )
        msg._on_visibility_expired()
        assert msg._timer_expired is True


class TestQueueMessageReceivedDoOperationMessageCompleted:
    """Test _do_operation when _message_completed is True (line 326)."""

    def test_raises_when_message_already_completed(self):
        msg = QueueMessageReceived(
            id="m1",
            channel="q",
            transaction_id="txn-1",
            receiver_client_id="receiver",
            response_handler=MagicMock(),
            is_auto_acked=False,
            is_transaction_completed=False,
        )
        msg._message_completed = True
        with pytest.raises(ValueError, match="already completed"):
            msg.ack()


class TestQueueMessageReceivedMD5OfBody:
    """GAP-M3: Tests for MD5OfBody field."""

    def test_decode_md5_of_body(self):
        pb_message = MagicMock()
        pb_message.MessageID = "msg-md5"
        pb_message.Channel = "ch"
        pb_message.Metadata = ""
        pb_message.Body = b"hello"
        pb_message.ClientID = "client"
        pb_message.Tags = {}
        pb_message.Attributes.Timestamp = 0
        pb_message.Attributes.Sequence = 1
        pb_message.Attributes.ReceiveCount = 0
        pb_message.Attributes.ReRouted = False
        pb_message.Attributes.ReRoutedFromQueue = ""
        pb_message.Attributes.ExpirationAt = 0
        pb_message.Attributes.DelayedTo = 0
        pb_message.Attributes.MD5OfBody = "5d41402abc4b2a76b9719d911017c592"

        msg = QueueMessageReceived.decode(pb_message, "txn-1", False, "receiver", None)

        assert msg.md5_of_body == "5d41402abc4b2a76b9719d911017c592"
