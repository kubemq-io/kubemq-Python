"""Unit tests for kubemq.queues.queues_poll_response module.

Tests for QueuesPollResponse class - decoding, operations, and utility methods.
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from kubemq.grpc import QueuesDownstreamRequestType
from kubemq.queues.queues_message_received import QueueMessageReceived
from kubemq.queues.queues_poll_response import QueuesPollResponse


class TestQueuesPollResponseCreation:
    """Tests for QueuesPollResponse creation and defaults."""

    def test_default_values(self):
        """Test default values are set correctly."""
        response = QueuesPollResponse()

        assert response.ref_request_id == ""
        assert response.transaction_id == ""
        assert response.messages == []
        assert response.error == ""
        assert response.is_error is False
        assert response.is_transaction_completed is False
        assert response.active_offsets == []
        assert response.receiver_client_id == ""
        assert response.visibility_seconds == 0
        assert response.is_auto_acked is False

    def test_custom_values(self):
        """Test custom values are set correctly."""
        messages = [
            QueueMessageReceived(id="msg-1", channel="queue"),
            QueueMessageReceived(id="msg-2", channel="queue"),
        ]

        response = QueuesPollResponse(
            ref_request_id="req-123",
            transaction_id="txn-456",
            messages=messages,
            error="",
            is_error=False,
            is_transaction_completed=False,
            active_offsets=[1, 2, 3],
            receiver_client_id="receiver",
            visibility_seconds=30,
            is_auto_acked=False,
        )

        assert response.ref_request_id == "req-123"
        assert response.transaction_id == "txn-456"
        assert len(response.messages) == 2
        assert response.active_offsets == [1, 2, 3]
        assert response.visibility_seconds == 30


class TestQueuesPollResponseValidation:
    """Tests for QueuesPollResponse validation."""

    def test_visibility_seconds_cannot_be_negative(self):
        """Test visibility_seconds cannot be negative."""
        with pytest.raises(ValueError, match="cannot be negative"):
            QueuesPollResponse(visibility_seconds=-1)

    def test_visibility_seconds_can_be_zero(self):
        """Test visibility_seconds can be zero."""
        response = QueuesPollResponse(visibility_seconds=0)
        assert response.visibility_seconds == 0

    def test_visibility_seconds_can_be_positive(self):
        """Test visibility_seconds can be positive."""
        response = QueuesPollResponse(visibility_seconds=60)
        assert response.visibility_seconds == 60


class TestQueuesPollResponseDecode:
    """Tests for QueuesPollResponse decoding from protobuf."""

    def test_decode_success(self):
        """Test decoding a successful response."""
        pb_response = MagicMock()
        pb_response.RefRequestId = "req-decode"
        pb_response.TransactionId = "txn-decode"
        pb_response.Error = ""
        pb_response.IsError = False
        pb_response.TransactionComplete = False
        pb_response.ActiveOffsets = [1, 2]
        pb_response.Messages = []

        response_handler = MagicMock()

        result = QueuesPollResponse.decode(
            response=pb_response,
            receiver_client_id="receiver-1",
            response_handler=response_handler,
            request_visibility_seconds=30,
            request_auto_ack=False,
        )

        assert result.ref_request_id == "req-decode"
        assert result.transaction_id == "txn-decode"
        assert result.is_error is False
        assert result.receiver_client_id == "receiver-1"
        assert result.visibility_seconds == 30

    def test_decode_error_response(self):
        """Test decoding an error response."""
        pb_response = MagicMock()
        pb_response.RefRequestId = "req-error"
        pb_response.TransactionId = ""
        pb_response.Error = "Queue not found"
        pb_response.IsError = True
        pb_response.TransactionComplete = True
        pb_response.ActiveOffsets = []
        pb_response.Messages = []

        response_handler = MagicMock()

        result = QueuesPollResponse.decode(
            response=pb_response,
            receiver_client_id="receiver",
            response_handler=response_handler,
        )

        assert result.is_error is True
        assert result.error == "Queue not found"

    def test_decode_with_messages(self):
        """Test decoding response with messages."""
        pb_message = MagicMock()
        pb_message.MessageID = "msg-decoded"
        pb_message.Channel = "decoded-queue"
        pb_message.Metadata = ""
        pb_message.Body = b"body"
        pb_message.ClientID = "sender"
        pb_message.Tags = {}
        pb_message.Attributes.Timestamp = int(datetime.now().timestamp() * 1e9)
        pb_message.Attributes.Sequence = 1
        pb_message.Attributes.ReceiveCount = 1
        pb_message.Attributes.ReRouted = False
        pb_message.Attributes.ReRoutedFromQueue = ""
        pb_message.Attributes.ExpirationAt = 0
        pb_message.Attributes.DelayedTo = 0

        pb_response = MagicMock()
        pb_response.RefRequestId = "req-msgs"
        pb_response.TransactionId = "txn-msgs"
        pb_response.Error = ""
        pb_response.IsError = False
        pb_response.TransactionComplete = False
        pb_response.ActiveOffsets = [1]
        pb_response.Messages = [pb_message]

        response_handler = MagicMock()

        result = QueuesPollResponse.decode(
            response=pb_response,
            receiver_client_id="receiver",
            response_handler=response_handler,
        )

        assert len(result.messages) == 1
        assert result.messages[0].id == "msg-decoded"

    def test_decode_none_raises_error(self):
        """Test that decoding None raises ValueError."""
        with pytest.raises(ValueError, match="Cannot decode None"):
            QueuesPollResponse.decode(
                response=None,
                receiver_client_id="receiver",
                response_handler=MagicMock(),
            )


class TestQueuesPollResponseOperations:
    """Tests for QueuesPollResponse operations (ack_all, reject_all, re_queue_all)."""

    def test_ack_all_calls_response_handler(self):
        """Test ack_all calls response handler with correct request type."""
        response_handler = MagicMock()

        response = QueuesPollResponse(
            transaction_id="txn-1",
            receiver_client_id="receiver",
            response_handler=response_handler,
            active_offsets=[1, 2, 3],
            is_auto_acked=False,
            is_transaction_completed=False,
        )

        response.ack_all()

        response_handler.assert_called_once()
        request = response_handler.call_args[0][0]
        assert request.RequestTypeData == QueuesDownstreamRequestType.AckAll

    def test_reject_all_calls_response_handler(self):
        """Test reject_all calls response handler with correct request type."""
        response_handler = MagicMock()

        response = QueuesPollResponse(
            transaction_id="txn-1",
            receiver_client_id="receiver",
            response_handler=response_handler,
            active_offsets=[1, 2],
            is_auto_acked=False,
            is_transaction_completed=False,
        )

        response.reject_all()

        response_handler.assert_called_once()
        request = response_handler.call_args[0][0]
        assert request.RequestTypeData == QueuesDownstreamRequestType.NAckAll

    def test_re_queue_all_calls_response_handler(self):
        """Test re_queue_all calls response handler with correct request type and channel."""
        response_handler = MagicMock()

        response = QueuesPollResponse(
            transaction_id="txn-1",
            receiver_client_id="receiver",
            response_handler=response_handler,
            active_offsets=[1],
            is_auto_acked=False,
            is_transaction_completed=False,
        )

        response.re_queue_all("new-queue")

        response_handler.assert_called_once()
        request = response_handler.call_args[0][0]
        assert request.RequestTypeData == QueuesDownstreamRequestType.ReQueueAll
        assert request.ReQueueChannel == "new-queue"

    def test_ack_all_raises_error_when_auto_acked(self):
        """Test ack_all raises error when auto-acknowledged."""
        response = QueuesPollResponse(
            transaction_id="txn-1",
            receiver_client_id="receiver",
            response_handler=MagicMock(),
            is_auto_acked=True,
        )

        with pytest.raises(ValueError, match="auto ack"):
            response.ack_all()

    def test_ack_all_raises_error_when_transaction_completed(self):
        """Test ack_all raises error when transaction is completed."""
        response = QueuesPollResponse(
            transaction_id="txn-1",
            receiver_client_id="receiver",
            response_handler=MagicMock(),
            is_transaction_completed=True,
        )

        with pytest.raises(ValueError, match="already completed"):
            response.ack_all()

    def test_ack_all_raises_error_when_no_response_handler(self):
        """Test ack_all raises error when response handler is not set."""
        response = QueuesPollResponse(
            transaction_id="txn-1",
            receiver_client_id="receiver",
            response_handler=None,
        )

        with pytest.raises(ValueError, match="not set"):
            response.ack_all()

    def test_operation_marks_transaction_completed(self):
        """Test that performing an operation marks transaction as completed."""
        response_handler = MagicMock()

        response = QueuesPollResponse(
            transaction_id="txn-1",
            receiver_client_id="receiver",
            response_handler=response_handler,
            active_offsets=[1],
            is_auto_acked=False,
            is_transaction_completed=False,
        )

        response.ack_all()

        assert response.is_transaction_completed is True

    def test_operation_marks_messages_completed(self):
        """Test that performing an operation marks all messages as completed."""
        response_handler = MagicMock()

        msg1 = QueueMessageReceived(
            id="msg-1",
            channel="queue",
            response_handler=response_handler,
        )
        msg2 = QueueMessageReceived(
            id="msg-2",
            channel="queue",
            response_handler=response_handler,
        )

        response = QueuesPollResponse(
            transaction_id="txn-1",
            receiver_client_id="receiver",
            response_handler=response_handler,
            messages=[msg1, msg2],
            active_offsets=[1, 2],
            is_auto_acked=False,
            is_transaction_completed=False,
        )

        response.ack_all()

        assert msg1.is_completed is True
        assert msg2.is_completed is True


class TestQueuesPollResponseUtility:
    """Tests for QueuesPollResponse utility methods."""

    def test_count_returns_message_count(self):
        """Test count returns correct message count."""
        messages = [
            QueueMessageReceived(id="msg-1", channel="queue"),
            QueueMessageReceived(id="msg-2", channel="queue"),
            QueueMessageReceived(id="msg-3", channel="queue"),
        ]

        response = QueuesPollResponse(messages=messages)

        assert response.count() == 3

    def test_count_returns_zero_for_empty(self):
        """Test count returns 0 for empty response."""
        response = QueuesPollResponse()

        assert response.count() == 0

    def test_is_empty_returns_true_when_no_messages(self):
        """Test is_empty returns True when no messages."""
        response = QueuesPollResponse()

        assert response.is_empty() is True

    def test_is_empty_returns_false_when_has_messages(self):
        """Test is_empty returns False when has messages."""
        response = QueuesPollResponse(messages=[QueueMessageReceived(id="msg-1", channel="queue")])

        assert response.is_empty() is False

    def test_with_updates_creates_new_instance(self):
        """Test with_updates creates a new response with updates."""
        original = QueuesPollResponse(
            ref_request_id="original-req",
            transaction_id="original-txn",
        )

        updated = original.with_updates(ref_request_id="updated-req")

        assert original.ref_request_id == "original-req"
        assert updated.ref_request_id == "updated-req"
        assert updated.transaction_id == "original-txn"


class TestQueuesPollResponseStr:
    """Tests for QueuesPollResponse string representations."""

    def test_string_representation(self):
        """Test string representation of poll response."""
        response = QueuesPollResponse(
            ref_request_id="req-str",
            transaction_id="txn-str",
            is_error=False,
        )

        str_repr = str(response)

        assert "req-str" in str_repr
        assert "txn-str" in str_repr
        assert "is_error=False" in str_repr

    def test_string_representation_with_error(self):
        """Test string representation with error."""
        response = QueuesPollResponse(
            ref_request_id="req-err",
            is_error=True,
            error="Test error message",
        )

        str_repr = str(response)

        assert "req-err" in str_repr
        assert "is_error=True" in str_repr
        assert "Test error message" in str_repr

    def test_repr_representation(self):
        """Test repr representation of poll response."""
        response = QueuesPollResponse(
            ref_request_id="req-repr",
            transaction_id="txn-repr",
        )

        repr_str = repr(response)

        assert "QueuesPollResponse(" in repr_str
        assert "req-repr" in repr_str
        assert "txn-repr" in repr_str
