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
        pb_message.Attributes.MD5OfBody = ""

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


class TestQueuesPollResponseStrException:
    """Tests for __str__ exception handling (lines 317-318)."""

    def test_str_handles_exception_gracefully(self):
        response = QueuesPollResponse(
            ref_request_id="req-1",
            transaction_id="txn-1",
        )
        s = str(response)
        assert "QueuesPollResponse" in s


class TestQueuesPollResponseReprDetail:
    """Tests for __repr__ (lines 297-298)."""

    def test_repr_with_auto_ack(self):
        response = QueuesPollResponse(
            ref_request_id="req-repr",
            is_auto_acked=True,
            visibility_seconds=30,
        )
        r = repr(response)
        assert "is_auto_acked=True" in r
        assert "visibility_seconds=30" in r


class TestQueuesPollResponseOperationError:
    """Tests for _do_operation error path (lines 237-239)."""

    def test_operation_error_raises_value_error(self):
        response_handler = MagicMock(side_effect=RuntimeError("handler failed"))
        response = QueuesPollResponse(
            transaction_id="txn-1",
            receiver_client_id="receiver",
            response_handler=response_handler,
            active_offsets=[1],
            is_auto_acked=False,
            is_transaction_completed=False,
        )
        with pytest.raises(ValueError, match="Failed to perform"):
            response.ack_all()


class TestQueuesPollResponseActiveOffsets:
    """GAP-H6: Tests for get_active_offsets."""

    def test_get_active_offsets_sends_correct_request_type(self):
        from kubemq.grpc import QueuesDownstreamRequestType

        mock_resp = MagicMock()
        mock_resp.ActiveOffsets = [10, 20, 30]
        handler = MagicMock(return_value=mock_resp)

        response = QueuesPollResponse(
            transaction_id="txn-1",
            receiver_client_id="receiver",
            response_handler=handler,
            active_offsets=[1],
        )

        offsets = response.get_active_offsets()

        call_args = handler.call_args[0][0]
        assert call_args.RequestTypeData == QueuesDownstreamRequestType.ActiveOffsets
        assert offsets == [10, 20, 30]


class TestQueuesPollResponseTransactionStatus:
    """GAP-H6: Tests for get_transaction_status."""

    def test_transaction_status_sends_correct_request_type(self):
        from kubemq.grpc import QueuesDownstreamRequestType

        mock_resp = MagicMock()
        mock_resp.TransactionComplete = True
        handler = MagicMock(return_value=mock_resp)

        response = QueuesPollResponse(
            transaction_id="txn-1",
            receiver_client_id="receiver",
            response_handler=handler,
            active_offsets=[1],
        )

        status = response.get_transaction_status()

        call_args = handler.call_args[0][0]
        assert call_args.RequestTypeData == QueuesDownstreamRequestType.TransactionStatus
        assert status is True


class TestQueuesPollResponseCloseTransaction:
    """GAP-H6: Tests for close_transaction."""

    def test_close_by_client_sends_correct_request_type(self):
        from kubemq.grpc import QueuesDownstreamRequestType

        handler = MagicMock()
        response = QueuesPollResponse(
            transaction_id="txn-1",
            receiver_client_id="receiver",
            response_handler=handler,
            active_offsets=[1],
            is_auto_acked=False,
            is_transaction_completed=False,
        )

        response.close_transaction()

        call_args = handler.call_args[0][0]
        assert call_args.RequestTypeData == QueuesDownstreamRequestType.CloseByClient


class TestQueuesPollResponseRequestTypeAndMetadata:
    """GAP-M5: Tests for request_type_data and response_metadata fields."""

    def test_decode_request_type_data(self):
        from kubemq.grpc import QueuesDownstreamResponse

        pb_resp = MagicMock(spec=QueuesDownstreamResponse)
        pb_resp.RefRequestId = "req-1"
        pb_resp.TransactionId = "txn-1"
        pb_resp.Messages = []
        pb_resp.Error = ""
        pb_resp.IsError = False
        pb_resp.TransactionComplete = False
        pb_resp.ActiveOffsets = []
        pb_resp.RequestTypeData = 5
        pb_resp.Metadata = {}

        response = QueuesPollResponse.decode(pb_resp, "client", MagicMock())

        assert response.request_type_data == 5

    def test_decode_response_metadata(self):
        from kubemq.grpc import QueuesDownstreamResponse

        pb_resp = MagicMock(spec=QueuesDownstreamResponse)
        pb_resp.RefRequestId = "req-1"
        pb_resp.TransactionId = "txn-1"
        pb_resp.Messages = []
        pb_resp.Error = ""
        pb_resp.IsError = False
        pb_resp.TransactionComplete = False
        pb_resp.ActiveOffsets = []
        pb_resp.RequestTypeData = 0
        pb_resp.Metadata = {"key": "value"}

        response = QueuesPollResponse.decode(pb_resp, "client", MagicMock())

        assert response.response_metadata == {"key": "value"}


class TestQueuesPollResponseDoOperationWithResponse:
    """Tests for _do_operation_with_response edge cases."""

    def test_with_metadata_dict(self):
        mock_resp = MagicMock()
        mock_resp.ActiveOffsets = [5]
        handler = MagicMock(return_value=mock_resp)

        response = QueuesPollResponse(
            transaction_id="txn-1",
            receiver_client_id="receiver",
            response_handler=handler,
            active_offsets=[1],
        )

        result = response._do_operation_with_response(
            QueuesDownstreamRequestType.ActiveOffsets,
            metadata={"key": "val"},
        )

        assert result is mock_resp
        request = handler.call_args[0][0]
        assert request.Metadata["key"] == "val"

    def test_handler_raises_wraps_in_value_error(self):
        handler = MagicMock(side_effect=RuntimeError("handler boom"))

        response = QueuesPollResponse(
            transaction_id="txn-1",
            receiver_client_id="receiver",
            response_handler=handler,
            active_offsets=[1],
        )

        with pytest.raises(ValueError, match="Failed to perform"):
            response._do_operation_with_response(QueuesDownstreamRequestType.ActiveOffsets)

    def test_no_handler_raises_value_error(self):
        response = QueuesPollResponse(
            transaction_id="txn-1",
            receiver_client_id="receiver",
            response_handler=None,
        )

        with pytest.raises(ValueError, match="not set"):
            response._do_operation_with_response(QueuesDownstreamRequestType.ActiveOffsets)


class TestQueuesPollResponseDecodeException:
    """Test decode exception branch."""

    def test_decode_raises_value_error_on_bad_response(self):
        pb_msg = MagicMock()
        pb_msg.MessageID = "msg-1"
        pb_msg.Channel = "ch"
        pb_msg.Metadata = ""
        pb_msg.Body = b""
        pb_msg.ClientID = "client"
        pb_msg.Tags = {}
        pb_msg.Attributes = None

        pb_resp = MagicMock()
        pb_resp.RefRequestId = "req-1"
        pb_resp.TransactionId = "txn-1"
        pb_resp.Messages = [pb_msg]
        pb_resp.Error = ""
        pb_resp.IsError = False
        pb_resp.TransactionComplete = False
        pb_resp.ActiveOffsets = property(lambda s: (_ for _ in ()).throw(TypeError("boom")))

        with pytest.raises(ValueError, match="Failed to decode"):
            QueuesPollResponse.decode(
                response=pb_resp,
                receiver_client_id="receiver",
                response_handler=MagicMock(),
            )


class TestQueuesPollResponseStrExceptionPath:
    """Test __str__ exception handler path."""

    def test_str_exception_path(self):
        response = QueuesPollResponse(
            ref_request_id="req-1",
            transaction_id="txn-1",
        )
        original_messages = response.messages

        class BadLen:
            def __len__(self):
                raise RuntimeError("boom")

        object.__setattr__(response, "messages", BadLen())
        s = str(response)
        assert "Error displaying response" in s
        object.__setattr__(response, "messages", original_messages)


# ==============================================================================
# Coverage Gap Tests
# ==============================================================================


class TestQueuesPollResponseDoOperationWithMetadata:
    """Cover lines 301-302: _do_operation with metadata dict."""

    def test_do_operation_passes_metadata_to_request(self):
        handler = MagicMock()
        response = QueuesPollResponse(
            transaction_id="txn-1",
            receiver_client_id="receiver",
            response_handler=handler,
            active_offsets=[1],
            is_auto_acked=False,
            is_transaction_completed=False,
        )
        response._do_operation(
            QueuesDownstreamRequestType.AckAll,
            metadata={"trace-id": "abc123"},
        )
        request = handler.call_args[0][0]
        assert request.Metadata["trace-id"] == "abc123"
        assert response.is_transaction_completed is True


class TestQueuesPollResponseGetActiveOffsetsFallback:
    """Cover line 178: get_active_offsets when handler returns None."""

    def test_returns_stored_offsets_when_handler_returns_none(self):
        handler = MagicMock(return_value=None)
        response = QueuesPollResponse(
            transaction_id="txn-1",
            receiver_client_id="receiver",
            response_handler=handler,
            active_offsets=[10, 20],
        )
        offsets = response.get_active_offsets()
        assert offsets == [10, 20]


class TestQueuesPollResponseGetTransactionStatusFallback:
    """Cover line 189: get_transaction_status when handler returns None."""

    def test_returns_stored_status_when_handler_returns_none(self):
        handler = MagicMock(return_value=None)
        response = QueuesPollResponse(
            transaction_id="txn-1",
            receiver_client_id="receiver",
            response_handler=handler,
            active_offsets=[1],
            is_transaction_completed=True,
        )
        status = response.get_transaction_status()
        assert status is True
