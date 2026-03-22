"""Unit tests for timestamp conversions across queue message models.

Verifies that all timestamp fields (Timestamp, ExpirationAt, DelayedTo, SentAt)
are correctly converted between nanoseconds (server/protobuf) and Python datetime.

The server sends timestamps in nanoseconds since epoch. The SDK must divide by 1e9
(not 1e6) when decoding, and multiply by 1e9 when encoding.

Bug reference: ExpirationAt and DelayedTo were previously divided by 1e6 instead of 1e9,
causing ValueError("year XXXXX is out of range") for realistic timestamps.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta
from unittest.mock import MagicMock

from kubemq.queues.queues_message_received import QueueMessageReceived
from kubemq.queues.queues_messages_waiting_pulled import QueueMessageWaitingPulled
from kubemq.queues.queues_send_result import QueueSendResult

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now_ns() -> int:
    """Return current time as integer nanoseconds since epoch."""
    return int(time.time() * 1e9)


def _future_ns(seconds: int = 3600) -> int:
    """Return a future time as integer nanoseconds since epoch."""
    return int((time.time() + seconds) * 1e9)


def _past_ns(seconds: int = 3600) -> int:
    """Return a past time as integer nanoseconds since epoch."""
    return int((time.time() - seconds) * 1e9)


def _make_pb_queue_message(
    *,
    timestamp_ns: int = 0,
    expiration_at_ns: int = 0,
    delayed_to_ns: int = 0,
    channel: str = "test-queue",
) -> MagicMock:
    """Create a mock protobuf QueueMessage with nanosecond timestamps."""
    pb = MagicMock()
    pb.MessageID = "msg-ts-test"
    pb.Channel = channel
    pb.Metadata = ""
    pb.Body = b"test body"
    pb.ClientID = "test-client"
    pb.Tags = {}
    pb.Attributes.Timestamp = timestamp_ns
    pb.Attributes.Sequence = 1
    pb.Attributes.ReceiveCount = 0
    pb.Attributes.ReRouted = False
    pb.Attributes.ReRoutedFromQueue = ""
    pb.Attributes.ExpirationAt = expiration_at_ns
    pb.Attributes.DelayedTo = delayed_to_ns
    pb.Attributes.MD5OfBody = ""
    return pb


def _make_pb_send_result(
    *,
    sent_at_ns: int = 0,
    expiration_at_ns: int = 0,
    delayed_to_ns: int = 0,
) -> MagicMock:
    """Create a mock protobuf SendQueueMessageResult with nanosecond timestamps."""
    pb = MagicMock()
    pb.MessageID = "msg-send-ts"
    pb.SentAt = sent_at_ns
    pb.ExpirationAt = expiration_at_ns
    pb.DelayedTo = delayed_to_ns
    pb.IsError = False
    pb.Error = ""
    return pb


def _assert_datetime_close(dt: datetime, expected_epoch: float, tolerance_s: float = 2.0) -> None:
    """Assert that a datetime is within tolerance of an expected epoch time."""
    actual_epoch = dt.timestamp()
    diff = abs(actual_epoch - expected_epoch)
    assert diff < tolerance_s, (
        f"datetime {dt} (epoch={actual_epoch:.3f}) differs from "
        f"expected epoch={expected_epoch:.3f} by {diff:.3f}s (tolerance={tolerance_s}s)"
    )


# ===========================================================================
# QueueMessageReceived timestamp tests
# ===========================================================================


class TestQueueMessageReceivedTimestamps:
    """Verify QueueMessageReceived.decode() handles nanosecond timestamps correctly."""

    def test_timestamp_field_converts_nanoseconds(self):
        """Timestamp field should be decoded from nanoseconds."""
        now = time.time()
        now_ns = int(now * 1e9)
        pb = _make_pb_queue_message(timestamp_ns=now_ns)

        msg = QueueMessageReceived.decode(pb, "txn-1", False, "receiver")

        _assert_datetime_close(msg.timestamp, now)

    def test_expiration_at_converts_nanoseconds(self):
        """ExpirationAt field should be decoded from nanoseconds (was bug: used /1e6)."""
        future = time.time() + 3600
        future_ns = int(future * 1e9)
        pb = _make_pb_queue_message(expiration_at_ns=future_ns)

        msg = QueueMessageReceived.decode(pb, "txn-1", False, "receiver")

        _assert_datetime_close(msg.expired_at, future)

    def test_delayed_to_converts_nanoseconds(self):
        """DelayedTo field should be decoded from nanoseconds (was bug: used /1e6)."""
        future = time.time() + 7200
        future_ns = int(future * 1e9)
        pb = _make_pb_queue_message(delayed_to_ns=future_ns)

        msg = QueueMessageReceived.decode(pb, "txn-1", False, "receiver")

        _assert_datetime_close(msg.delayed_to, future)

    def test_large_nanosecond_value_does_not_overflow(self):
        """A realistic nanosecond timestamp should not cause year-out-of-range."""
        # This is the actual bug scenario: a nanosecond timestamp divided by 1e6
        # gives a value ~1000x too large, producing year ~58000.
        realistic_ns = int(time.time() * 1e9)
        pb = _make_pb_queue_message(
            timestamp_ns=realistic_ns,
            expiration_at_ns=realistic_ns + int(3600 * 1e9),
            delayed_to_ns=realistic_ns + int(7200 * 1e9),
        )

        # This would raise ValueError("year 58173 is out of range") with /1e6
        msg = QueueMessageReceived.decode(pb, "txn-1", False, "receiver")

        assert msg.timestamp.year <= 2100
        assert msg.expired_at.year <= 2100
        assert msg.delayed_to.year <= 2100

    def test_zero_timestamps_produce_epoch(self):
        """Zero nanoseconds should produce epoch datetime."""
        pb = _make_pb_queue_message(
            timestamp_ns=0,
            expiration_at_ns=0,
            delayed_to_ns=0,
        )
        # Set Attributes to mock that returns 0 for all timestamp fields
        pb.Attributes.Timestamp = 0
        pb.Attributes.ExpirationAt = 0
        pb.Attributes.DelayedTo = 0

        msg = QueueMessageReceived.decode(pb, "txn-1", False, "receiver")

        # timestamp=0 ns => epoch (or close to it since 0/1e9 = 0.0)
        assert msg.timestamp.year in (1969, 1970)  # depends on timezone
        assert msg.expired_at.year in (1969, 1970)
        assert msg.delayed_to.year in (1969, 1970)

    def test_no_attributes_returns_epoch(self):
        """When Attributes is None, timestamps should default to epoch."""
        pb = _make_pb_queue_message()
        pb.Attributes = None

        msg = QueueMessageReceived.decode(pb, "txn-1", False, "receiver")

        assert msg.timestamp == datetime.fromtimestamp(0)
        assert msg.expired_at == datetime.fromtimestamp(0)
        assert msg.delayed_to == datetime.fromtimestamp(0)


# ===========================================================================
# QueueMessageWaitingPulled timestamp tests
# ===========================================================================


class TestQueueMessageWaitingPulledTimestamps:
    """Verify QueueMessageWaitingPulled decode/encode handles nanoseconds correctly."""

    def test_decode_timestamp_from_nanoseconds(self):
        """Timestamp field should be decoded from nanoseconds."""
        now = time.time()
        now_ns = int(now * 1e9)
        pb = _make_pb_queue_message(timestamp_ns=now_ns)

        msg = QueueMessageWaitingPulled.decode(pb, "receiver")

        _assert_datetime_close(msg.timestamp, now)

    def test_decode_expiration_at_from_nanoseconds(self):
        """ExpirationAt should be decoded from nanoseconds (was bug: /1e6)."""
        future = time.time() + 3600
        future_ns = int(future * 1e9)
        pb = _make_pb_queue_message(expiration_at_ns=future_ns)

        msg = QueueMessageWaitingPulled.decode(pb, "receiver")

        _assert_datetime_close(msg.expired_at, future)

    def test_decode_delayed_to_from_nanoseconds(self):
        """DelayedTo should be decoded from nanoseconds (was bug: /1e6)."""
        future = time.time() + 7200
        future_ns = int(future * 1e9)
        pb = _make_pb_queue_message(delayed_to_ns=future_ns)

        msg = QueueMessageWaitingPulled.decode(pb, "receiver")

        _assert_datetime_close(msg.delayed_to, future)

    def test_decode_large_nanosecond_value_does_not_overflow(self):
        """Realistic nanosecond value should not cause year overflow."""
        realistic_ns = _now_ns()
        pb = _make_pb_queue_message(
            timestamp_ns=realistic_ns,
            expiration_at_ns=realistic_ns + int(3600 * 1e9),
            delayed_to_ns=realistic_ns + int(7200 * 1e9),
        )

        msg = QueueMessageWaitingPulled.decode(pb, "receiver")

        assert msg.timestamp.year <= 2100
        assert msg.expired_at.year <= 2100
        assert msg.delayed_to.year <= 2100

    def test_encode_timestamp_to_nanoseconds(self):
        """Encoding should produce nanosecond values for Timestamp."""
        ts = datetime(2025, 6, 15, 12, 0, 0)
        msg = QueueMessageWaitingPulled(
            id="msg-enc",
            channel="q",
            timestamp=ts,
            sequence=1,
            receiver_client_id="r",
        )

        pb = msg.encode()

        expected_ns = int(ts.timestamp() * 1e9)
        assert pb.Attributes.Timestamp == expected_ns

    def test_encode_expiration_at_to_nanoseconds(self):
        """Encoding should produce nanosecond values for ExpirationAt (was bug: *1e6)."""
        exp = datetime.now() + timedelta(hours=1)
        msg = QueueMessageWaitingPulled(
            id="msg-enc-exp",
            channel="q",
            expired_at=exp,
            receiver_client_id="r",
        )

        pb = msg.encode()

        expected_ns = int(exp.timestamp() * 1e9)
        assert pb.Attributes.ExpirationAt == expected_ns

    def test_encode_delayed_to_to_nanoseconds(self):
        """Encoding should produce nanosecond values for DelayedTo (was bug: *1e6)."""
        delay = datetime.now() + timedelta(hours=2)
        msg = QueueMessageWaitingPulled(
            id="msg-enc-delay",
            channel="q",
            delayed_to=delay,
            receiver_client_id="r",
        )

        pb = msg.encode()

        expected_ns = int(delay.timestamp() * 1e9)
        assert pb.Attributes.DelayedTo == expected_ns

    def test_roundtrip_preserves_timestamps(self):
        """Encode then decode should preserve timestamp values."""
        now = datetime.now()
        exp = now + timedelta(hours=1)
        delay = now + timedelta(hours=2)

        original = QueueMessageWaitingPulled(
            id="roundtrip",
            channel="q",
            body=b"data",
            from_client_id="sender",
            receiver_client_id="r",
            timestamp=now,
            expired_at=exp,
            delayed_to=delay,
            sequence=5,
        )

        pb = original.encode()
        decoded = QueueMessageWaitingPulled.decode(pb, "r")

        # Should be within 1 microsecond (nanosecond precision loss is acceptable)
        assert abs((decoded.timestamp - now).total_seconds()) < 0.001
        assert abs((decoded.expired_at - exp).total_seconds()) < 0.001
        assert abs((decoded.delayed_to - delay).total_seconds()) < 0.001


# ===========================================================================
# QueueSendResult timestamp tests
# ===========================================================================


class TestQueueSendResultTimestamps:
    """Verify QueueSendResult.decode() handles nanosecond timestamps correctly."""

    def test_sent_at_converts_nanoseconds(self):
        """SentAt should be decoded from nanoseconds."""
        now = time.time()
        now_ns = int(now * 1e9)
        pb = _make_pb_send_result(sent_at_ns=now_ns)

        result = QueueSendResult.decode(pb)

        assert result.sent_at is not None
        _assert_datetime_close(result.sent_at, now)

    def test_expiration_at_converts_nanoseconds(self):
        """ExpirationAt should be decoded from nanoseconds."""
        future = time.time() + 3600
        future_ns = int(future * 1e9)
        pb = _make_pb_send_result(expiration_at_ns=future_ns)

        result = QueueSendResult.decode(pb)

        assert result.expired_at is not None
        _assert_datetime_close(result.expired_at, future)

    def test_delayed_to_converts_nanoseconds(self):
        """DelayedTo should be decoded from nanoseconds."""
        future = time.time() + 7200
        future_ns = int(future * 1e9)
        pb = _make_pb_send_result(delayed_to_ns=future_ns)

        result = QueueSendResult.decode(pb)

        assert result.delayed_to is not None
        _assert_datetime_close(result.delayed_to, future)

    def test_zero_timestamps_produce_none(self):
        """Zero nanosecond values should produce None for optional fields."""
        pb = _make_pb_send_result(sent_at_ns=0, expiration_at_ns=0, delayed_to_ns=0)

        result = QueueSendResult.decode(pb)

        assert result.sent_at is None
        assert result.expired_at is None
        assert result.delayed_to is None

    def test_large_nanosecond_value_does_not_overflow(self):
        """Realistic nanosecond values should not cause year overflow."""
        now_ns = _now_ns()
        pb = _make_pb_send_result(
            sent_at_ns=now_ns,
            expiration_at_ns=now_ns + int(3600 * 1e9),
            delayed_to_ns=now_ns + int(7200 * 1e9),
        )

        result = QueueSendResult.decode(pb)

        assert result.sent_at is not None
        assert result.sent_at.year <= 2100
        assert result.expired_at is not None
        assert result.expired_at.year <= 2100
        assert result.delayed_to is not None
        assert result.delayed_to.year <= 2100


# ===========================================================================
# Consistency check: all models use /1e9, not /1e6
# ===========================================================================


class TestTimestampConsistency:
    """Verify all three models handle the same nanosecond input identically."""

    def test_same_nanosecond_produces_same_datetime_across_models(self):
        """Given the same nanosecond value, all models should produce the same datetime."""
        target_ns = int(datetime(2025, 7, 1, 12, 0, 0).timestamp() * 1e9)

        # QueueMessageReceived
        pb_recv = _make_pb_queue_message(
            timestamp_ns=target_ns,
            expiration_at_ns=target_ns,
            delayed_to_ns=target_ns,
        )
        msg_recv = QueueMessageReceived.decode(pb_recv, "txn-1", False, "r")

        # QueueMessageWaitingPulled
        pb_wait = _make_pb_queue_message(
            timestamp_ns=target_ns,
            expiration_at_ns=target_ns,
            delayed_to_ns=target_ns,
        )
        msg_wait = QueueMessageWaitingPulled.decode(pb_wait, "r")

        # QueueSendResult
        pb_send = _make_pb_send_result(
            sent_at_ns=target_ns,
            expiration_at_ns=target_ns,
            delayed_to_ns=target_ns,
        )
        result_send = QueueSendResult.decode(pb_send)

        # All should agree on the timestamp value
        expected = datetime.fromtimestamp(target_ns / 1e9)
        _assert_datetime_close(msg_recv.timestamp, expected.timestamp())
        _assert_datetime_close(msg_recv.expired_at, expected.timestamp())
        _assert_datetime_close(msg_recv.delayed_to, expected.timestamp())

        _assert_datetime_close(msg_wait.timestamp, expected.timestamp())
        _assert_datetime_close(msg_wait.expired_at, expected.timestamp())
        _assert_datetime_close(msg_wait.delayed_to, expected.timestamp())

        assert result_send.sent_at is not None
        _assert_datetime_close(result_send.sent_at, expected.timestamp())
        assert result_send.expired_at is not None
        _assert_datetime_close(result_send.expired_at, expected.timestamp())
        assert result_send.delayed_to is not None
        _assert_datetime_close(result_send.delayed_to, expected.timestamp())
