"""Tests for QueueMessageWaitingPulled and collection classes."""

from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest

from kubemq.queues.queues_messages_waiting_pulled import (
    QueueMessagesPulled,
    QueueMessagesWaiting,
    QueueMessageWaitingPulled,
)


def _make_message(**overrides) -> QueueMessageWaitingPulled:
    defaults = dict(
        id="msg-1",
        channel="test-queue",
        metadata="meta",
        body=b"hello",
        from_client_id="sender",
        tags={"k": "v"},
        receiver_client_id="receiver",
    )
    defaults.update(overrides)
    return QueueMessageWaitingPulled(**defaults)


class TestQueueMessageWaitingPulledValidation:

    def test_channel_must_not_be_empty(self):
        with pytest.raises(ValueError, match="[Cc]hannel"):
            QueueMessageWaitingPulled(id="x", channel="", receiver_client_id="r")

    def test_valid_construction(self):
        msg = _make_message()
        assert msg.id == "msg-1"
        assert msg.channel == "test-queue"
        assert msg.body == b"hello"
        assert msg.tags == {"k": "v"}

    def test_frozen_model(self):
        msg = _make_message()
        with pytest.raises(Exception):
            msg.id = "changed"


class TestIsExpired:

    def test_not_expired_when_epoch(self):
        msg = _make_message()
        assert msg.is_expired() is False

    def test_expired_when_past(self):
        msg = _make_message(expired_at=datetime.now() - timedelta(hours=1))
        assert msg.is_expired() is True

    def test_not_expired_when_future(self):
        msg = _make_message(expired_at=datetime.now() + timedelta(hours=1))
        assert msg.is_expired() is False


class TestIsDelayed:

    def test_not_delayed_when_epoch(self):
        msg = _make_message()
        assert msg.is_delayed() is False

    def test_delayed_when_future(self):
        msg = _make_message(delayed_to=datetime.now() + timedelta(hours=1))
        assert msg.is_delayed() is True

    def test_not_delayed_when_past(self):
        msg = _make_message(delayed_to=datetime.now() - timedelta(hours=1))
        assert msg.is_delayed() is False


class TestGetDelaySeconds:

    def test_zero_when_not_delayed(self):
        msg = _make_message()
        assert msg.get_delay_seconds() == 0

    def test_positive_when_delayed(self):
        msg = _make_message(delayed_to=datetime.now() + timedelta(seconds=60))
        assert msg.get_delay_seconds() > 50


class TestGetAgeSeconds:

    def test_zero_when_epoch_timestamp(self):
        msg = _make_message()
        assert msg.get_age_seconds() == 0

    def test_positive_when_old(self):
        msg = _make_message(timestamp=datetime.now() - timedelta(seconds=30))
        assert msg.get_age_seconds() >= 29


class TestWithUpdates:

    def test_returns_new_instance(self):
        original = _make_message()
        updated = original.with_updates(metadata="new-meta")
        assert updated.metadata == "new-meta"
        assert original.metadata == "meta"
        assert updated is not original


class TestEncodeDecode:

    def test_encode_basic(self):
        msg = _make_message()
        pb = msg.encode()
        assert pb.MessageID == "msg-1"
        assert pb.Channel == "test-queue"
        assert pb.Metadata == "meta"
        assert pb.Body == b"hello"
        assert pb.ClientID == "sender"

    def test_decode_basic(self):
        pb = MagicMock()
        pb.MessageID = "msg-2"
        pb.Channel = "q1"
        pb.Metadata = "m"
        pb.Body = b"data"
        pb.ClientID = "c1"
        pb.Tags = {"tag1": "val1"}
        pb.Attributes = None

        msg = QueueMessageWaitingPulled.decode(pb, "receiver-1")

        assert msg.id == "msg-2"
        assert msg.channel == "q1"
        assert msg.body == b"data"
        assert msg.receiver_client_id == "receiver-1"
        assert msg.tags == {"tag1": "val1"}

    def test_decode_none_raises(self):
        with pytest.raises(ValueError, match="None"):
            QueueMessageWaitingPulled.decode(None, "r")

    def test_decode_empty_channel_raises(self):
        pb = MagicMock()
        pb.Channel = ""
        with pytest.raises(ValueError, match="channel"):

            QueueMessageWaitingPulled.decode(pb, "r")

    def test_decode_with_attributes(self):
        pb = MagicMock()
        pb.MessageID = "msg-3"
        pb.Channel = "q2"
        pb.Metadata = ""
        pb.Body = b""
        pb.ClientID = "c2"
        pb.Tags = {}
        pb.Attributes.Timestamp = int(datetime(2025, 1, 1).timestamp() * 1e9)
        pb.Attributes.Sequence = 42
        pb.Attributes.ReceiveCount = 3
        pb.Attributes.ReRouted = True
        pb.Attributes.ReRoutedFromQueue = "original-q"
        pb.Attributes.ExpirationAt = int(
            (datetime.now() + timedelta(hours=1)).timestamp() * 1e6
        )
        pb.Attributes.DelayedTo = 0

        msg = QueueMessageWaitingPulled.decode(pb, "r2")

        assert msg.sequence == 42
        assert msg.receive_count == 3
        assert msg.is_re_routed is True
        assert msg.re_route_from_queue == "original-q"

    def test_roundtrip_preserves_fields(self):
        original = _make_message(sequence=7, receive_count=2)
        pb = original.encode()

        decoded = QueueMessageWaitingPulled.decode(pb, "receiver")
        assert decoded.id == original.id
        assert decoded.channel == original.channel
        assert decoded.body == original.body


class TestQueueMessagesCollections:

    def test_waiting_empty(self):
        w = QueueMessagesWaiting(messages=[], is_error=False)
        assert w.is_empty() is True
        assert w.count() == 0

    def test_waiting_with_messages(self):
        m1 = _make_message(id="a")
        m2 = _make_message(id="b")
        w = QueueMessagesWaiting(messages=[m1, m2])
        assert w.count() == 2
        assert w.get_messages() == [m1, m2]

    def test_pulled_error(self):
        p = QueueMessagesPulled(messages=[], is_error=True, error="timeout")
        assert p.is_error is True
        assert p.error == "timeout"

    def test_str_representation(self):
        w = QueueMessagesWaiting(messages=[])
        assert "QueueMessagesWaiting" in str(w)

    def test_repr_representation(self):
        w = QueueMessagesWaiting(messages=[])
        assert "QueueMessagesWaiting(" in repr(w)

    def test_pulled_repr(self):
        p = QueueMessagesPulled(messages=[])
        assert "QueueMessagesPulled(" in repr(p)

    def test_with_updates_on_collection(self):
        w = QueueMessagesWaiting(messages=[], is_error=False)
        updated = w.with_updates(is_error=True, error="new error")
        assert updated.is_error is True
        assert updated.error == "new error"


class TestQueueMessageWaitingPulledEncodeEdge:
    """Tests for encode edge cases (lines 190, 198, 201)."""

    def test_encode_with_timestamp_and_sequence(self):
        from datetime import datetime, timedelta
        msg = _make_message(
            timestamp=datetime(2025, 1, 1, 12, 0, 0),
            sequence=7,
            receive_count=2,
        )
        pb = msg.encode()
        assert pb.Attributes.Sequence == 7
        assert pb.Attributes.ReceiveCount == 2

    def test_encode_with_expired_at(self):
        from datetime import datetime, timedelta
        exp = datetime.now() + timedelta(hours=1)
        msg = _make_message(expired_at=exp)
        pb = msg.encode()
        assert pb.Attributes.ExpirationAt > 0

    def test_encode_with_delayed_to(self):
        from datetime import datetime, timedelta
        delay = datetime.now() + timedelta(hours=1)
        msg = _make_message(delayed_to=delay)
        pb = msg.encode()
        assert pb.Attributes.DelayedTo > 0


class TestQueueMessageWaitingPulledStrRepr:
    """Tests for __str__ and __repr__ (lines 263-264, 273-286, 295)."""

    def test_str_basic(self):
        msg = _make_message()
        s = str(msg)
        assert "msg-1" in s
        assert "test-queue" in s

    def test_str_with_long_body(self):
        msg = _make_message(body=b"x" * 50)
        s = str(msg)
        assert "..." in s

    def test_repr_basic(self):
        msg = _make_message()
        r = repr(msg)
        assert "QueueMessageWaitingPulled(" in r
        assert "msg-1" in r


class TestQueueMessageWaitingPulledDecodeEdge:
    """Tests for decode edge cases (lines 263-264)."""

    def test_decode_with_delayed_to_attribute(self):
        from datetime import datetime, timedelta
        pb = MagicMock()
        pb.MessageID = "msg-delay"
        pb.Channel = "q"
        pb.Metadata = ""
        pb.Body = b""
        pb.ClientID = "c"
        pb.Tags = {}
        delay = datetime.now() + timedelta(hours=1)
        pb.Attributes.Timestamp = int(datetime(2025, 1, 1).timestamp() * 1e9)
        pb.Attributes.Sequence = 1
        pb.Attributes.ReceiveCount = 1
        pb.Attributes.ReRouted = False
        pb.Attributes.ReRoutedFromQueue = ""
        pb.Attributes.ExpirationAt = 0
        pb.Attributes.DelayedTo = int(delay.timestamp() * 1e6)
        msg = QueueMessageWaitingPulled.decode(pb, "r")
        assert msg.delayed_to > datetime.fromtimestamp(0)


class TestQueueMessagesBaseSimpleApiFields:
    """GAP-M6: Tests for MessagesReceived/MessagesExpired/IsPeak fields."""

    def test_waiting_messages_surfaces_fields(self):
        from kubemq.queues.queues_messages_waiting_pulled import QueueMessagesWaiting

        waiting = QueueMessagesWaiting(
            is_error=False,
            messages_received=10,
            messages_expired=2,
            is_peak=True,
        )
        assert waiting.messages_received == 10
        assert waiting.messages_expired == 2
        assert waiting.is_peak is True

    def test_pulled_messages_surfaces_fields(self):
        from kubemq.queues.queues_messages_waiting_pulled import QueueMessagesPulled

        pulled = QueueMessagesPulled(
            is_error=False,
            messages_received=5,
            messages_expired=1,
            is_peak=False,
        )
        assert pulled.messages_received == 5
        assert pulled.messages_expired == 1
        assert pulled.is_peak is False
