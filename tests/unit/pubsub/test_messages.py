"""Unit tests for kubemq.pubsub message classes.

Tests for EventMessage, EventStoreMessage, EventMessageReceived, and EventStoreMessageReceived classes.
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from kubemq.pubsub.event_message import EventMessage
from kubemq.pubsub.event_message_received import EventMessageReceived
from kubemq.pubsub.event_store_message import EventStoreMessage
from kubemq.pubsub.event_store_message_received import EventStoreMessageReceived


class TestEventMessageValidation:
    """Tests for EventMessage validation."""

    def test_requires_channel(self):
        """Test that channel is required."""
        with pytest.raises(ValueError, match="must have a channel"):
            EventMessage(
                channel="",
                body=b"test",
            )

    def test_valid_with_body(self):
        """Test valid event with body."""
        event = EventMessage(
            channel="test-channel",
            body=b"test body",
        )

        assert event.channel == "test-channel"
        assert event.body == b"test body"

    def test_valid_with_metadata(self):
        """Test valid event with metadata only."""
        event = EventMessage(
            channel="test-channel",
            metadata="test metadata",
        )

        assert event.metadata == "test metadata"

    def test_valid_with_tags_and_body(self):
        """Test valid event with tags and body."""
        event = EventMessage(
            channel="test-channel",
            body=b"test",
            tags={"key": "value"},
        )

        assert event.tags == {"key": "value"}

    def test_generates_id_if_not_provided(self):
        """Test that ID is auto-generated if not provided."""
        event = EventMessage(
            channel="test-channel",
            body=b"test",
        )

        assert event.id is not None
        assert len(event.id) > 0

    def test_custom_id_is_preserved(self):
        """Test that custom ID is preserved."""
        event = EventMessage(
            id="custom-id",
            channel="test-channel",
            body=b"test",
        )

        assert event.id == "custom-id"


class TestEventMessageEncode:
    """Tests for EventMessage encoding."""

    def test_encode_basic(self):
        """Test basic encoding to protobuf."""
        event = EventMessage(
            id="evt-123",
            channel="encode-channel",
            metadata="encode metadata",
            body=b"encode body",
        )

        pb_event = event.encode("test-client")

        assert pb_event.EventID == "evt-123"
        assert pb_event.ClientID == "test-client"
        assert pb_event.Channel == "encode-channel"
        assert pb_event.Body == b"encode body"
        assert pb_event.Store is False  # EventMessage sets Store=False

    def test_encode_adds_client_id_tag(self):
        """Test encoding adds x-kubemq-client-id tag."""
        event = EventMessage(
            channel="test-channel",
            body=b"test",
        )

        pb_event = event.encode("tagged-client")

        assert pb_event.Tags["x-kubemq-client-id"] == "tagged-client"

    def test_encode_with_tags(self):
        """Test encoding preserves custom tags."""
        event = EventMessage(
            channel="test-channel",
            body=b"test",
            tags={"env": "prod"},
        )

        pb_event = event.encode("client-1")

        assert pb_event.Tags["env"] == "prod"


class TestEventStoreMessageValidation:
    """Tests for EventStoreMessage validation."""

    def test_requires_channel(self):
        """Test that channel is required."""
        with pytest.raises(ValueError, match="must have a channel"):
            EventStoreMessage(
                channel="",
                body=b"test",
            )

    def test_valid_with_body(self):
        """Test valid event store with body."""
        event = EventStoreMessage(
            channel="test-channel",
            body=b"test body",
        )

        assert event.channel == "test-channel"
        assert event.body == b"test body"

    def test_generates_id_if_not_provided(self):
        """Test that ID is auto-generated if not provided."""
        event = EventStoreMessage(
            channel="test-channel",
            body=b"test",
        )

        assert event.id is not None


class TestEventStoreMessageEncode:
    """Tests for EventStoreMessage encoding."""

    def test_encode_basic(self):
        """Test basic encoding to protobuf."""
        event = EventStoreMessage(
            id="evtstore-123",
            channel="encode-channel",
            body=b"encode body",
        )

        pb_event = event.encode("test-client")

        assert pb_event.EventID == "evtstore-123"
        assert pb_event.ClientID == "test-client"
        assert pb_event.Store is True  # EventStoreMessage sets Store=True

    def test_encode_adds_client_id_tag(self):
        """Test encoding adds x-kubemq-client-id tag."""
        event = EventStoreMessage(
            channel="test-channel",
            body=b"test",
        )

        pb_event = event.encode("tagged-client")

        assert pb_event.Tags["x-kubemq-client-id"] == "tagged-client"


class TestEventStoreMessageModelDump:
    """Tests for EventStoreMessage model_dump."""

    def test_model_dump_returns_body_as_bytes(self):
        """Test model_dump returns body as standard bytes (custom hex override removed)."""
        event = EventStoreMessage(
            channel="test-channel",
            body=b"\x00\x01\x02\x03",
        )

        dump = event.model_dump()

        assert dump["body"] == b"\x00\x01\x02\x03"


class TestEventMessageReceivedCreation:
    """Tests for EventMessageReceived creation."""

    def test_default_values(self):
        """Test default values are set correctly."""
        msg = EventMessageReceived()

        assert msg.id == ""
        assert msg.from_client_id == ""
        assert msg.channel == ""
        assert msg.metadata == ""
        assert msg.body == b""
        assert msg.tags == {}
        assert msg.timestamp is not None

    def test_custom_values(self):
        """Test custom values are set correctly."""
        msg = EventMessageReceived(
            id="rcv-123",
            from_client_id="sender-client",
            channel="rcv-channel",
            metadata="rcv metadata",
            body=b"rcv body",
            tags={"rcv": "tag"},
        )

        assert msg.id == "rcv-123"
        assert msg.from_client_id == "sender-client"
        assert msg.channel == "rcv-channel"


class TestEventMessageReceivedDecode:
    """Tests for EventMessageReceived decoding."""

    def test_decode_from_protobuf(self):
        """Test decoding from protobuf."""
        pb_event = MagicMock()
        pb_event.EventID = "decoded-evt"
        pb_event.Channel = "decoded-channel"
        pb_event.Metadata = "decoded metadata"
        pb_event.Body = b"decoded body"
        pb_event.Tags = {"x-kubemq-client-id": "sender-client", "custom": "tag"}

        msg = EventMessageReceived.decode(pb_event)

        assert msg.id == "decoded-evt"
        assert msg.from_client_id == "sender-client"
        assert msg.channel == "decoded-channel"
        assert msg.tags["custom"] == "tag"

    def test_decode_with_no_tags(self):
        """Test decoding when Tags is None."""
        pb_event = MagicMock()
        pb_event.EventID = "evt-no-tags"
        pb_event.Channel = "channel"
        pb_event.Metadata = ""
        pb_event.Body = b""
        pb_event.Tags = None

        msg = EventMessageReceived.decode(pb_event)

        assert msg.from_client_id == ""
        assert msg.tags == {}


class TestEventMessageReceivedModelDump:
    """Tests for EventMessageReceived model_dump."""

    def test_model_dump_converts_timestamp(self):
        """Test model_dump converts timestamp to ISO format."""
        timestamp = datetime(2024, 6, 15, 10, 30, 0)
        msg = EventMessageReceived(
            id="evt-dump",
            channel="channel",
            timestamp=timestamp,
        )

        dump = msg.model_dump()

        assert dump["timestamp"] == timestamp.isoformat()


class TestEventStoreMessageReceivedCreation:
    """Tests for EventStoreMessageReceived creation."""

    def test_default_values(self):
        """Test default values are set correctly."""
        msg = EventStoreMessageReceived()

        assert msg.id == ""
        assert msg.from_client_id == ""
        assert msg.channel == ""
        assert msg.metadata == ""
        assert msg.body == b""
        assert msg.sequence == 0
        assert msg.tags == {}

    def test_custom_values_with_sequence(self):
        """Test custom values with sequence are set correctly."""
        msg = EventStoreMessageReceived(
            id="store-rcv-123",
            channel="store-channel",
            sequence=42,
            body=b"store body",
        )

        assert msg.id == "store-rcv-123"
        assert msg.channel == "store-channel"
        assert msg.sequence == 42


class TestEventStoreMessageReceivedDecode:
    """Tests for EventStoreMessageReceived decoding."""

    def test_decode_from_protobuf(self):
        """Test decoding from protobuf."""
        pb_event = MagicMock()
        pb_event.EventID = "decoded-store"
        pb_event.Channel = "decoded-channel"
        pb_event.Metadata = "decoded metadata"
        pb_event.Body = b"decoded body"
        pb_event.Sequence = 100
        pb_event.Timestamp = int(datetime(2024, 6, 15).timestamp() * 1e9)
        pb_event.Tags = {"x-kubemq-client-id": "sender"}

        msg = EventStoreMessageReceived.decode(pb_event)

        assert msg.id == "decoded-store"
        assert msg.sequence == 100
        assert msg.from_client_id == "sender"

    def test_decode_with_no_tags(self):
        """Test decoding when Tags is None."""
        pb_event = MagicMock()
        pb_event.EventID = "evt-no-tags"
        pb_event.Channel = "channel"
        pb_event.Metadata = ""
        pb_event.Body = b""
        pb_event.Sequence = 1
        pb_event.Timestamp = 0
        pb_event.Tags = None

        msg = EventStoreMessageReceived.decode(pb_event)

        assert msg.from_client_id == ""
        assert msg.tags == {}


class TestEventStoreMessageReceivedModelDump:
    """Tests for EventStoreMessageReceived model_dump."""

    def test_model_dump_converts_body_to_hex(self):
        """Test model_dump converts body bytes to hex string."""
        msg = EventStoreMessageReceived(
            id="store-dump",
            channel="channel",
            body=b"\xde\xad\xbe\xef",
        )

        dump = msg.model_dump()

        assert dump["body"] == "deadbeef"

    def test_model_dump_converts_timestamp(self):
        """Test model_dump converts timestamp to ISO format."""
        timestamp = datetime(2024, 6, 15, 10, 30, 0)
        msg = EventStoreMessageReceived(
            id="store-dump",
            channel="channel",
            timestamp=timestamp,
        )

        dump = msg.model_dump()

        assert dump["timestamp"] == timestamp.isoformat()


class TestEventMessageAtLeastOneValidator:
    """Tests for at_least_one_must_exist validator edge cases."""

    def test_no_metadata_no_body_no_tags_raises(self):
        with pytest.raises(ValueError, match="at least one"):
            EventMessage(channel="ch", metadata=None, body=b"", tags={})

    def test_with_updates_creates_copy(self):
        msg = EventMessage(channel="ch", body=b"data")
        updated = msg.with_updates(body=b"new")
        assert updated.body == b"new"
        assert msg.body == b"data"


class TestEventMessageEncodeAllFields:
    """Test encode with all fields populated."""

    def test_encode_with_metadata_body_tags(self):
        event = EventMessage(
            id="full-evt",
            channel="ch",
            metadata="meta",
            body=b"body",
            tags={"a": "b"},
        )
        pb = event.encode("c1")
        assert pb.EventID == "full-evt"
        assert pb.Metadata == "meta"
        assert pb.Body == b"body"
        assert pb.Tags["a"] == "b"
        assert pb.Store is False


class TestEventStoreMessageAtLeastOneValidator:
    """Tests for at_least_one_must_exist validator on EventStoreMessage."""

    def test_no_metadata_no_body_no_tags_raises(self):
        with pytest.raises(ValueError, match="at least one"):
            EventStoreMessage(channel="ch", metadata=None, body=b"", tags={})

    def test_with_updates_creates_copy(self):
        msg = EventStoreMessage(channel="ch", body=b"data")
        updated = msg.with_updates(body=b"new")
        assert updated.body == b"new"
        assert msg.body == b"data"


class TestEventStoreMessageEncodeAllFields:
    """Test encode with all fields populated."""

    def test_encode_with_all_fields(self):
        event = EventStoreMessage(
            id="store-full",
            channel="ch",
            metadata="meta",
            body=b"body",
            tags={"x": "y"},
        )
        pb = event.encode("c1")
        assert pb.EventID == "store-full"
        assert pb.Metadata == "meta"
        assert pb.Body == b"body"
        assert pb.Tags["x"] == "y"
        assert pb.Store is True


class TestEventSendResultModelDumpJson:
    """Tests for EventSendResult.model_dump_json."""

    def test_model_dump_json_excludes_none(self):
        from kubemq.pubsub.event_send_result import EventSendResult
        result = EventSendResult(id="r1", sent=True, error=None)
        json_str = result.model_dump_json()
        assert "error" not in json_str
        assert "r1" in json_str

    def test_decode_from_result(self):
        from kubemq.pubsub.event_send_result import EventSendResult
        pb_result = MagicMock()
        pb_result.EventID = "decoded-r"
        pb_result.Sent = True
        pb_result.Error = "some error"
        result = EventSendResult.decode(pb_result)
        assert result.id == "decoded-r"
        assert result.sent is True
        assert result.error == "some error"
