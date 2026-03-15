"""Unit tests for kubemq.cq message classes.

Tests for CommandMessage, QueryMessage, CommandMessageReceived, QueryMessageReceived,
CommandResponseMessage, and QueryResponseMessage classes.
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from kubemq.cq.command_message import CommandMessage
from kubemq.cq.command_message_received import CommandMessageReceived
from kubemq.cq.command_response_message import CommandResponseMessage
from kubemq.cq.query_message import QueryMessage
from kubemq.cq.query_message_received import QueryMessageReceived
from kubemq.cq.query_response_message import QueryResponseMessage


class TestCommandMessageValidation:
    """Tests for CommandMessage validation."""

    def test_requires_channel(self):
        """Test that channel is required."""
        with pytest.raises(ValueError, match="must have a channel"):
            CommandMessage(
                channel="",
                body=b"test",
                timeout_in_seconds=10,
            )

    def test_requires_at_least_one_of_metadata_body_tags(self):
        """Test that at least one of metadata, body, or tags is required."""
        with pytest.raises(ValueError, match="must have at least one"):
            CommandMessage(
                channel="test-channel",
                timeout_in_seconds=10,
            )

    def test_timeout_must_be_positive(self):
        """Test that timeout must be greater than 0."""
        with pytest.raises(ValueError):
            CommandMessage(
                channel="test-channel",
                body=b"test",
                timeout_in_seconds=0,  # type: ignore[arg-type]
            )

    def test_valid_with_body(self):
        """Test valid command with body."""
        cmd = CommandMessage(
            channel="test-channel",
            body=b"test body",
            timeout_in_seconds=30,
        )

        assert cmd.channel == "test-channel"
        assert cmd.body == b"test body"
        assert cmd.timeout_in_seconds == 30

    def test_valid_with_metadata(self):
        """Test valid command with metadata only."""
        cmd = CommandMessage(
            channel="test-channel",
            metadata="test metadata",
            timeout_in_seconds=10,
        )

        assert cmd.metadata == "test metadata"

    def test_valid_with_tags(self):
        """Test valid command with tags only."""
        cmd = CommandMessage(
            channel="test-channel",
            tags={"key": "value"},
            timeout_in_seconds=10,
        )

        assert cmd.tags == {"key": "value"}

    def test_generates_id_if_not_provided(self):
        """Test that ID is auto-generated if not provided."""
        cmd = CommandMessage(
            channel="test-channel",
            body=b"test",
            timeout_in_seconds=10,
        )

        assert cmd.id is not None
        assert len(cmd.id) > 0


class TestCommandMessageEncode:
    """Tests for CommandMessage encoding."""

    def test_encode_basic(self):
        """Test basic encoding to protobuf."""
        cmd = CommandMessage(
            id="cmd-123",
            channel="encode-channel",
            body=b"encode body",
            timeout_in_seconds=15,
        )

        pb_cmd = cmd.encode("test-client")

        assert pb_cmd.RequestID == "cmd-123"
        assert pb_cmd.ClientID == "test-client"
        assert pb_cmd.Channel == "encode-channel"
        assert pb_cmd.Body == b"encode body"
        assert pb_cmd.Timeout == 15000  # Converted to ms

    def test_encode_with_tags(self):
        """Test encoding with tags."""
        cmd = CommandMessage(
            channel="test-channel",
            body=b"test",
            tags={"env": "prod", "priority": "high"},
            timeout_in_seconds=10,
        )

        pb_cmd = cmd.encode("client-1")

        assert pb_cmd.Tags["env"] == "prod"
        assert pb_cmd.Tags["priority"] == "high"


class TestCommandMessageStr:
    """Tests for CommandMessage string representations."""

    def test_repr_representation(self):
        """Test repr representation."""
        cmd = CommandMessage(
            id="cmd-repr",
            channel="repr-channel",
            body=b"repr body",
            timeout_in_seconds=20,
        )

        repr_str = repr(cmd)

        assert "CommandMessage" in repr_str
        assert "cmd-repr" in repr_str
        assert "repr-channel" in repr_str


class TestQueryMessageValidation:
    """Tests for QueryMessage validation."""

    def test_requires_channel(self):
        """Test that channel is required."""
        with pytest.raises(ValueError, match="must have a channel"):
            QueryMessage(
                channel="",
                body=b"test",
                timeout_in_seconds=10,
            )

    def test_requires_at_least_one_of_metadata_body_tags(self):
        """Test that at least one of metadata, body, or tags is required."""
        with pytest.raises(ValueError, match="must have at least one"):
            QueryMessage(
                channel="test-channel",
                timeout_in_seconds=10,
            )

    def test_timeout_must_be_positive(self):
        """Test that timeout must be greater than 0."""
        with pytest.raises(ValueError):
            QueryMessage(
                channel="test-channel",
                body=b"test",
                timeout_in_seconds=0,
            )

    def test_valid_with_all_fields(self):
        """Test valid query with all fields."""
        query = QueryMessage(
            id="qry-123",
            channel="test-channel",
            metadata="test metadata",
            body=b"test body",
            tags={"key": "value"},
            timeout_in_seconds=30,
            cache_key="cache-key",
            cache_ttl_int_seconds=60,
        )

        assert query.id == "qry-123"
        assert query.channel == "test-channel"
        assert query.cache_key == "cache-key"
        assert query.cache_ttl_int_seconds == 60


class TestQueryMessageEncode:
    """Tests for QueryMessage encoding."""

    def test_encode_basic(self):
        """Test basic encoding to protobuf."""
        query = QueryMessage(
            id="qry-encode",
            channel="encode-channel",
            body=b"encode body",
            timeout_in_seconds=20,
        )

        pb_query = query.encode("test-client")

        assert pb_query.RequestID == "qry-encode"
        assert pb_query.ClientID == "test-client"
        assert pb_query.Channel == "encode-channel"
        assert pb_query.Timeout == 20000  # Converted to ms

    def test_encode_with_cache(self):
        """Test encoding with cache parameters."""
        query = QueryMessage(
            channel="test-channel",
            body=b"test",
            timeout_in_seconds=10,
            cache_key="my-cache-key",
            cache_ttl_int_seconds=120,
        )

        pb_query = query.encode("client-1")

        assert pb_query.CacheKey == "my-cache-key"
        assert pb_query.CacheTTL == 120


class TestQueryMessageFactory:
    """Tests for QueryMessage factory method."""

    def test_create_valid(self):
        """Test create factory method creates valid query."""
        query = QueryMessage.create(
            channel="factory-channel",
            body=b"factory body",
            timeout_in_seconds=15,
            tags={},  # Required to avoid None being passed
        )

        assert query.channel == "factory-channel"
        assert query.body == b"factory body"


class TestQueryMessageStr:
    """Tests for QueryMessage string representations."""

    def test_repr_representation(self):
        """Test repr representation."""
        query = QueryMessage(
            id="qry-repr",
            channel="repr-channel",
            body=b"repr body",
            timeout_in_seconds=10,
        )

        repr_str = repr(query)

        assert "QueryMessage" in repr_str
        assert "qry-repr" in repr_str
        assert "repr-channel" in repr_str


class TestCommandMessageReceivedCreation:
    """Tests for CommandMessageReceived creation."""

    def test_default_values(self):
        """Test default values are set correctly."""
        msg = CommandMessageReceived()

        assert msg.id == ""
        assert msg.from_client_id == ""
        assert msg.channel == ""
        assert msg.metadata == ""
        assert msg.body == b""
        assert msg.reply_channel == ""
        assert msg.tags == {}
        assert msg.timestamp is not None

    def test_custom_values(self):
        """Test custom values are set correctly."""
        msg = CommandMessageReceived(
            id="rcv-123",
            from_client_id="sender-client",
            channel="rcv-channel",
            metadata="rcv metadata",
            body=b"rcv body",
            reply_channel="reply-channel",
            tags={"rcv": "tag"},
        )

        assert msg.id == "rcv-123"
        assert msg.from_client_id == "sender-client"
        assert msg.channel == "rcv-channel"
        assert msg.reply_channel == "reply-channel"


class TestCommandMessageReceivedDecode:
    """Tests for CommandMessageReceived decoding."""

    def test_decode_from_protobuf(self):
        """Test decoding from protobuf."""
        pb_request = MagicMock()
        pb_request.RequestID = "decoded-cmd"
        pb_request.ClientID = "decoded-client"
        pb_request.Channel = "decoded-channel"
        pb_request.Metadata = "decoded metadata"
        pb_request.Body = b"decoded body"
        pb_request.ReplyChannel = "decoded-reply"
        pb_request.Tags = {"decoded": "tag"}

        msg = CommandMessageReceived.decode(pb_request)

        assert msg.id == "decoded-cmd"
        assert msg.from_client_id == "decoded-client"
        assert msg.channel == "decoded-channel"
        assert msg.body == b"decoded body"
        assert msg.reply_channel == "decoded-reply"


class TestCommandMessageReceivedStr:
    """Tests for CommandMessageReceived string representations."""

    def test_repr_representation(self):
        """Test repr representation."""
        msg = CommandMessageReceived(
            id="rcv-repr",
            channel="repr-channel",
        )

        repr_str = repr(msg)

        assert "CommandMessageReceived" in repr_str
        assert "rcv-repr" in repr_str


class TestQueryMessageReceivedCreation:
    """Tests for QueryMessageReceived creation."""

    def test_default_values(self):
        """Test default values are set correctly."""
        msg = QueryMessageReceived()

        assert msg.id == ""
        assert msg.from_client_id == ""
        assert msg.channel == ""
        assert msg.metadata == ""
        assert msg.body == b""
        assert msg.reply_channel == ""
        assert msg.tags == {}


class TestQueryMessageReceivedDecode:
    """Tests for QueryMessageReceived decoding."""

    def test_decode_from_protobuf(self):
        """Test decoding from protobuf."""
        pb_request = MagicMock()
        pb_request.RequestID = "decoded-qry"
        pb_request.ClientID = "decoded-client"
        pb_request.Channel = "decoded-channel"
        pb_request.Metadata = "decoded metadata"
        pb_request.Body = b"decoded body"
        pb_request.ReplyChannel = "decoded-reply"
        pb_request.Tags = {"decoded": "tag"}

        msg = QueryMessageReceived.decode(pb_request)

        assert msg.id == "decoded-qry"
        assert msg.from_client_id == "decoded-client"
        assert msg.channel == "decoded-channel"


class TestQueryMessageReceivedStr:
    """Tests for QueryMessageReceived string representations."""

    def test_repr_representation(self):
        """Test repr representation."""
        msg = QueryMessageReceived(
            id="qry-rcv-repr",
            channel="repr-channel",
        )

        repr_str = repr(msg)

        assert "QueryMessageReceived" in repr_str
        assert "qry-rcv-repr" in repr_str


class TestCommandResponseMessageValidation:
    """Tests for CommandResponseMessage validation."""

    def test_requires_command_received(self):
        """Test that command_received is required."""
        with pytest.raises(ValueError, match="must have a command request"):
            CommandResponseMessage(
                command_received=None,
                is_executed=True,
            )

    def test_requires_reply_channel(self):
        """Test that command_received must have reply_channel."""
        cmd_rcv = CommandMessageReceived(
            id="cmd-1",
            channel="test",
            reply_channel="",  # Empty reply channel
        )

        with pytest.raises(ValueError, match="must have a reply channel"):
            CommandResponseMessage(
                command_received=cmd_rcv,
                is_executed=True,
            )

    def test_valid_response(self):
        """Test valid command response creation."""
        cmd_rcv = CommandMessageReceived(
            id="cmd-1",
            channel="test",
            reply_channel="reply-channel",
        )

        response = CommandResponseMessage(
            command_received=cmd_rcv,
            is_executed=True,
        )

        assert response.is_executed is True


class TestCommandResponseMessageEncode:
    """Tests for CommandResponseMessage encoding."""

    def test_encode(self):
        """Test encoding to protobuf."""
        cmd_rcv = CommandMessageReceived(
            id="cmd-encode",
            channel="test",
            reply_channel="reply-channel",
        )

        response = CommandResponseMessage(
            command_received=cmd_rcv,
            is_executed=True,
            error="",
        )

        pb_response = response.encode("test-client")

        assert pb_response.ClientID == "test-client"
        assert pb_response.RequestID == "cmd-encode"
        assert pb_response.ReplyChannel == "reply-channel"
        assert pb_response.Executed is True

    def test_encode_raises_without_command_received(self):
        """Test encode raises error without command_received."""
        # Create response directly without validation
        response = CommandResponseMessage.__new__(CommandResponseMessage)
        object.__setattr__(response, "command_received", None)
        object.__setattr__(response, "is_executed", True)
        object.__setattr__(response, "error", "")
        object.__setattr__(response, "timestamp", datetime.now())

        with pytest.raises(ValueError, match="required for encoding"):
            response.encode("client")


class TestCommandResponseMessageDecode:
    """Tests for CommandResponseMessage decoding."""

    def test_decode_from_protobuf(self):
        """Test decoding from protobuf."""
        pb_response = MagicMock()
        pb_response.ClientID = "decoded-client"
        pb_response.RequestID = "decoded-req"
        pb_response.Executed = True
        pb_response.Error = ""
        pb_response.Timestamp = int(datetime.now().timestamp() * 1e9)
        pb_response.Metadata = ""
        pb_response.Body = b""
        pb_response.Tags = {}

        response = CommandResponseMessage.decode(pb_response)

        assert response.client_id == "decoded-client"
        assert response.request_id == "decoded-req"
        assert response.is_executed is True

    def test_decode_with_metadata_body_tags(self):
        """GAP-H5: Decode should surface metadata, body, tags."""
        pb_response = MagicMock()
        pb_response.ClientID = "client"
        pb_response.RequestID = "req-1"
        pb_response.Executed = True
        pb_response.Error = ""
        pb_response.Timestamp = int(datetime.now().timestamp() * 1e9)
        pb_response.Metadata = "response-meta"
        pb_response.Body = b"response-body"
        pb_response.Tags = {"k": "v"}

        response = CommandResponseMessage.decode(pb_response)

        assert response.metadata == "response-meta"
        assert response.body == b"response-body"
        assert response.tags == {"k": "v"}


class TestCommandResponseMessageEncodeWithFields:
    """GAP-H4: Tests for CommandResponseMessage encode with Metadata/Body/Tags."""

    def test_encode_with_metadata_body_tags(self):
        cmd_rcv = CommandMessageReceived(
            id="cmd-1",
            channel="test",
            reply_channel="reply",
        )
        response = CommandResponseMessage(
            command_received=cmd_rcv,
            is_executed=True,
            metadata="meta",
            body=b"body",
            tags={"k": "v"},
        )
        pb = response.encode("client")
        assert pb.Metadata == "meta"
        assert pb.Body == b"body"
        assert pb.Tags["k"] == "v"

    def test_encode_without_optional_fields(self):
        cmd_rcv = CommandMessageReceived(
            id="cmd-2",
            channel="test",
            reply_channel="reply",
        )
        response = CommandResponseMessage(
            command_received=cmd_rcv,
            is_executed=True,
        )
        pb = response.encode("client")
        assert pb.Metadata == ""
        assert pb.Body == b""


class TestCommandResponseMessageStr:
    """Tests for CommandResponseMessage string representations."""

    def test_repr_representation(self):
        """Test repr representation."""
        cmd_rcv = CommandMessageReceived(
            id="cmd-repr",
            reply_channel="reply",
        )

        response = CommandResponseMessage(
            command_received=cmd_rcv,
            is_executed=True,
        )

        repr_str = repr(response)

        assert "CommandResponseMessage" in repr_str


class TestQueryResponseMessageValidation:
    """Tests for QueryResponseMessage validation."""

    def test_requires_query_received(self):
        """Test that query_received is required."""
        with pytest.raises(ValueError, match="must have a query request"):
            QueryResponseMessage(
                query_received=None,
                is_executed=True,
            )

    def test_requires_reply_channel(self):
        """Test that query_received must have reply_channel."""
        qry_rcv = QueryMessageReceived(
            id="qry-1",
            channel="test",
            reply_channel="",  # Empty reply channel
        )

        with pytest.raises(ValueError, match="must have a reply channel"):
            QueryResponseMessage(
                query_received=qry_rcv,
                is_executed=True,
            )

    def test_valid_response(self):
        """Test valid query response creation."""
        qry_rcv = QueryMessageReceived(
            id="qry-1",
            channel="test",
            reply_channel="reply-channel",
        )

        response = QueryResponseMessage(
            query_received=qry_rcv,
            is_executed=True,
            metadata="response metadata",
            body=b"response body",
        )

        assert response.is_executed is True
        assert response.metadata == "response metadata"
        assert response.body == b"response body"


class TestQueryResponseMessageEncode:
    """Tests for QueryResponseMessage encoding."""

    def test_encode(self):
        """Test encoding to protobuf."""
        qry_rcv = QueryMessageReceived(
            id="qry-encode",
            channel="test",
            reply_channel="reply-channel",
        )

        response = QueryResponseMessage(
            query_received=qry_rcv,
            is_executed=True,
            metadata="encoded metadata",
            body=b"encoded body",
            tags={"tag1": "value1"},
        )

        pb_response = response.encode("test-client")

        assert pb_response.ClientID == "test-client"
        assert pb_response.RequestID == "qry-encode"
        assert pb_response.Metadata == "encoded metadata"
        assert pb_response.Body == b"encoded body"
        assert pb_response.Tags["tag1"] == "value1"


class TestQueryResponseMessageDecode:
    """Tests for QueryResponseMessage decoding."""

    def test_decode_from_protobuf(self):
        """Test decoding from protobuf."""
        pb_response = MagicMock()
        pb_response.ClientID = "decoded-client"
        pb_response.RequestID = "decoded-req"
        pb_response.Executed = True
        pb_response.Error = ""
        pb_response.Timestamp = int(datetime.now().timestamp() * 1e9)
        pb_response.Metadata = "decoded metadata"
        pb_response.Body = b"decoded body"
        pb_response.Tags = {"decoded": "tag"}

        response = QueryResponseMessage.decode(pb_response)

        assert response.client_id == "decoded-client"
        assert response.metadata == "decoded metadata"
        assert response.body == b"decoded body"


class TestQueryResponseMessageFactory:
    """Tests for QueryResponseMessage factory method."""

    def test_create_valid(self):
        """Test create factory method creates valid response."""
        qry_rcv = QueryMessageReceived(
            id="qry-factory",
            reply_channel="reply",
        )

        response = QueryResponseMessage.create(
            query_received=qry_rcv,
            metadata="factory metadata",
            body=b"factory body",
            is_executed=True,
            tags={},  # Required to avoid None being passed
        )

        assert response.metadata == "factory metadata"
        assert response.is_executed is True


class TestQueryResponseMessageStr:
    """Tests for QueryResponseMessage string representations."""

    def test_repr_representation(self):
        """Test repr representation."""
        qry_rcv = QueryMessageReceived(
            id="qry-repr",
            reply_channel="reply",
        )

        response = QueryResponseMessage(
            query_received=qry_rcv,
            is_executed=True,
        )

        repr_str = repr(response)

        assert "QueryResponseMessage" in repr_str


class TestCommandMessageWithUpdates:
    """Tests for CommandMessage.with_updates (line 75)."""

    def test_with_updates_creates_copy(self):
        cmd = CommandMessage(
            id="cmd-orig",
            channel="ch",
            body=b"body",
            timeout_in_seconds=10,
        )
        updated = cmd.with_updates(body=b"new-body")
        assert updated.body == b"new-body"
        assert cmd.body == b"body"
        assert updated.channel == "ch"


class TestCommandMessageChannelEmpty:
    """Tests for CommandMessage channel empty validator (line 14)."""

    def test_channel_none_raises(self):
        with pytest.raises(Exception):
            CommandMessage(
                channel=None,
                body=b"test",
                timeout_in_seconds=10,
            )


class TestQueryMessageEncodeExtended:
    """Extended tests for QueryMessage encode (lines 75, 89, 100)."""

    def test_encode_with_cache_key_and_ttl(self):
        query = QueryMessage(
            id="q1",
            channel="ch",
            body=b"data",
            timeout_in_seconds=5,
            cache_key="my-key",
            cache_ttl_int_seconds=30,
        )
        pb = query.encode("client")
        assert pb.CacheKey == "my-key"
        assert pb.CacheTTL == 30
        assert pb.Tags is not None

    def test_encode_defaults_cache_key_empty(self):
        query = QueryMessage(
            channel="ch",
            body=b"data",
            timeout_in_seconds=5,
        )
        pb = query.encode("client")
        assert pb.CacheKey == ""
        assert pb.CacheTTL == 0

    def test_with_updates_creates_copy(self):
        query = QueryMessage(
            channel="ch",
            body=b"data",
            timeout_in_seconds=5,
        )
        updated = query.with_updates(body=b"new")
        assert updated.body == b"new"
        assert query.body == b"data"


class TestQueryMessageChannelEmpty:
    """Tests for QueryMessage channel empty validator (line 14)."""

    def test_channel_none_raises(self):
        with pytest.raises(Exception):
            QueryMessage(
                channel=None,
                body=b"test",
                timeout_in_seconds=10,
            )


class TestQueryResponseMessageEncodeEdge:
    """Test QueryResponseMessage encode with metadata/body handling (line 67)."""

    def test_encode_with_none_metadata_and_body(self):
        qry_rcv = QueryMessageReceived(
            id="qry-1",
            channel="test",
            reply_channel="reply-ch",
        )
        response = QueryResponseMessage(
            query_received=qry_rcv,
            is_executed=True,
            metadata=None,
            body=b"",
            tags={"t": "v"},
        )
        pb = response.encode("client")
        assert pb.Metadata == ""
        assert pb.Body == b""
        assert pb.Tags["t"] == "v"

    def test_encode_raises_without_query_received(self):
        response = QueryResponseMessage.__new__(QueryResponseMessage)
        object.__setattr__(response, "query_received", None)
        object.__setattr__(response, "is_executed", True)
        object.__setattr__(response, "error", "")
        object.__setattr__(response, "timestamp", datetime.now())
        object.__setattr__(response, "metadata", None)
        object.__setattr__(response, "body", b"")
        object.__setattr__(response, "tags", {})
        with pytest.raises(ValueError, match="required for encoding"):
            response.encode("client")


class TestQueryResponseCacheHit:
    """GAP-C2: Tests for CacheHit field on QueryResponseMessage."""

    def test_decode_cache_hit_true(self):
        pb_response = MagicMock()
        pb_response.ClientID = "client"
        pb_response.RequestID = "req-1"
        pb_response.Executed = True
        pb_response.Error = ""
        pb_response.Timestamp = int(datetime.now().timestamp() * 1e9)
        pb_response.Metadata = ""
        pb_response.Body = b""
        pb_response.Tags = {}
        pb_response.CacheHit = True

        response = QueryResponseMessage.decode(pb_response)
        assert response.cache_hit is True

    def test_decode_cache_hit_false_default(self):
        pb_response = MagicMock()
        pb_response.ClientID = "client"
        pb_response.RequestID = "req-1"
        pb_response.Executed = True
        pb_response.Error = ""
        pb_response.Timestamp = int(datetime.now().timestamp() * 1e9)
        pb_response.Metadata = ""
        pb_response.Body = b""
        pb_response.Tags = {}
        pb_response.CacheHit = False

        response = QueryResponseMessage.decode(pb_response)
        assert response.cache_hit is False


class TestQueryCacheKeyTtlCrossValidation:
    """GAP-C4: Tests for CacheKey/CacheTTL cross-validation."""

    def test_cache_key_without_ttl_raises(self):
        with pytest.raises(ValueError, match="cache_ttl_int_seconds must be > 0"):
            QueryMessage(
                channel="ch",
                body=b"data",
                timeout_in_seconds=5,
                cache_key="my-key",
                cache_ttl_int_seconds=0,
            )

    def test_cache_key_with_valid_ttl_passes(self):
        query = QueryMessage(
            channel="ch",
            body=b"data",
            timeout_in_seconds=5,
            cache_key="my-key",
            cache_ttl_int_seconds=30,
        )
        assert query.cache_key == "my-key"
        assert query.cache_ttl_int_seconds == 30

    def test_no_cache_key_zero_ttl_passes(self):
        query = QueryMessage(
            channel="ch",
            body=b"data",
            timeout_in_seconds=5,
            cache_key="",
            cache_ttl_int_seconds=0,
        )
        assert query.cache_key == ""
        assert query.cache_ttl_int_seconds == 0


class TestCommandMessageChannelValidation:
    """GAP-H1/H2/H3: Channel validation for CommandMessage."""

    def test_wildcard_star_raises(self):
        with pytest.raises(ValueError, match="wildcard"):
            CommandMessage(channel="cmd.*", body=b"x", timeout_in_seconds=5)

    def test_wildcard_gt_raises(self):
        with pytest.raises(ValueError, match="wildcard"):
            CommandMessage(channel="cmd.>", body=b"x", timeout_in_seconds=5)

    def test_whitespace_raises(self):
        with pytest.raises(ValueError, match="whitespace"):
            CommandMessage(channel="cmd channel", body=b"x", timeout_in_seconds=5)

    def test_trailing_dot_raises(self):
        with pytest.raises(ValueError, match="end with"):
            CommandMessage(channel="cmd.", body=b"x", timeout_in_seconds=5)


class TestQueryMessageChannelValidation:
    """GAP-H1/H2/H3: Channel validation for QueryMessage."""

    def test_wildcard_star_raises(self):
        with pytest.raises(ValueError, match="wildcard"):
            QueryMessage(channel="qry.*", body=b"x", timeout_in_seconds=5)

    def test_wildcard_gt_raises(self):
        with pytest.raises(ValueError, match="wildcard"):
            QueryMessage(channel="qry.>", body=b"x", timeout_in_seconds=5)

    def test_whitespace_raises(self):
        with pytest.raises(ValueError, match="whitespace"):
            QueryMessage(channel="qry channel", body=b"x", timeout_in_seconds=5)

    def test_trailing_dot_raises(self):
        with pytest.raises(ValueError, match="end with"):
            QueryMessage(channel="qry.", body=b"x", timeout_in_seconds=5)


class TestQueryMessageEncodeWithSpan:
    """Test QueryMessage.encode() with span bytes sets Span field."""

    def test_encode_with_span_sets_field(self):
        query = QueryMessage(
            id="q-span",
            channel="ch",
            body=b"data",
            timeout_in_seconds=5,
        )
        pb = query.encode("client", span=b"\x01\x02")
        assert pb.Span == b"\x01\x02"

    def test_encode_without_span_leaves_empty(self):
        query = QueryMessage(
            id="q-no-span",
            channel="ch",
            body=b"data",
            timeout_in_seconds=5,
        )
        pb = query.encode("client")
        assert pb.Span == b""


class TestCommandMessageEncodeWithSpan:
    """Test CommandMessage.encode() with span bytes sets Span field."""

    def test_encode_with_span_sets_field(self):
        cmd = CommandMessage(
            id="c-span",
            channel="ch",
            body=b"data",
            timeout_in_seconds=5,
        )
        pb = cmd.encode("client", span=b"\x01\x02")
        assert pb.Span == b"\x01\x02"

    def test_encode_without_span_leaves_empty(self):
        cmd = CommandMessage(
            id="c-no-span",
            channel="ch",
            body=b"data",
            timeout_in_seconds=5,
        )
        pb = cmd.encode("client")
        assert pb.Span == b""


# ==============================================================================
# Coverage Gap Tests
# ==============================================================================


class TestQueryMessageEncodeWithTagsCoverage:
    """Cover line 81: pb_query.Tags[key] = value in encode with non-empty tags."""

    def test_encode_with_tags_sets_pb_tags(self):
        query = QueryMessage(
            id="q-tags",
            channel="ch",
            body=b"data",
            tags={"env": "prod", "version": "2"},
            timeout_in_seconds=5,
        )
        pb = query.encode("client")
        assert pb.Tags["env"] == "prod"
        assert pb.Tags["version"] == "2"
