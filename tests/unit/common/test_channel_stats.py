"""Unit tests for kubemq.common.channel_stats module.

Tests for channel stats and channel classes.
"""

from __future__ import annotations

import json

from kubemq.common.channel_stats import (
    CQChannel,
    CQStats,
    PubSubChannel,
    PubSubStats,
    QueuesChannel,
    QueuesStats,
    decode_cq_channel_list,
    decode_pub_sub_channel_list,
    decode_queues_channel_list,
)


class TestQueuesStats:
    """Tests for QueuesStats class."""

    def test_init_stores_values(self):
        """Test initialization stores all values."""
        stats = QueuesStats(
            messages=100,
            volume=5000,
            waiting=10,
            expired=5,
            delayed=2,
        )

        assert stats.messages == 100
        assert stats.volume == 5000
        assert stats.waiting == 10
        assert stats.expired == 5
        assert stats.delayed == 2

    def test_repr(self):
        """Test string representation."""
        stats = QueuesStats(messages=10, volume=100, waiting=2, expired=1, delayed=0)

        repr_str = repr(stats)

        assert "messages=10" in repr_str
        assert "volume=100" in repr_str
        assert "waiting=2" in repr_str

    def test_accepts_kwargs(self):
        """Test accepts additional kwargs."""
        stats = QueuesStats(messages=1, volume=1, waiting=0, expired=0, delayed=0, extra="ignored")

        assert stats.messages == 1


class TestQueuesChannel:
    """Tests for QueuesChannel class."""

    def test_init_stores_values(self):
        """Test initialization stores all values."""
        incoming = QueuesStats(messages=10, volume=100, waiting=1, expired=0, delayed=0)
        outgoing = QueuesStats(messages=8, volume=80, waiting=0, expired=0, delayed=0)

        channel = QueuesChannel(
            name="test-queue",
            type="queues",
            last_activity=1234567890,
            is_active=True,
            incoming=incoming,
            outgoing=outgoing,
        )

        assert channel.name == "test-queue"
        assert channel.type == "queues"
        assert channel.last_activity == 1234567890
        assert channel.is_active is True
        assert channel.incoming is incoming
        assert channel.outgoing is outgoing

    def test_repr(self):
        """Test string representation."""
        incoming = QueuesStats(messages=1, volume=1, waiting=0, expired=0, delayed=0)
        outgoing = QueuesStats(messages=1, volume=1, waiting=0, expired=0, delayed=0)

        channel = QueuesChannel(
            name="my-queue",
            type="queues",
            last_activity=0,
            is_active=False,
            incoming=incoming,
            outgoing=outgoing,
        )

        repr_str = repr(channel)

        assert "my-queue" in repr_str
        assert "queues" in repr_str


class TestPubSubStats:
    """Tests for PubSubStats class."""

    def test_init_stores_values(self):
        """Test initialization stores all values."""
        stats = PubSubStats(messages=50, volume=2500)

        assert stats.messages == 50
        assert stats.volume == 2500

    def test_repr(self):
        """Test string representation."""
        stats = PubSubStats(messages=100, volume=5000)

        repr_str = repr(stats)

        assert "messages=100" in repr_str
        assert "volume=5000" in repr_str


class TestPubSubChannel:
    """Tests for PubSubChannel class."""

    def test_init_stores_values(self):
        """Test initialization stores all values."""
        incoming = PubSubStats(messages=20, volume=1000)
        outgoing = PubSubStats(messages=18, volume=900)

        channel = PubSubChannel(
            name="events-channel",
            type="events",
            last_activity=1234567890,
            is_active=True,
            incoming=incoming,
            outgoing=outgoing,
        )

        assert channel.name == "events-channel"
        assert channel.type == "events"
        assert channel.is_active is True

    def test_repr(self):
        """Test string representation."""
        incoming = PubSubStats(messages=1, volume=1)
        outgoing = PubSubStats(messages=1, volume=1)

        channel = PubSubChannel(
            name="my-events",
            type="events_store",
            last_activity=0,
            is_active=True,
            incoming=incoming,
            outgoing=outgoing,
        )

        repr_str = repr(channel)

        assert "my-events" in repr_str


class TestCQStats:
    """Tests for CQStats class."""

    def test_init_stores_values(self):
        """Test initialization stores all values."""
        stats = CQStats(messages=30, volume=1500, responses=25)

        assert stats.messages == 30
        assert stats.volume == 1500
        assert stats.responses == 25

    def test_repr(self):
        """Test string representation."""
        stats = CQStats(messages=10, volume=500, responses=8)

        repr_str = repr(stats)

        assert "messages=10" in repr_str
        assert "responses=8" in repr_str


class TestCQChannel:
    """Tests for CQChannel class."""

    def test_init_stores_values(self):
        """Test initialization stores all values."""
        incoming = CQStats(messages=15, volume=750, responses=12)
        outgoing = CQStats(messages=15, volume=750, responses=12)

        channel = CQChannel(
            name="commands-channel",
            type="commands",
            last_activity=1234567890,
            is_active=True,
            incoming=incoming,
            outgoing=outgoing,
        )

        assert channel.name == "commands-channel"
        assert channel.type == "commands"

    def test_repr(self):
        """Test string representation."""
        incoming = CQStats(messages=1, volume=1, responses=1)
        outgoing = CQStats(messages=1, volume=1, responses=1)

        channel = CQChannel(
            name="my-queries",
            type="queries",
            last_activity=0,
            is_active=False,
            incoming=incoming,
            outgoing=outgoing,
        )

        repr_str = repr(channel)

        assert "my-queries" in repr_str


class TestDecodePubSubChannelList:
    """Tests for decode_pub_sub_channel_list function."""

    def test_decode_empty_list(self):
        """Test decoding empty list."""
        data = json.dumps([]).encode("utf-8")

        result = decode_pub_sub_channel_list(data)

        assert result == []

    def test_decode_single_channel(self):
        """Test decoding single channel."""
        data = json.dumps(
            [
                {
                    "name": "test-channel",
                    "type": "events",
                    "lastActivity": 1234567890,
                    "isActive": True,
                    "incoming": {"messages": 10, "volume": 100},
                    "outgoing": {"messages": 8, "volume": 80},
                }
            ]
        ).encode("utf-8")

        result = decode_pub_sub_channel_list(data)

        assert len(result) == 1
        assert result[0].name == "test-channel"
        assert result[0].type == "events"
        assert result[0].is_active is True
        assert result[0].incoming.messages == 10

    def test_decode_multiple_channels(self):
        """Test decoding multiple channels."""
        data = json.dumps(
            [
                {
                    "name": "channel-1",
                    "type": "events",
                    "lastActivity": 100,
                    "isActive": True,
                    "incoming": {"messages": 5, "volume": 50},
                    "outgoing": {"messages": 5, "volume": 50},
                },
                {
                    "name": "channel-2",
                    "type": "events_store",
                    "lastActivity": 200,
                    "isActive": False,
                    "incoming": {"messages": 10, "volume": 100},
                    "outgoing": {"messages": 10, "volume": 100},
                },
            ]
        ).encode("utf-8")

        result = decode_pub_sub_channel_list(data)

        assert len(result) == 2
        assert result[0].name == "channel-1"
        assert result[1].name == "channel-2"


class TestDecodeQueuesChannelList:
    """Tests for decode_queues_channel_list function."""

    def test_decode_empty_list(self):
        """Test decoding empty list."""
        data = json.dumps([]).encode("utf-8")

        result = decode_queues_channel_list(data)

        assert result == []

    def test_decode_single_channel(self):
        """Test decoding single channel."""
        data = json.dumps(
            [
                {
                    "name": "test-queue",
                    "type": "queues",
                    "lastActivity": 1234567890,
                    "isActive": True,
                    "incoming": {
                        "messages": 20,
                        "volume": 200,
                        "waiting": 5,
                        "expired": 1,
                        "delayed": 2,
                    },
                    "outgoing": {
                        "messages": 15,
                        "volume": 150,
                        "waiting": 0,
                        "expired": 0,
                        "delayed": 0,
                    },
                }
            ]
        ).encode("utf-8")

        result = decode_queues_channel_list(data)

        assert len(result) == 1
        assert result[0].name == "test-queue"
        assert result[0].incoming.waiting == 5


class TestDecodeCQChannelList:
    """Tests for decode_cq_channel_list function."""

    def test_decode_empty_list(self):
        """Test decoding empty list."""
        data = json.dumps([]).encode("utf-8")

        result = decode_cq_channel_list(data)

        assert result == []

    def test_decode_single_channel(self):
        """Test decoding single channel."""
        data = json.dumps(
            [
                {
                    "name": "test-commands",
                    "type": "commands",
                    "lastActivity": 1234567890,
                    "isActive": True,
                    "incoming": {"messages": 30, "volume": 300, "responses": 28},
                    "outgoing": {"messages": 28, "volume": 280, "responses": 28},
                }
            ]
        ).encode("utf-8")

        result = decode_cq_channel_list(data)

        assert len(result) == 1
        assert result[0].name == "test-commands"
        assert result[0].incoming.responses == 28
