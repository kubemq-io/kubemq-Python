"""Unit tests for kubemq.cq subscription classes.

Tests for CommandsSubscription and QueriesSubscription classes.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from kubemq.common.subscribe_type import SubscribeType
from kubemq.cq.command_message_received import CommandMessageReceived
from kubemq.cq.commands_subscription import CommandsSubscription
from kubemq.cq.queries_subscription import QueriesSubscription
from kubemq.cq.query_message_received import QueryMessageReceived


class TestCommandsSubscriptionValidation:
    """Tests for CommandsSubscription validation."""

    def test_requires_channel(self):
        """Test that channel is required."""
        with pytest.raises(ValueError, match="must have a channel"):
            CommandsSubscription(
                channel="",
                on_receive_command_callback=MagicMock(),
            )

    def test_requires_channel_not_none(self):
        """Test that channel cannot be None."""
        # Pydantic raises ValidationError for wrong type before our validator runs
        with pytest.raises(Exception):
            CommandsSubscription(
                channel=None,  # type: ignore[arg-type]
                on_receive_command_callback=MagicMock(),
            )

    def test_valid_subscription(self):
        """Test valid subscription creation."""
        callback = MagicMock()
        sub = CommandsSubscription(
            channel="test-channel",
            on_receive_command_callback=callback,
        )

        assert sub.channel == "test-channel"
        assert sub.group is None
        assert sub.on_receive_command_callback == callback

    def test_valid_subscription_with_group(self):
        """Test valid subscription with group."""
        callback = MagicMock()
        sub = CommandsSubscription(
            channel="test-channel",
            group="test-group",
            on_receive_command_callback=callback,
        )

        assert sub.channel == "test-channel"
        assert sub.group == "test-group"


class TestCommandsSubscriptionEncode:
    """Tests for CommandsSubscription encoding."""

    def test_encode_basic(self):
        """Test basic encoding to Subscribe message."""
        sub = CommandsSubscription(
            channel="encode-channel",
            on_receive_command_callback=MagicMock(),
        )

        request = sub.encode("test-client")

        assert request.Channel == "encode-channel"
        assert request.ClientID == "test-client"
        assert request.Group == ""
        assert request.SubscribeTypeData == SubscribeType.Commands.value

    def test_encode_with_group(self):
        """Test encoding with group."""
        sub = CommandsSubscription(
            channel="encode-channel",
            group="encode-group",
            on_receive_command_callback=MagicMock(),
        )

        request = sub.encode("test-client")

        assert request.Group == "encode-group"


class TestCommandsSubscriptionCallbacks:
    """Tests for CommandsSubscription callback handling."""

    def test_raise_on_receive_message(self):
        """Test raise_on_receive_message calls callback."""
        callback = MagicMock()
        sub = CommandsSubscription(
            channel="test-channel",
            on_receive_command_callback=callback,
        )

        command = CommandMessageReceived(
            id="cmd-1",
            channel="test-channel",
            body=b"test body",
        )

        sub.raise_on_receive_message(command)

        callback.assert_called_once_with(command)

    def test_raise_on_error(self):
        """Test raise_on_error calls error callback."""
        callback = MagicMock()
        error_callback = MagicMock()
        sub = CommandsSubscription(
            channel="test-channel",
            on_receive_command_callback=callback,
            on_error_callback=error_callback,
        )

        sub.raise_on_error("Test error")

        error_callback.assert_called_once_with("Test error")

    def test_raise_on_error_without_callback(self):
        """Test raise_on_error does nothing without callback."""
        sub = CommandsSubscription(
            channel="test-channel",
            on_receive_command_callback=MagicMock(),
            on_error_callback=None,
        )

        # Should not raise
        sub.raise_on_error("Test error")

    @pytest.mark.asyncio
    async def test_raise_on_receive_message_async_with_sync_callback(self):
        """Test async raise_on_receive_message with sync callback."""
        callback = MagicMock()
        sub = CommandsSubscription(
            channel="test-channel",
            on_receive_command_callback=callback,
        )

        command = CommandMessageReceived(
            id="cmd-1",
            channel="test-channel",
            body=b"test",
        )

        await sub.raise_on_receive_message_async(command)

        callback.assert_called_once_with(command)

    @pytest.mark.asyncio
    async def test_raise_on_receive_message_async_with_async_callback(self):
        """Test async raise_on_receive_message with async callback."""
        callback = AsyncMock()
        sub = CommandsSubscription(
            channel="test-channel",
            on_receive_command_callback=callback,
        )

        command = CommandMessageReceived(
            id="cmd-1",
            channel="test-channel",
            body=b"test",
        )

        await sub.raise_on_receive_message_async(command)

        callback.assert_called_once_with(command)

    @pytest.mark.asyncio
    async def test_raise_on_error_async_with_async_callback(self):
        """Test async raise_on_error with async callback."""
        error_callback = AsyncMock()
        sub = CommandsSubscription(
            channel="test-channel",
            on_receive_command_callback=MagicMock(),
            on_error_callback=error_callback,
        )

        await sub.raise_on_error_async("Test error")

        error_callback.assert_called_once_with("Test error")


class TestCommandsSubscriptionFactory:
    """Tests for CommandsSubscription factory method."""

    def test_create_valid(self):
        """Test create factory method creates valid subscription."""
        callback = MagicMock()
        sub = CommandsSubscription.create(
            channel="factory-channel",
            group="factory-group",
            on_receive_command_callback=callback,
        )

        assert sub.channel == "factory-channel"
        assert sub.group == "factory-group"
        assert sub.on_receive_command_callback == callback

    def test_create_raises_on_invalid(self):
        """Test create factory method raises on invalid input."""
        with pytest.raises(ValueError):
            CommandsSubscription.create(
                channel="",
                on_receive_command_callback=MagicMock(),
            )


class TestCommandsSubscriptionStr:
    """Tests for CommandsSubscription string representations."""

    def test_repr_representation(self):
        """Test repr representation."""
        sub = CommandsSubscription(
            channel="repr-channel",
            group="repr-group",
            on_receive_command_callback=MagicMock(),
        )

        repr_str = repr(sub)

        assert "CommandsSubscription" in repr_str
        assert "repr-channel" in repr_str
        assert "repr-group" in repr_str


class TestQueriesSubscriptionValidation:
    """Tests for QueriesSubscription validation."""

    def test_requires_channel(self):
        """Test that channel is required."""
        with pytest.raises(ValueError, match="must have a channel"):
            QueriesSubscription(
                channel="",
                on_receive_query_callback=MagicMock(),
            )

    def test_requires_channel_not_none(self):
        """Test that channel cannot be None."""
        # Pydantic raises ValidationError for wrong type before our validator runs
        with pytest.raises(Exception):
            QueriesSubscription(
                channel=None,
                on_receive_query_callback=MagicMock(),
            )

    def test_valid_subscription(self):
        """Test valid subscription creation."""
        callback = MagicMock()
        sub = QueriesSubscription(
            channel="test-channel",
            on_receive_query_callback=callback,
        )

        assert sub.channel == "test-channel"
        assert sub.group is None
        assert sub.on_receive_query_callback == callback

    def test_valid_subscription_with_group(self):
        """Test valid subscription with group."""
        callback = MagicMock()
        sub = QueriesSubscription(
            channel="test-channel",
            group="test-group",
            on_receive_query_callback=callback,
        )

        assert sub.channel == "test-channel"
        assert sub.group == "test-group"


class TestQueriesSubscriptionEncode:
    """Tests for QueriesSubscription encoding."""

    def test_encode_basic(self):
        """Test basic encoding to Subscribe message."""
        sub = QueriesSubscription(
            channel="encode-channel",
            on_receive_query_callback=MagicMock(),
        )

        request = sub.encode("test-client")

        assert request.Channel == "encode-channel"
        assert request.ClientID == "test-client"
        assert request.Group == ""
        assert request.SubscribeTypeData == SubscribeType.Queries.value

    def test_encode_with_group(self):
        """Test encoding with group."""
        sub = QueriesSubscription(
            channel="encode-channel",
            group="encode-group",
            on_receive_query_callback=MagicMock(),
        )

        request = sub.encode("test-client")

        assert request.Group == "encode-group"


class TestQueriesSubscriptionCallbacks:
    """Tests for QueriesSubscription callback handling."""

    def test_raise_on_receive_message(self):
        """Test raise_on_receive_message calls callback."""
        callback = MagicMock()
        sub = QueriesSubscription(
            channel="test-channel",
            on_receive_query_callback=callback,
        )

        query = QueryMessageReceived(
            id="qry-1",
            channel="test-channel",
            body=b"test body",
        )

        sub.raise_on_receive_message(query)

        callback.assert_called_once_with(query)

    def test_raise_on_error(self):
        """Test raise_on_error calls error callback."""
        callback = MagicMock()
        error_callback = MagicMock()
        sub = QueriesSubscription(
            channel="test-channel",
            on_receive_query_callback=callback,
            on_error_callback=error_callback,
        )

        sub.raise_on_error("Test error")

        error_callback.assert_called_once_with("Test error")

    def test_raise_on_error_without_callback(self):
        """Test raise_on_error does nothing without callback."""
        sub = QueriesSubscription(
            channel="test-channel",
            on_receive_query_callback=MagicMock(),
            on_error_callback=None,
        )

        # Should not raise
        sub.raise_on_error("Test error")

    @pytest.mark.asyncio
    async def test_raise_on_receive_message_async_with_sync_callback(self):
        """Test async raise_on_receive_message with sync callback."""
        callback = MagicMock()
        sub = QueriesSubscription(
            channel="test-channel",
            on_receive_query_callback=callback,
        )

        query = QueryMessageReceived(
            id="qry-1",
            channel="test-channel",
            body=b"test",
        )

        await sub.raise_on_receive_message_async(query)

        callback.assert_called_once_with(query)

    @pytest.mark.asyncio
    async def test_raise_on_receive_message_async_with_async_callback(self):
        """Test async raise_on_receive_message with async callback."""
        callback = AsyncMock()
        sub = QueriesSubscription(
            channel="test-channel",
            on_receive_query_callback=callback,
        )

        query = QueryMessageReceived(
            id="qry-1",
            channel="test-channel",
            body=b"test",
        )

        await sub.raise_on_receive_message_async(query)

        callback.assert_called_once_with(query)

    @pytest.mark.asyncio
    async def test_raise_on_error_async_with_async_callback(self):
        """Test async raise_on_error with async callback."""
        error_callback = AsyncMock()
        sub = QueriesSubscription(
            channel="test-channel",
            on_receive_query_callback=MagicMock(),
            on_error_callback=error_callback,
        )

        await sub.raise_on_error_async("Test error")

        error_callback.assert_called_once_with("Test error")


class TestQueriesSubscriptionFactory:
    """Tests for QueriesSubscription factory method."""

    def test_create_valid(self):
        """Test create factory method creates valid subscription."""
        callback = MagicMock()
        sub = QueriesSubscription.create(
            channel="factory-channel",
            group="factory-group",
            on_receive_query_callback=callback,
        )

        assert sub.channel == "factory-channel"
        assert sub.group == "factory-group"
        assert sub.on_receive_query_callback == callback


class TestQueriesSubscriptionStr:
    """Tests for QueriesSubscription string representations."""

    def test_repr_representation(self):
        """Test repr representation."""
        sub = QueriesSubscription(
            channel="repr-channel",
            group="repr-group",
            on_receive_query_callback=MagicMock(),
        )

        repr_str = repr(sub)

        assert "QueriesSubscription" in repr_str
        assert "repr-channel" in repr_str
        assert "repr-group" in repr_str
