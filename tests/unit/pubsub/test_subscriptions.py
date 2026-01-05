"""Unit tests for kubemq.pubsub subscription classes.

Tests for EventsSubscription and EventsStoreSubscription classes.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from kubemq.common.subscribe_type import SubscribeType
from kubemq.pubsub.event_message_received import EventMessageReceived
from kubemq.pubsub.event_store_message_received import EventStoreMessageReceived
from kubemq.pubsub.events_store_subscription import EventsStoreSubscription, EventsStoreType
from kubemq.pubsub.events_subscription import EventsSubscription


class TestEventsSubscriptionValidation:
    """Tests for EventsSubscription validation."""

    def test_requires_channel(self):
        """Test that channel is required."""
        with pytest.raises(ValueError, match="must have a channel"):
            EventsSubscription(
                channel="",
                on_receive_event_callback=MagicMock(),
            )

    def test_requires_channel_not_none(self):
        """Test that channel cannot be None."""
        # Pydantic raises ValidationError for wrong type before our validator runs
        with pytest.raises(Exception):
            EventsSubscription(
                channel=None,  # type: ignore[arg-type]
                on_receive_event_callback=MagicMock(),
            )

    def test_valid_subscription(self):
        """Test valid subscription creation."""
        callback = MagicMock()
        sub = EventsSubscription(
            channel="test-channel",
            on_receive_event_callback=callback,
        )

        assert sub.channel == "test-channel"
        assert sub.group is None
        assert sub.on_receive_event_callback == callback

    def test_valid_subscription_with_group(self):
        """Test valid subscription with group."""
        callback = MagicMock()
        sub = EventsSubscription(
            channel="test-channel",
            group="test-group",
            on_receive_event_callback=callback,
        )

        assert sub.channel == "test-channel"
        assert sub.group == "test-group"


class TestEventsSubscriptionEncode:
    """Tests for EventsSubscription encoding."""

    def test_encode_basic(self):
        """Test basic encoding to Subscribe message."""
        sub = EventsSubscription(
            channel="encode-channel",
            on_receive_event_callback=MagicMock(),
        )

        request = sub.encode("test-client")

        assert request.Channel == "encode-channel"
        assert request.ClientID == "test-client"
        assert request.Group == ""
        assert request.SubscribeTypeData == SubscribeType.Events.value

    def test_encode_with_group(self):
        """Test encoding with group."""
        sub = EventsSubscription(
            channel="encode-channel",
            group="encode-group",
            on_receive_event_callback=MagicMock(),
        )

        request = sub.encode("test-client")

        assert request.Group == "encode-group"


class TestEventsSubscriptionCallbacks:
    """Tests for EventsSubscription callback handling."""

    def test_raise_on_receive_message(self):
        """Test raise_on_receive_message calls callback."""
        callback = MagicMock()
        sub = EventsSubscription(
            channel="test-channel",
            on_receive_event_callback=callback,
        )

        event = EventMessageReceived(
            id="msg-1",
            channel="test-channel",
            body=b"test body",
        )

        sub.raise_on_receive_message(event)

        callback.assert_called_once_with(event)

    def test_raise_on_error(self):
        """Test raise_on_error calls error callback."""
        callback = MagicMock()
        error_callback = MagicMock()
        sub = EventsSubscription(
            channel="test-channel",
            on_receive_event_callback=callback,
            on_error_callback=error_callback,
        )

        sub.raise_on_error("Test error")

        error_callback.assert_called_once_with("Test error")

    def test_raise_on_error_without_callback(self):
        """Test raise_on_error does nothing without callback."""
        sub = EventsSubscription(
            channel="test-channel",
            on_receive_event_callback=MagicMock(),
            on_error_callback=None,
        )

        # Should not raise
        sub.raise_on_error("Test error")

    @pytest.mark.asyncio
    async def test_raise_on_receive_message_async_with_sync_callback(self):
        """Test async raise_on_receive_message with sync callback."""
        callback = MagicMock()
        sub = EventsSubscription(
            channel="test-channel",
            on_receive_event_callback=callback,
        )

        event = EventMessageReceived(
            id="msg-1",
            channel="test-channel",
            body=b"test",
        )

        await sub.raise_on_receive_message_async(event)

        callback.assert_called_once_with(event)

    @pytest.mark.asyncio
    async def test_raise_on_receive_message_async_with_async_callback(self):
        """Test async raise_on_receive_message with async callback."""
        callback = AsyncMock()
        sub = EventsSubscription(
            channel="test-channel",
            on_receive_event_callback=callback,
        )

        event = EventMessageReceived(
            id="msg-1",
            channel="test-channel",
            body=b"test",
        )

        await sub.raise_on_receive_message_async(event)

        callback.assert_called_once_with(event)


class TestEventsSubscriptionModelDump:
    """Tests for EventsSubscription model_dump."""

    def test_model_dump_excludes_callbacks(self):
        """Test model_dump excludes callback functions."""
        sub = EventsSubscription(
            channel="test-channel",
            on_receive_event_callback=MagicMock(),
            on_error_callback=MagicMock(),
        )

        dump = sub.model_dump()

        assert "on_receive_event_callback" not in dump
        assert "on_error_callback" not in dump
        assert dump["channel"] == "test-channel"


class TestEventsStoreSubscriptionValidation:
    """Tests for EventsStoreSubscription validation."""

    def test_requires_channel(self):
        """Test that channel is required."""
        with pytest.raises(ValueError, match="must have a channel"):
            EventsStoreSubscription(
                channel="",
                events_store_type=EventsStoreType.StartNewOnly,
                on_receive_event_callback=MagicMock(),
            )

    def test_requires_events_store_type(self):
        """Test that events_store_type must not be Undefined."""
        with pytest.raises(ValueError, match="must have an events store type"):
            EventsStoreSubscription(
                channel="test-channel",
                events_store_type=EventsStoreType.Undefined,
                on_receive_event_callback=MagicMock(),
            )

    def test_valid_subscription_start_new_only(self):
        """Test valid subscription with StartNewOnly type."""
        callback = MagicMock()
        sub = EventsStoreSubscription(
            channel="test-channel",
            events_store_type=EventsStoreType.StartNewOnly,
            on_receive_event_callback=callback,
        )

        assert sub.channel == "test-channel"
        assert sub.events_store_type == EventsStoreType.StartNewOnly

    def test_valid_subscription_start_from_first(self):
        """Test valid subscription with StartFromFirst type."""
        sub = EventsStoreSubscription(
            channel="test-channel",
            events_store_type=EventsStoreType.StartFromFirst,
            on_receive_event_callback=MagicMock(),
        )

        assert sub.events_store_type == EventsStoreType.StartFromFirst

    def test_valid_subscription_start_from_last(self):
        """Test valid subscription with StartFromLast type."""
        sub = EventsStoreSubscription(
            channel="test-channel",
            events_store_type=EventsStoreType.StartFromLast,
            on_receive_event_callback=MagicMock(),
        )

        assert sub.events_store_type == EventsStoreType.StartFromLast


class TestEventsStoreSubscriptionEncode:
    """Tests for EventsStoreSubscription encoding."""

    def test_encode_start_new_only(self):
        """Test encoding StartNewOnly subscription."""
        sub = EventsStoreSubscription(
            channel="encode-channel",
            events_store_type=EventsStoreType.StartNewOnly,
            on_receive_event_callback=MagicMock(),
        )

        request = sub.encode("test-client")

        assert request.Channel == "encode-channel"
        assert request.ClientID == "test-client"
        assert request.EventsStoreTypeData == EventsStoreType.StartNewOnly.value
        assert request.SubscribeTypeData == SubscribeType.EventsStore.value

    def test_encode_start_from_first(self):
        """Test encoding StartFromFirst subscription."""
        sub = EventsStoreSubscription(
            channel="encode-channel",
            events_store_type=EventsStoreType.StartFromFirst,
            on_receive_event_callback=MagicMock(),
        )

        request = sub.encode("test-client")

        assert request.EventsStoreTypeData == EventsStoreType.StartFromFirst.value

    def test_encode_start_from_last(self):
        """Test encoding StartFromLast subscription."""
        sub = EventsStoreSubscription(
            channel="encode-channel",
            events_store_type=EventsStoreType.StartFromLast,
            on_receive_event_callback=MagicMock(),
        )

        request = sub.encode("test-client")

        assert request.EventsStoreTypeData == EventsStoreType.StartFromLast.value


class TestEventsStoreSubscriptionCallbacks:
    """Tests for EventsStoreSubscription callback handling."""

    def test_raise_on_receive_message(self):
        """Test raise_on_receive_message calls callback."""
        callback = MagicMock()
        sub = EventsStoreSubscription(
            channel="test-channel",
            events_store_type=EventsStoreType.StartNewOnly,
            on_receive_event_callback=callback,
        )

        event = EventStoreMessageReceived(
            id="msg-1",
            channel="test-channel",
            body=b"test body",
            sequence=1,
        )

        sub.raise_on_receive_message(event)

        callback.assert_called_once_with(event)

    @pytest.mark.asyncio
    async def test_raise_on_receive_message_async_with_async_callback(self):
        """Test async raise_on_receive_message with async callback."""
        callback = AsyncMock()
        sub = EventsStoreSubscription(
            channel="test-channel",
            events_store_type=EventsStoreType.StartNewOnly,
            on_receive_event_callback=callback,
        )

        event = EventStoreMessageReceived(
            id="msg-1",
            channel="test-channel",
            body=b"test",
            sequence=1,
        )

        await sub.raise_on_receive_message_async(event)

        callback.assert_called_once_with(event)


class TestEventsStoreSubscriptionModelDump:
    """Tests for EventsStoreSubscription model_dump."""

    def test_model_dump_converts_enum(self):
        """Test model_dump converts events_store_type to name."""
        sub = EventsStoreSubscription(
            channel="test-channel",
            events_store_type=EventsStoreType.StartFromFirst,
            on_receive_event_callback=MagicMock(),
        )

        dump = sub.model_dump()

        assert dump["events_store_type"] == "StartFromFirst"
        assert "on_receive_event_callback" not in dump

    def test_model_dump_excludes_callbacks(self):
        """Test model_dump excludes callback functions."""
        sub = EventsStoreSubscription(
            channel="test-channel",
            events_store_type=EventsStoreType.StartNewOnly,
            on_receive_event_callback=MagicMock(),
            on_error_callback=MagicMock(),
        )

        dump = sub.model_dump()

        assert "on_receive_event_callback" not in dump
        assert "on_error_callback" not in dump


class TestEventsStoreType:
    """Tests for EventsStoreType enum."""

    def test_enum_values(self):
        """Test enum values are correct."""
        assert EventsStoreType.Undefined.value == 0
        assert EventsStoreType.StartNewOnly.value == 1
        assert EventsStoreType.StartFromFirst.value == 2
        assert EventsStoreType.StartFromLast.value == 3
        assert EventsStoreType.StartAtSequence.value == 4
        assert EventsStoreType.StartAtTime.value == 5
        assert EventsStoreType.StartAtTimeDelta.value == 6
