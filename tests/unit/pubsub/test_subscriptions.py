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


class TestEventsStoreSubscriptionSequenceValidation:
    """Tests for EventsStoreSubscription sequence value validation."""

    def test_start_at_sequence_with_zero_raises(self):
        with pytest.raises(ValueError, match="sequence value"):
            EventsStoreSubscription(
                channel="ch",
                events_store_type=EventsStoreType.StartAtSequence,
                events_store_sequence_value=0,
                on_receive_event_callback=MagicMock(),
            )

    def test_start_at_sequence_with_positive_value(self):
        sub = EventsStoreSubscription(
            channel="ch",
            events_store_type=EventsStoreType.StartAtSequence,
            events_store_sequence_value=42,
            on_receive_event_callback=MagicMock(),
        )
        assert sub.events_store_sequence_value == 42


class TestEventsStoreSubscriptionTimeValidation:
    """Tests for EventsStoreSubscription start time validation."""

    def test_start_at_time_with_none_raises(self):
        with pytest.raises(ValueError, match="start time"):
            EventsStoreSubscription(
                channel="ch",
                events_store_type=EventsStoreType.StartAtTime,
                events_store_start_time=None,
                on_receive_event_callback=MagicMock(),
            )

    def test_start_at_time_with_datetime(self):
        from datetime import datetime

        dt = datetime(2025, 1, 1, 12, 0, 0)
        sub = EventsStoreSubscription(
            channel="ch",
            events_store_type=EventsStoreType.StartAtTime,
            events_store_start_time=dt,
            on_receive_event_callback=MagicMock(),
        )
        assert sub.events_store_start_time == dt


class TestEventsStoreSubscriptionCallbacksExtended:
    """Extended tests for EventsStoreSubscription callback handling."""

    def test_raise_on_receive_message_no_callback(self):
        sub = EventsStoreSubscription(
            channel="ch",
            events_store_type=EventsStoreType.StartNewOnly,
            on_receive_event_callback=MagicMock(),
        )
        sub.on_receive_event_callback = None
        sub.raise_on_receive_message(MagicMock())

    @pytest.mark.asyncio
    async def test_raise_on_receive_message_async_with_sync_callback(self):
        callback = MagicMock()
        sub = EventsStoreSubscription(
            channel="ch",
            events_store_type=EventsStoreType.StartNewOnly,
            on_receive_event_callback=callback,
        )
        event = EventStoreMessageReceived(id="m1", channel="ch", body=b"x", sequence=1)
        await sub.raise_on_receive_message_async(event)
        callback.assert_called_once_with(event)

    def test_raise_on_error_no_callback(self):
        sub = EventsStoreSubscription(
            channel="ch",
            events_store_type=EventsStoreType.StartNewOnly,
            on_receive_event_callback=MagicMock(),
            on_error_callback=None,
        )
        sub.raise_on_error("err")

    @pytest.mark.asyncio
    async def test_raise_on_error_async_with_sync_callback(self):
        err_cb = MagicMock()
        sub = EventsStoreSubscription(
            channel="ch",
            events_store_type=EventsStoreType.StartNewOnly,
            on_receive_event_callback=MagicMock(),
            on_error_callback=err_cb,
        )
        await sub.raise_on_error_async("some error")
        err_cb.assert_called_once_with("some error")

    @pytest.mark.asyncio
    async def test_raise_on_error_async_with_async_callback(self):
        err_cb = AsyncMock()
        sub = EventsStoreSubscription(
            channel="ch",
            events_store_type=EventsStoreType.StartNewOnly,
            on_receive_event_callback=MagicMock(),
            on_error_callback=err_cb,
        )
        await sub.raise_on_error_async("async error")
        err_cb.assert_called_once_with("async error")

    @pytest.mark.asyncio
    async def test_raise_on_error_async_no_callback(self):
        sub = EventsStoreSubscription(
            channel="ch",
            events_store_type=EventsStoreType.StartNewOnly,
            on_receive_event_callback=MagicMock(),
            on_error_callback=None,
        )
        await sub.raise_on_error_async("no handler")


class TestEventsStoreSubscriptionEncodeExtended:
    """Extended tests for EventsStoreSubscription encoding."""

    def test_encode_start_at_sequence(self):
        sub = EventsStoreSubscription(
            channel="ch",
            events_store_type=EventsStoreType.StartAtSequence,
            events_store_sequence_value=100,
            on_receive_event_callback=MagicMock(),
        )
        request = sub.encode("client-1")
        assert request.EventsStoreTypeValue == 100

    def test_encode_start_at_time(self):
        from datetime import datetime

        dt = datetime(2025, 6, 15, 12, 0, 0)
        sub = EventsStoreSubscription(
            channel="ch",
            events_store_type=EventsStoreType.StartAtTime,
            events_store_start_time=dt,
            on_receive_event_callback=MagicMock(),
        )
        request = sub.encode("client-1")
        assert request.EventsStoreTypeValue == int(dt.timestamp())

    def test_encode_start_at_time_delta(self):
        """GAP-C1: StartAtTimeDelta value must be encoded correctly."""
        sub = EventsStoreSubscription(
            channel="ch",
            events_store_type=EventsStoreType.StartAtTimeDelta,
            events_store_time_delta_seconds=300,
            on_receive_event_callback=MagicMock(),
        )
        request = sub.encode("client-1")
        assert request.EventsStoreTypeData == EventsStoreType.StartAtTimeDelta.value
        assert request.EventsStoreTypeValue == 300


class TestEventsStoreSubscriptionTimeDeltaValidation:
    """GAP-C1: Tests for StartAtTimeDelta validation."""

    def test_start_at_time_delta_with_zero_raises(self):
        with pytest.raises(ValueError, match="time delta value > 0"):
            EventsStoreSubscription(
                channel="ch",
                events_store_type=EventsStoreType.StartAtTimeDelta,
                events_store_time_delta_seconds=0,
                on_receive_event_callback=MagicMock(),
            )

    def test_start_at_time_delta_with_negative_raises(self):
        with pytest.raises(ValueError, match="time delta value > 0"):
            EventsStoreSubscription(
                channel="ch",
                events_store_type=EventsStoreType.StartAtTimeDelta,
                events_store_time_delta_seconds=-10,
                on_receive_event_callback=MagicMock(),
            )

    def test_start_at_time_delta_with_positive_value(self):
        sub = EventsStoreSubscription(
            channel="ch",
            events_store_type=EventsStoreType.StartAtTimeDelta,
            events_store_time_delta_seconds=60,
            on_receive_event_callback=MagicMock(),
        )
        assert sub.events_store_time_delta_seconds == 60


class TestEventsStoreSubscriptionWildcardValidation:
    """GAP-H1: Tests for wildcard rejection in EventsStore channels."""

    def test_channel_with_star_wildcard_raises(self):
        with pytest.raises(ValueError, match="wildcard"):
            EventsStoreSubscription(
                channel="events.*",
                events_store_type=EventsStoreType.StartNewOnly,
                on_receive_event_callback=MagicMock(),
            )

    def test_channel_with_gt_wildcard_raises(self):
        with pytest.raises(ValueError, match="wildcard"):
            EventsStoreSubscription(
                channel="events.>",
                events_store_type=EventsStoreType.StartNewOnly,
                on_receive_event_callback=MagicMock(),
            )

    def test_channel_with_whitespace_raises(self):
        with pytest.raises(ValueError, match="whitespace"):
            EventsStoreSubscription(
                channel="events channel",
                events_store_type=EventsStoreType.StartNewOnly,
                on_receive_event_callback=MagicMock(),
            )

    def test_channel_with_trailing_dot_raises(self):
        with pytest.raises(ValueError, match="end with"):
            EventsStoreSubscription(
                channel="events.",
                events_store_type=EventsStoreType.StartNewOnly,
                on_receive_event_callback=MagicMock(),
            )


class TestEventsStoreSubscriptionModelDumpExtended:
    """Extended tests for EventsStoreSubscription model_dump."""

    def test_model_dump_with_start_time(self):
        from datetime import datetime

        dt = datetime(2025, 6, 15, 12, 0, 0)
        sub = EventsStoreSubscription(
            channel="ch",
            events_store_type=EventsStoreType.StartAtTime,
            events_store_start_time=dt,
            on_receive_event_callback=MagicMock(),
        )
        dump = sub.model_dump()
        assert dump["events_store_start_time"] == dt.isoformat()
        assert "on_receive_event_callback" not in dump


class TestEventsSubscriptionCallbacksExtended:
    """Extended callback tests for EventsSubscription."""

    @pytest.mark.asyncio
    async def test_raise_on_receive_message_async_with_async_callback(self):
        callback = AsyncMock()
        sub = EventsSubscription(
            channel="ch",
            on_receive_event_callback=callback,
        )
        event = EventMessageReceived(id="m1", channel="ch", body=b"x")
        await sub.raise_on_receive_message_async(event)
        callback.assert_called_once_with(event)

    @pytest.mark.asyncio
    async def test_raise_on_error_async_with_async_callback(self):
        err_cb = AsyncMock()
        sub = EventsSubscription(
            channel="ch",
            on_receive_event_callback=MagicMock(),
            on_error_callback=err_cb,
        )
        await sub.raise_on_error_async("async err")
        err_cb.assert_called_once_with("async err")

    @pytest.mark.asyncio
    async def test_raise_on_error_async_with_sync_callback(self):
        err_cb = MagicMock()
        sub = EventsSubscription(
            channel="ch",
            on_receive_event_callback=MagicMock(),
            on_error_callback=err_cb,
        )
        await sub.raise_on_error_async("sync err via async")
        err_cb.assert_called_once_with("sync err via async")

    @pytest.mark.asyncio
    async def test_raise_on_error_async_no_callback(self):
        sub = EventsSubscription(
            channel="ch",
            on_receive_event_callback=MagicMock(),
            on_error_callback=None,
        )
        await sub.raise_on_error_async("no handler")


class TestEventsSubscriptionModelDumpExtended:
    """Extended model_dump tests for EventsSubscription."""

    def test_model_dump_contains_channel(self):
        sub = EventsSubscription(
            channel="test-ch",
            on_receive_event_callback=MagicMock(),
            on_error_callback=MagicMock(),
        )
        dump = sub.model_dump()
        assert dump["channel"] == "test-ch"
        assert "on_receive_event_callback" not in dump
        assert "on_error_callback" not in dump
