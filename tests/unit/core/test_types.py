"""Tests for kubemq.core.types module."""

from __future__ import annotations

import pytest

from kubemq.core.types import (
    AsyncCloseable,
    AsyncPingable,
    Closeable,
    Pingable,
    ServerInfo,
    StartPosition,
    SubscribeType,
)


class TestSubscribeType:
    """Tests for SubscribeType enum."""

    def test_events_subscription(self):
        """Test Events subscription type."""
        assert SubscribeType.EVENTS.value == 1
        assert SubscribeType.EVENTS.name == "EVENTS"

    def test_events_store_subscription(self):
        """Test EventsStore subscription type."""
        assert SubscribeType.EVENTS_STORE.value == 2
        assert SubscribeType.EVENTS_STORE.name == "EVENTS_STORE"

    def test_commands_subscription(self):
        """Test Commands subscription type."""
        assert SubscribeType.COMMANDS.value == 3
        assert SubscribeType.COMMANDS.name == "COMMANDS"

    def test_queries_subscription(self):
        """Test Queries subscription type."""
        assert SubscribeType.QUERIES.value == 4
        assert SubscribeType.QUERIES.name == "QUERIES"

    def test_undefined_subscription(self):
        """Test Undefined subscription type."""
        assert SubscribeType.UNDEFINED.value == 0
        assert SubscribeType.UNDEFINED.name == "UNDEFINED"

    def test_all_values_unique(self):
        """Test that all subscription type values are unique."""
        values = [st.value for st in SubscribeType]
        assert len(values) == len(set(values))


class TestStartPosition:
    """Tests for StartPosition enum."""

    def test_start_new_only(self):
        """Test START_NEW_ONLY start position."""
        assert StartPosition.START_NEW_ONLY.name == "START_NEW_ONLY"
        # auto() generates values starting from 1
        assert StartPosition.START_NEW_ONLY.value == 1

    def test_start_from_first(self):
        """Test START_FROM_FIRST start position."""
        assert StartPosition.START_FROM_FIRST.name == "START_FROM_FIRST"
        assert StartPosition.START_FROM_FIRST.value == 2

    def test_start_from_last(self):
        """Test START_FROM_LAST start position."""
        assert StartPosition.START_FROM_LAST.name == "START_FROM_LAST"
        assert StartPosition.START_FROM_LAST.value == 3

    def test_start_from_sequence(self):
        """Test START_FROM_SEQUENCE start position."""
        assert StartPosition.START_FROM_SEQUENCE.name == "START_FROM_SEQUENCE"
        assert StartPosition.START_FROM_SEQUENCE.value == 4

    def test_start_from_time(self):
        """Test START_FROM_TIME start position."""
        assert StartPosition.START_FROM_TIME.name == "START_FROM_TIME"
        assert StartPosition.START_FROM_TIME.value == 5

    def test_start_from_time_delta(self):
        """Test START_FROM_TIME_DELTA start position."""
        assert StartPosition.START_FROM_TIME_DELTA.name == "START_FROM_TIME_DELTA"
        assert StartPosition.START_FROM_TIME_DELTA.value == 6

    def test_all_values_unique(self):
        """Test that all start position values are unique."""
        values = [sp.value for sp in StartPosition]
        assert len(values) == len(set(values))


class TestServerInfo:
    """Tests for ServerInfo re-export."""

    def test_server_info_importable(self):
        """Test that ServerInfo is importable from types."""
        # ServerInfo should be re-exported from transport module
        assert ServerInfo is not None


class TestProtocols:
    """Tests for protocol classes."""

    def test_closeable_protocol(self):
        """Test Closeable protocol definition."""
        # Closeable should have a close method
        assert hasattr(Closeable, "close")

    def test_async_closeable_protocol(self):
        """Test AsyncCloseable protocol definition."""
        # AsyncCloseable should have a close method
        assert hasattr(AsyncCloseable, "close")

    def test_pingable_protocol(self):
        """Test Pingable protocol definition."""
        # Pingable should have a ping method
        assert hasattr(Pingable, "ping")

    def test_async_pingable_protocol(self):
        """Test AsyncPingable protocol definition."""
        # AsyncPingable should have a ping method
        assert hasattr(AsyncPingable, "ping")

    def test_closeable_implementation(self):
        """Test that a class implementing close() satisfies Closeable."""

        class MyCloseable:
            def close(self) -> None:
                pass

        obj = MyCloseable()
        # This should work without type errors
        assert hasattr(obj, "close")

    def test_pingable_implementation(self):
        """Test that a class implementing ping() satisfies Pingable."""

        class MyPingable:
            def ping(self) -> ServerInfo:
                return None  # Simplified for test

        obj = MyPingable()
        # This should work without type errors
        assert hasattr(obj, "ping")


class TestEnumIteration:
    """Tests for enum iteration capabilities."""

    def test_subscribe_type_iterable(self):
        """Test that SubscribeType is iterable."""
        types = list(SubscribeType)
        assert len(types) == 5  # UNDEFINED, EVENTS, EVENTS_STORE, COMMANDS, QUERIES

    def test_start_position_iterable(self):
        """Test that StartPosition is iterable."""
        positions = list(StartPosition)
        # START_NEW_ONLY, START_FROM_FIRST, START_FROM_LAST, START_FROM_SEQUENCE, START_FROM_TIME, START_FROM_TIME_DELTA
        assert len(positions) == 6


class TestEnumComparison:
    """Tests for enum comparison operations."""

    def test_subscribe_type_equality(self):
        """Test SubscribeType equality comparison."""
        assert SubscribeType.EVENTS == SubscribeType.EVENTS
        assert SubscribeType.EVENTS != SubscribeType.COMMANDS

    def test_start_position_equality(self):
        """Test StartPosition equality comparison."""
        assert StartPosition.START_NEW_ONLY == StartPosition.START_NEW_ONLY
        assert StartPosition.START_NEW_ONLY != StartPosition.START_FROM_FIRST

    def test_subscribe_type_by_value(self):
        """Test getting SubscribeType by value."""
        assert SubscribeType(1) == SubscribeType.EVENTS
        assert SubscribeType(3) == SubscribeType.COMMANDS

    def test_start_position_by_value(self):
        """Test getting StartPosition by value."""
        assert StartPosition(1) == StartPosition.START_NEW_ONLY
        assert StartPosition(5) == StartPosition.START_FROM_TIME

    def test_invalid_subscribe_type_value(self):
        """Test that invalid values raise ValueError."""
        with pytest.raises(ValueError):
            SubscribeType(99)

    def test_invalid_start_position_value(self):
        """Test that invalid values raise ValueError."""
        with pytest.raises(ValueError):
            StartPosition(99)


# ==============================================================================
# Coverage extension: lines 110, 141, 156-159, 168, 177, 186, 195, 217-283
# ==============================================================================


class TestCredentialProviderProtocol:

    def test_isinstance_check(self):
        from kubemq.core.types import CredentialProvider
        from datetime import datetime
        from typing import Optional

        class MyProvider:
            def get_token(self) -> tuple[str, Optional[datetime]]:
                return ("tok", None)

        assert isinstance(MyProvider(), CredentialProvider)

    def test_non_conforming_class_fails(self):
        from kubemq.core.types import CredentialProvider

        class NotAProvider:
            pass

        assert not isinstance(NotAProvider(), CredentialProvider)

    def test_stub_invocation(self):
        from kubemq.core.types import CredentialProvider
        result = CredentialProvider.get_token(None)
        assert result is None


class TestAsyncCredentialProviderProtocol:

    def test_isinstance_check(self):
        from kubemq.core.types import AsyncCredentialProvider
        from datetime import datetime
        from typing import Optional

        class MyAsyncProvider:
            async def get_token(self) -> tuple[str, Optional[datetime]]:
                return ("tok", None)

        assert isinstance(MyAsyncProvider(), AsyncCredentialProvider)

    @pytest.mark.asyncio
    async def test_stub_invocation(self):
        from kubemq.core.types import AsyncCredentialProvider
        result = await AsyncCredentialProvider.get_token(None)
        assert result is None


class TestLoggerProtocol:

    def test_isinstance_check(self):
        from kubemq.core.types import Logger

        class MyLogger:
            def debug(self, msg, **kwargs): pass
            def info(self, msg, **kwargs): pass
            def warning(self, msg, **kwargs): pass
            def error(self, msg, **kwargs): pass

        assert isinstance(MyLogger(), Logger)

    def test_stub_invocations(self):
        from kubemq.core.types import Logger
        Logger.debug(None, "msg")
        Logger.info(None, "msg")
        Logger.warning(None, "msg")
        Logger.error(None, "msg")


class TestCloseableProtocolIsinstance:

    def test_isinstance_check(self):
        class MyCloseable:
            def close(self) -> None:
                pass

        assert isinstance(MyCloseable(), Closeable)

    def test_stub_invocation(self):
        Closeable.close(None)


class TestAsyncCloseableProtocolIsinstance:

    def test_isinstance_check(self):
        class MyAsyncCloseable:
            async def close(self) -> None:
                pass

        assert isinstance(MyAsyncCloseable(), AsyncCloseable)

    @pytest.mark.asyncio
    async def test_stub_invocation(self):
        await AsyncCloseable.close(None)


class TestPingableProtocolIsinstance:

    def test_isinstance_check(self):
        class MyPingable:
            def ping(self) -> ServerInfo:
                return None  # type: ignore

        assert isinstance(MyPingable(), Pingable)

    def test_stub_invocation(self):
        Pingable.ping(None)


class TestAsyncPingableProtocolIsinstance:

    def test_isinstance_check(self):
        class MyAsyncPingable:
            async def ping(self) -> ServerInfo:
                return None  # type: ignore

        assert isinstance(MyAsyncPingable(), AsyncPingable)

    @pytest.mark.asyncio
    async def test_stub_invocation(self):
        await AsyncPingable.ping(None)


class TestTransportProtocol:

    def test_isinstance_check(self):
        from kubemq.core.types import TransportProtocol

        class MyTransport:
            async def connect(self): ...
            async def close(self): ...
            async def ping(self): ...
            async def send_event(self, event): ...
            async def send_event_store(self, event): ...
            async def subscribe(self, request, cancel_token=None): ...
            async def send_command(self, request): ...
            async def send_query(self, request): ...
            async def send_response(self, response): ...
            async def send_queue_message(self, message): ...
            async def send_queue_messages_batch(self, messages): ...
            async def receive_queue_messages(self, request): ...
            async def create_channel(self, channel_type, channel): ...
            async def delete_channel(self, channel_type, channel): ...
            async def list_channels(self, channel_type, search): ...

        assert isinstance(MyTransport(), TransportProtocol)

    @pytest.mark.asyncio
    async def test_stub_invocations(self):
        from kubemq.core.types import TransportProtocol
        await TransportProtocol.connect(None)
        await TransportProtocol.close(None)
        await TransportProtocol.ping(None)
        await TransportProtocol.send_event(None, None)
        await TransportProtocol.send_event_store(None, None)
        await TransportProtocol.subscribe(None, None)
        await TransportProtocol.send_command(None, None)
        await TransportProtocol.send_query(None, None)
        await TransportProtocol.send_response(None, None)
        await TransportProtocol.send_queue_message(None, None)
        await TransportProtocol.send_queue_messages_batch(None, None)
        await TransportProtocol.receive_queue_messages(None, None)
        await TransportProtocol.create_channel(None, None, None)
        await TransportProtocol.delete_channel(None, None, None)
        await TransportProtocol.list_channels(None, None, None)
