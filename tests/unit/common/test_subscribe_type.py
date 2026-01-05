"""Unit tests for kubemq.common.subscribe_type module.

Tests for SubscribeType enum.
"""

from __future__ import annotations

from kubemq.common.subscribe_type import SubscribeType


class TestSubscribeType:
    """Tests for SubscribeType enum."""

    def test_undefined_value(self):
        """Test SubscribeTypeUndefined value."""
        assert SubscribeType.SubscribeTypeUndefined.value == 0

    def test_events_value(self):
        """Test Events value."""
        assert SubscribeType.Events.value == 1

    def test_events_store_value(self):
        """Test EventsStore value."""
        assert SubscribeType.EventsStore.value == 2

    def test_commands_value(self):
        """Test Commands value."""
        assert SubscribeType.Commands.value == 3

    def test_queries_value(self):
        """Test Queries value."""
        assert SubscribeType.Queries.value == 4

    def test_all_values_unique(self):
        """Test all enum values are unique."""
        values = [member.value for member in SubscribeType]

        assert len(values) == len(set(values))

    def test_can_access_by_value(self):
        """Test enum can be accessed by value."""
        assert SubscribeType(1) == SubscribeType.Events
        assert SubscribeType(2) == SubscribeType.EventsStore
        assert SubscribeType(3) == SubscribeType.Commands
        assert SubscribeType(4) == SubscribeType.Queries

    def test_can_compare_by_value(self):
        """Test enum values can be compared."""
        assert SubscribeType.Events.value < SubscribeType.EventsStore.value
        assert SubscribeType.Commands.value < SubscribeType.Queries.value
