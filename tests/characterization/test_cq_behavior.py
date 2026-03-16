"""
Characterization tests for CQ (Commands/Queries) client behavior.

These tests capture the CURRENT behavior of the SDK before refactoring.
They must pass both before AND after any refactoring to ensure
backward compatibility.
"""

import pytest

from kubemq.cq import (
    Client,
    CommandMessage,
    CommandsSubscription,
    QueriesSubscription,
    QueryMessage,
)


@pytest.mark.characterization
class TestCommandMessageBehavior:
    """Characterization tests for CommandMessage class."""

    def test_command_message_requires_channel(self):
        """Capture: CommandMessage raises ValueError without channel."""
        with pytest.raises(ValueError, match="channel"):
            CommandMessage(body=b"test")

    def test_command_message_requires_timeout(self):
        """Capture: CommandMessage requires timeout_in_seconds (no default)."""
        with pytest.raises(Exception):  # Pydantic ValidationError
            CommandMessage(channel="test-channel", body=b"test body")

    def test_command_message_accepts_valid_parameters(self):
        """Capture: CommandMessage accepts channel + body + timeout as minimum."""
        msg = CommandMessage(channel="test-channel", body=b"test body", timeout_in_seconds=30)
        assert msg.channel == "test-channel"
        assert msg.body == b"test body"

    def test_command_message_has_default_id(self):
        """Capture: CommandMessage generates a default ID if not provided."""
        msg = CommandMessage(channel="test", body=b"test", timeout_in_seconds=30)
        assert msg.id is not None
        assert len(msg.id) > 0

    def test_command_message_accepts_timeout(self):
        """Capture: CommandMessage accepts timeout_in_seconds."""
        msg = CommandMessage(channel="test", body=b"test", timeout_in_seconds=30)
        assert msg.timeout_in_seconds == 30

    def test_command_message_timeout_must_be_positive(self):
        """Capture: CommandMessage timeout must be greater than 0."""
        with pytest.raises(Exception):  # Pydantic ValidationError for gt=0
            CommandMessage(channel="test", body=b"test", timeout_in_seconds=0)


@pytest.mark.characterization
class TestQueryMessageBehavior:
    """Characterization tests for QueryMessage class."""

    def test_query_message_requires_channel(self):
        """Capture: QueryMessage raises ValueError without channel."""
        with pytest.raises(ValueError, match="channel"):
            QueryMessage(body=b"test")

    def test_query_message_requires_timeout(self):
        """Capture: QueryMessage requires timeout_in_seconds (no default)."""
        with pytest.raises(Exception):  # Pydantic ValidationError
            QueryMessage(channel="test-channel", body=b"test body")

    def test_query_message_accepts_valid_parameters(self):
        """Capture: QueryMessage accepts channel + body + timeout as minimum."""
        msg = QueryMessage(channel="test-channel", body=b"test body", timeout_in_seconds=30)
        assert msg.channel == "test-channel"
        assert msg.body == b"test body"

    def test_query_message_accepts_timeout(self):
        """Capture: QueryMessage accepts timeout_in_seconds."""
        msg = QueryMessage(channel="test", body=b"test", timeout_in_seconds=30)
        assert msg.timeout_in_seconds == 30


@pytest.mark.characterization
class TestCommandsSubscriptionBehavior:
    """Characterization tests for CommandsSubscription class."""

    def test_subscription_requires_channel(self):
        """Capture: CommandsSubscription requires channel."""
        with pytest.raises(ValueError, match="channel"):
            CommandsSubscription(channel="")

    def test_subscription_requires_callback(self):
        """Capture: CommandsSubscription requires on_receive_command_callback."""
        with pytest.raises(ValueError):
            CommandsSubscription(channel="test-channel")

    def test_subscription_accepts_valid_parameters(self):
        """Capture: CommandsSubscription accepts valid channel and callback."""

        def callback(msg):
            return None

        sub = CommandsSubscription(
            channel="test-channel",
            on_receive_command_callback=callback,
        )
        assert sub.channel == "test-channel"


@pytest.mark.characterization
class TestQueriesSubscriptionBehavior:
    """Characterization tests for QueriesSubscription class."""

    def test_subscription_requires_channel(self):
        """Capture: QueriesSubscription requires channel."""
        with pytest.raises(ValueError, match="channel"):
            QueriesSubscription(channel="")

    def test_subscription_requires_callback(self):
        """Capture: QueriesSubscription requires on_receive_query_callback."""
        with pytest.raises(ValueError):
            QueriesSubscription(channel="test-channel")

    def test_subscription_accepts_valid_parameters(self):
        """Capture: QueriesSubscription accepts valid channel and callback."""

        def callback(msg):
            return None

        sub = QueriesSubscription(
            channel="test-channel",
            on_receive_query_callback=callback,
        )
        assert sub.channel == "test-channel"


@pytest.mark.characterization
class TestCQClientInitializationBehavior:
    """Characterization tests for CQ Client initialization."""

    def test_client_defaults_address_when_empty(self):
        """Capture: Client defaults empty address to localhost:50000.

        Changed from raising ValueError — address now defaults per GS spec.
        """
        client = Client(address="")
        assert client._config.address == "localhost:50000"


@pytest.mark.characterization
class TestCQClientMethodSignatures:
    """Characterization tests for CQ Client method signatures."""

    def test_send_command_request_exists(self):
        """Capture: Client has send_command_request method."""
        assert hasattr(Client, "send_command_request")
        assert callable(Client.send_command_request)

    def test_send_query_request_exists(self):
        """Capture: Client has send_query_request method."""
        assert hasattr(Client, "send_query_request")
        assert callable(Client.send_query_request)

    def test_send_response_message_exists(self):
        """Capture: Client has send_response_message method."""
        assert hasattr(Client, "send_response_message")
        assert callable(Client.send_response_message)

    def test_async_methods_exist(self):
        """Capture: Client has async versions of all methods."""
        async_methods = [
            "ping_async",
            "send_command_request_async",
            "send_query_request_async",
            "send_response_message_async",
            "create_commands_channel_async",
            "create_queries_channel_async",
            "delete_commands_channel_async",
            "delete_queries_channel_async",
            "list_commands_channels_async",
            "list_queries_channels_async",
            "subscribe_to_commands_async",
            "subscribe_to_queries_async",
            "close_async",
        ]
        for method in async_methods:
            assert hasattr(Client, method), f"Missing async method: {method}"
            assert callable(getattr(Client, method))
