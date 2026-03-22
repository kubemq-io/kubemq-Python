"""
Characterization tests for Queues client behavior.

These tests capture the CURRENT behavior of the SDK before refactoring.
They must pass both before AND after any refactoring to ensure
backward compatibility.
"""

import pytest

from kubemq.queues import Client, QueueMessage


@pytest.mark.characterization
class TestQueueMessageBehavior:
    """Characterization tests for QueueMessage class."""

    def test_queue_message_requires_channel(self):
        """Capture: QueueMessage raises ValueError without channel."""
        with pytest.raises(ValueError, match="channel"):
            QueueMessage(body=b"test")

    def test_queue_message_requires_body(self):
        """Capture: QueueMessage requires body."""
        with pytest.raises(ValueError, match="body"):
            QueueMessage(channel="test-channel")

    def test_queue_message_accepts_valid_parameters(self):
        """Capture: QueueMessage accepts channel + body as minimum."""
        msg = QueueMessage(channel="test-channel", body=b"test body")
        assert msg.channel == "test-channel"
        assert msg.body == b"test body"

    def test_queue_message_has_optional_id(self):
        """Capture: QueueMessage id defaults to None, generated on encode."""
        msg = QueueMessage(channel="test", body=b"test")
        # id defaults to None (generated during encode)
        assert msg.id is None or len(msg.id) > 0

    def test_queue_message_custom_id(self):
        """Capture: QueueMessage accepts custom ID."""
        msg = QueueMessage(channel="test", body=b"test", id="custom-id-123")
        assert msg.id == "custom-id-123"

    def test_queue_message_accepts_tags(self):
        """Capture: QueueMessage accepts tags."""
        msg = QueueMessage(channel="test", body=b"test", tags={"key": "value"})
        assert msg.tags == {"key": "value"}

    def test_queue_message_accepts_metadata(self):
        """Capture: QueueMessage accepts metadata."""
        msg = QueueMessage(channel="test", body=b"test", metadata="test-metadata")
        assert msg.metadata == "test-metadata"

    def test_queue_message_accepts_policy(self):
        """Capture: QueueMessage accepts policy fields.

        Note: In v3.x, the fields are named:
        - delay_in_seconds (not policy_delay_seconds)
        - expiration_in_seconds (not policy_expiration_seconds)
        - max_receive_count (not policy_max_receive_count)
        """
        msg = QueueMessage(
            channel="test",
            body=b"test",
            expiration_in_seconds=60,
            delay_in_seconds=10,
            max_receive_count=3,
            max_receive_queue="test-dlq",  # Required when attempts > 0
        )
        assert msg.expiration_in_seconds == 60
        assert msg.delay_in_seconds == 10
        assert msg.max_receive_count == 3


@pytest.mark.characterization
class TestQueuesClientInitializationBehavior:
    """Characterization tests for Queues Client initialization."""

    def test_client_defaults_address_when_empty(self):
        """Capture: Client defaults empty address to localhost:50000.

        Changed from raising ValueError — address now defaults per GS spec.
        """
        client = Client(address="")
        assert client._config.address == "localhost:50000"

    def test_client_accepts_send_timeout(self):
        """Capture: Client accepts send_timeout parameter."""
        # Can't fully test without server, but verify the parameter exists
        # in the signature by checking the class
        import inspect

        sig = inspect.signature(Client.__init__)
        assert "send_timeout" in sig.parameters

    def test_client_accepts_connection_monitor_interval(self):
        """Capture: Client accepts connection_monitor_interval parameter."""
        import inspect

        sig = inspect.signature(Client.__init__)
        assert "connection_monitor_interval" in sig.parameters


@pytest.mark.characterization
class TestQueuesClientMethodSignatures:
    """Characterization tests for Queues Client method signatures."""

    def test_receive_queues_messages_signature(self):
        """Capture: receive_queues_messages has specific parameters."""
        import inspect

        sig = inspect.signature(Client.receive_queues_messages)
        params = list(sig.parameters.keys())

        # Expected parameters
        assert "channel" in params
        assert "max_messages" in params
        assert "wait_timeout_in_seconds" in params
        assert "auto_ack" in params

    def test_peek_queue_messages_method_exists(self):
        """Capture: Client has peek_queue_messages() method."""
        assert hasattr(Client, "peek_queue_messages")
        assert callable(Client.peek_queue_messages)

    def test_pull_method_exists(self):
        """Capture: Client has pull() method."""
        assert hasattr(Client, "pull")
        assert callable(Client.pull)

    def test_async_methods_exist(self):
        """Capture: Client has async versions of all methods."""
        async_methods = [
            "ping_async",
            "send_queues_message_async",
            "create_queues_channel_async",
            "delete_queues_channel_async",
            "list_queues_channels_async",
            "receive_queues_messages_async",
            "peek_queue_messages_async",
            "pull_async",
            "close_async",
        ]
        for method in async_methods:
            assert hasattr(Client, method), f"Missing async method: {method}"
            assert callable(getattr(Client, method))
