"""Unit tests for kubemq.transport.channel_manager module.

Tests for ChannelManager and ConnectionState classes.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest

from kubemq.transport.channel_manager import ChannelManager, ConnectionState
from kubemq.transport.connection import Connection


class TestConnectionState:
    """Tests for ConnectionState class."""

    def test_initial_state_is_connected(self):
        """Test that initial state is connected."""
        state = ConnectionState()
        assert state.is_connected is True

    def test_is_accepting_requests_returns_true_when_connected(self):
        """Test is_accepting_requests returns True when connected."""
        state = ConnectionState()
        assert state.is_accepting_requests() is True

    def test_is_accepting_requests_returns_false_when_disconnected(self):
        """Test is_accepting_requests returns False when disconnected."""
        state = ConnectionState()
        state.set_connected(False)
        assert state.is_accepting_requests() is False

    def test_set_connected_returns_true_on_state_change(self):
        """Test set_connected returns True when state changes."""
        state = ConnectionState()
        changed = state.set_connected(False)
        assert changed is True

    def test_set_connected_returns_false_when_no_change(self):
        """Test set_connected returns False when state doesn't change."""
        state = ConnectionState()
        changed = state.set_connected(True)  # Same as initial
        assert changed is False

    def test_set_connected_is_thread_safe(self):
        """Test set_connected uses lock for thread safety."""
        state = ConnectionState()
        # Verify lock exists
        assert state.lock is not None

        # Should not raise when called multiple times
        state.set_connected(False)
        state.set_connected(True)
        state.set_connected(False)


class TestChannelManagerInit:
    """Tests for ChannelManager initialization."""

    @patch("kubemq.transport.channel_manager.grpc.insecure_channel")
    @patch("kubemq.transport.channel_manager.grpc.intercept_channel")
    @patch("kubemq.transport.channel_manager.kubemq_pb2_grpc.kubemqStub")
    def test_init_creates_channel_manager(self, mock_stub, mock_intercept, mock_insecure):
        """Test that initialization creates channel and stub."""
        mock_channel = MagicMock()
        mock_insecure.return_value = mock_channel
        mock_intercept.return_value = mock_channel

        mock_client = MagicMock()
        mock_client.Ping.return_value = MagicMock()
        mock_stub.return_value = mock_client

        connection = Connection(
            address="localhost:50000",
            client_id="test-client",
        )
        logger = logging.getLogger("test")

        manager = ChannelManager(connection, logger)

        assert manager._client is not None
        assert manager._channel is not None
        assert manager.connection_state.is_connected is True

    @patch("kubemq.transport.channel_manager.grpc.insecure_channel")
    @patch("kubemq.transport.channel_manager.grpc.intercept_channel")
    @patch("kubemq.transport.channel_manager.kubemq_pb2_grpc.kubemqStub")
    def test_init_fails_on_connection_error(self, mock_stub, mock_intercept, mock_insecure):
        """Test that initialization fails on connection error."""
        mock_insecure.side_effect = Exception("Connection failed")

        connection = Connection(
            address="localhost:50000",
            client_id="test-client",
        )
        logger = logging.getLogger("test")

        with pytest.raises(Exception, match="Connection failed"):
            ChannelManager(connection, logger)


class TestChannelManagerOperations:
    """Tests for ChannelManager operations."""

    @patch("kubemq.transport.channel_manager.grpc.insecure_channel")
    @patch("kubemq.transport.channel_manager.grpc.intercept_channel")
    @patch("kubemq.transport.channel_manager.kubemq_pb2_grpc.kubemqStub")
    def test_get_client_returns_stub(self, mock_stub, mock_intercept, mock_insecure):
        """Test get_client returns the gRPC stub."""
        mock_channel = MagicMock()
        mock_insecure.return_value = mock_channel
        mock_intercept.return_value = mock_channel

        mock_client = MagicMock()
        mock_stub.return_value = mock_client

        connection = Connection(
            address="localhost:50000",
            client_id="test-client",
        )
        logger = logging.getLogger("test")

        manager = ChannelManager(connection, logger)
        client = manager.get_client()

        assert client is not None

    @patch("kubemq.transport.channel_manager.grpc.insecure_channel")
    @patch("kubemq.transport.channel_manager.grpc.intercept_channel")
    @patch("kubemq.transport.channel_manager.kubemq_pb2_grpc.kubemqStub")
    def test_register_client(self, mock_stub, mock_intercept, mock_insecure):
        """Test registering a client reference."""
        mock_channel = MagicMock()
        mock_insecure.return_value = mock_channel
        mock_intercept.return_value = mock_channel
        mock_stub.return_value = MagicMock()

        connection = Connection(
            address="localhost:50000",
            client_id="test-client",
        )
        logger = logging.getLogger("test")

        manager = ChannelManager(connection, logger)
        client_ref = MagicMock()

        manager.register_client(client_ref)

        assert client_ref in manager._registered_clients

    @patch("kubemq.transport.channel_manager.grpc.insecure_channel")
    @patch("kubemq.transport.channel_manager.grpc.intercept_channel")
    @patch("kubemq.transport.channel_manager.kubemq_pb2_grpc.kubemqStub")
    def test_is_channel_healthy_returns_true(self, mock_stub, mock_intercept, mock_insecure):
        """Test is_channel_healthy returns True when connection succeeds."""
        mock_channel = MagicMock()
        mock_insecure.return_value = mock_channel
        mock_intercept.return_value = mock_channel

        mock_client = MagicMock()
        mock_client.Ping.return_value = MagicMock()
        mock_stub.return_value = mock_client

        connection = Connection(
            address="localhost:50000",
            client_id="test-client",
        )
        logger = logging.getLogger("test")

        manager = ChannelManager(connection, logger)

        assert manager.is_channel_healthy() is True

    @patch("kubemq.transport.channel_manager.grpc.insecure_channel")
    @patch("kubemq.transport.channel_manager.grpc.intercept_channel")
    @patch("kubemq.transport.channel_manager.kubemq_pb2_grpc.kubemqStub")
    def test_is_channel_healthy_returns_false_on_error(
        self, mock_stub, mock_intercept, mock_insecure
    ):
        """Test is_channel_healthy returns False when connection fails."""
        mock_channel = MagicMock()
        mock_insecure.return_value = mock_channel
        mock_intercept.return_value = mock_channel

        mock_client = MagicMock()
        # First call succeeds (init), subsequent calls fail
        mock_client.Ping.side_effect = [MagicMock(), Exception("Connection lost")]
        mock_stub.return_value = mock_client

        connection = Connection(
            address="localhost:50000",
            client_id="test-client",
        )
        logger = logging.getLogger("test")

        manager = ChannelManager(connection, logger)

        assert manager.is_channel_healthy() is False


class TestChannelManagerClose:
    """Tests for ChannelManager close operation."""

    @patch("kubemq.transport.channel_manager.grpc.insecure_channel")
    @patch("kubemq.transport.channel_manager.grpc.intercept_channel")
    @patch("kubemq.transport.channel_manager.kubemq_pb2_grpc.kubemqStub")
    def test_close_closes_channel(self, mock_stub, mock_intercept, mock_insecure):
        """Test close properly closes the channel."""
        mock_channel = MagicMock()
        mock_insecure.return_value = mock_channel
        mock_intercept.return_value = mock_channel
        mock_stub.return_value = MagicMock()

        connection = Connection(
            address="localhost:50000",
            client_id="test-client",
        )
        logger = logging.getLogger("test")

        manager = ChannelManager(connection, logger)
        manager.close()

        mock_channel.close.assert_called()
        assert manager._channel is None
        assert manager._client is None
        assert manager.connection_state.is_connected is False

    @patch("kubemq.transport.channel_manager.grpc.insecure_channel")
    @patch("kubemq.transport.channel_manager.grpc.intercept_channel")
    @patch("kubemq.transport.channel_manager.kubemq_pb2_grpc.kubemqStub")
    def test_close_handles_error(self, mock_stub, mock_intercept, mock_insecure):
        """Test close handles errors gracefully."""
        mock_channel = MagicMock()
        mock_channel.close.side_effect = Exception("Close error")
        mock_insecure.return_value = mock_channel
        mock_intercept.return_value = mock_channel
        mock_stub.return_value = MagicMock()

        connection = Connection(
            address="localhost:50000",
            client_id="test-client",
        )
        logger = logging.getLogger("test")

        manager = ChannelManager(connection, logger)

        # Should not raise
        manager.close()

        assert manager._channel is None
        assert manager.connection_state.is_connected is False


class TestChannelManagerRecreate:
    """Tests for ChannelManager channel recreation."""

    @patch("kubemq.transport.channel_manager.time.sleep")
    @patch("kubemq.transport.channel_manager.grpc.insecure_channel")
    @patch("kubemq.transport.channel_manager.grpc.intercept_channel")
    @patch("kubemq.transport.channel_manager.kubemq_pb2_grpc.kubemqStub")
    def test_recreate_channel_raises_when_auto_reconnect_disabled(
        self, mock_stub, mock_intercept, mock_insecure, mock_sleep
    ):
        """Test recreate_channel raises when auto_reconnect is disabled."""
        mock_channel = MagicMock()
        mock_insecure.return_value = mock_channel
        mock_intercept.return_value = mock_channel
        mock_stub.return_value = MagicMock()

        connection = Connection(
            address="localhost:50000",
            client_id="test-client",
            disable_auto_reconnect=True,
        )
        logger = logging.getLogger("test")

        manager = ChannelManager(connection, logger)

        with pytest.raises(ConnectionError, match="Auto-reconnect is disabled"):
            manager.recreate_channel()

    @patch("kubemq.transport.channel_manager.time.sleep")
    @patch("kubemq.transport.channel_manager.grpc.insecure_channel")
    @patch("kubemq.transport.channel_manager.grpc.intercept_channel")
    @patch("kubemq.transport.channel_manager.kubemq_pb2_grpc.kubemqStub")
    def test_recreate_channel_success(self, mock_stub, mock_intercept, mock_insecure, mock_sleep):
        """Test successful channel recreation."""
        mock_channel = MagicMock()
        mock_insecure.return_value = mock_channel
        mock_intercept.return_value = mock_channel

        mock_client = MagicMock()
        mock_client.Ping.return_value = MagicMock()
        mock_stub.return_value = mock_client

        connection = Connection(
            address="localhost:50000",
            client_id="test-client",
            disable_auto_reconnect=False,
            reconnect_interval_seconds=0,
        )
        logger = logging.getLogger("test")

        manager = ChannelManager(connection, logger)
        new_client = manager.recreate_channel()

        assert new_client is not None
        assert manager.connection_state.is_connected is True
