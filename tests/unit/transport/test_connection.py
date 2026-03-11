"""Unit tests for kubemq.transport.connection module.

Tests for Connection class.
"""

from __future__ import annotations

import pytest

from kubemq.transport.connection import Connection
from kubemq.transport.keep_alive import KeepAliveConfig
from kubemq.transport.tls_config import TlsConfig


class TestConnectionValidation:
    """Tests for Connection validation."""

    def test_requires_address(self):
        """Test that address is required."""
        with pytest.raises(ValueError, match="must have an address"):
            Connection(
                address="",
                client_id="test-client",
            )

    def test_requires_client_id(self):
        """Test that client_id is required."""
        with pytest.raises(ValueError, match="must have a client_id"):
            Connection(
                address="localhost:50000",
                client_id="",
            )

    def test_valid_connection(self):
        """Test valid connection creation."""
        conn = Connection(
            address="localhost:50000",
            client_id="test-client",
        )

        assert conn.address == "localhost:50000"
        assert conn.client_id == "test-client"

    def test_default_values(self):
        """Test default values are set correctly."""
        conn = Connection(
            address="localhost:50000",
            client_id="test-client",
        )

        assert conn.auth_token == ""
        assert conn.max_send_size == Connection.DEFAULT_MAX_SEND_SIZE
        assert conn.max_receive_size == Connection.DEFAULT_MAX_RCV_SIZE
        assert conn.disable_auto_reconnect is False
        assert conn.reconnect_interval_seconds == Connection.DEFAULT_RECONNECT_INTERVAL_SECONDS
        assert conn.log_level is None

    def test_custom_values(self):
        """Test custom values are set correctly."""
        conn = Connection(
            address="localhost:50000",
            client_id="test-client",
            auth_token="my-token",
            max_send_size=1000,
            max_receive_size=2000,
            disable_auto_reconnect=True,
            reconnect_interval_seconds=5,
        )

        assert conn.auth_token == "my-token"
        assert conn.max_send_size == 1000
        assert conn.max_receive_size == 2000
        assert conn.disable_auto_reconnect is True
        assert conn.reconnect_interval_seconds == 5


class TestConnectionPositiveValidation:
    """Tests for Connection positive value validation."""

    def test_max_send_size_must_be_positive(self):
        """Test max_send_size must be >= 0."""
        with pytest.raises(ValueError, match="must be greater than or equal to 0"):
            Connection(
                address="localhost:50000",
                client_id="test-client",
                max_send_size=-1,
            )

    def test_max_receive_size_must_be_positive(self):
        """Test max_receive_size must be >= 0."""
        with pytest.raises(ValueError, match="must be greater than or equal to 0"):
            Connection(
                address="localhost:50000",
                client_id="test-client",
                max_receive_size=-1,
            )

    def test_reconnect_interval_must_be_positive(self):
        """Test reconnect_interval_seconds must be >= 0."""
        with pytest.raises(ValueError, match="must be greater than or equal to 0"):
            Connection(
                address="localhost:50000",
                client_id="test-client",
                reconnect_interval_seconds=-1,
            )

    def test_zero_values_allowed(self):
        """Test zero values are allowed for size fields."""
        conn = Connection(
            address="localhost:50000",
            client_id="test-client",
            max_send_size=0,
            max_receive_size=0,
            reconnect_interval_seconds=0,
        )

        assert conn.max_send_size == 0
        assert conn.max_receive_size == 0
        assert conn.reconnect_interval_seconds == 0


class TestConnectionComplete:
    """Tests for Connection complete method."""

    def test_complete_fills_zero_max_send_size(self):
        """Test complete fills default for zero max_send_size."""
        conn = Connection(
            address="localhost:50000",
            client_id="test-client",
            max_send_size=0,
        )

        conn.complete()

        assert conn.max_send_size == Connection.DEFAULT_MAX_SEND_SIZE

    def test_complete_fills_zero_max_receive_size(self):
        """Test complete fills default for zero max_receive_size."""
        conn = Connection(
            address="localhost:50000",
            client_id="test-client",
            max_receive_size=0,
        )

        conn.complete()

        assert conn.max_receive_size == Connection.DEFAULT_MAX_RCV_SIZE

    def test_complete_fills_zero_reconnect_interval(self):
        """Test complete fills default for zero reconnect_interval."""
        conn = Connection(
            address="localhost:50000",
            client_id="test-client",
            reconnect_interval_seconds=0,
        )

        conn.complete()

        assert conn.reconnect_interval_seconds == Connection.DEFAULT_RECONNECT_INTERVAL_SECONDS

    def test_complete_preserves_non_zero_values(self):
        """Test complete preserves non-zero values."""
        conn = Connection(
            address="localhost:50000",
            client_id="test-client",
            max_send_size=500,
            max_receive_size=600,
            reconnect_interval_seconds=10,
        )

        conn.complete()

        assert conn.max_send_size == 500
        assert conn.max_receive_size == 600
        assert conn.reconnect_interval_seconds == 10

    def test_complete_returns_self(self):
        """Test complete returns self for chaining."""
        conn = Connection(
            address="localhost:50000",
            client_id="test-client",
        )

        result = conn.complete()

        assert result is conn


class TestConnectionMethods:
    """Tests for Connection methods."""

    def test_get_reconnect_delay(self):
        """Test get_reconnect_delay returns reconnect_interval_seconds."""
        conn = Connection(
            address="localhost:50000",
            client_id="test-client",
            reconnect_interval_seconds=5,
        )

        assert conn.get_reconnect_delay() == 5

    def test_set_log_level(self):
        """Test set_log_level sets log level."""
        conn = Connection(
            address="localhost:50000",
            client_id="test-client",
        )

        conn.set_log_level(20)

        assert conn.log_level == 20

    def test_set_log_level_returns_self(self):
        """Test set_log_level returns self for chaining."""
        conn = Connection(
            address="localhost:50000",
            client_id="test-client",
        )

        result = conn.set_log_level(10)

        assert result is conn


class TestConnectionWithTlsConfig:
    """Tests for Connection with TlsConfig."""

    def test_default_tls_config(self):
        """Test default TLS config is created."""
        conn = Connection(
            address="localhost:50000",
            client_id="test-client",
        )

        assert conn.tls is not None
        assert conn.tls.enabled is False

    def test_custom_tls_config_disabled(self):
        """Test custom TLS config can be set (disabled)."""
        tls = TlsConfig(enabled=False)
        conn = Connection(
            address="localhost:50000",
            client_id="test-client",
            tls=tls,
        )

        assert conn.tls.enabled is False


class TestConnectionWithKeepAliveConfig:
    """Tests for Connection with KeepAliveConfig."""

    def test_default_keep_alive_config(self):
        """Test default keep alive config is GS-compliant (enabled)."""
        conn = Connection(
            address="localhost:50000",
            client_id="test-client",
        )

        assert conn.keep_alive is not None
        assert conn.keep_alive.enabled is True
        assert conn.keep_alive.ping_interval_in_seconds == 10
        assert conn.keep_alive.ping_timeout_in_seconds == 5

    def test_custom_keep_alive_config(self):
        """Test custom keep alive config can be set."""
        keep_alive = KeepAliveConfig(
            enabled=True,
            ping_interval_in_seconds=30,
            ping_timeout_in_seconds=10,
        )
        conn = Connection(
            address="localhost:50000",
            client_id="test-client",
            keep_alive=keep_alive,
        )

        assert conn.keep_alive.enabled is True
        assert conn.keep_alive.ping_interval_in_seconds == 30
        assert conn.keep_alive.ping_timeout_in_seconds == 10


class TestConnectionConstants:
    """Tests for Connection class constants."""

    def test_default_max_send_size_constant(self):
        """Test DEFAULT_MAX_SEND_SIZE is 100MB."""
        assert Connection.DEFAULT_MAX_SEND_SIZE == 1024 * 1024 * 100

    def test_default_max_rcv_size_constant(self):
        """Test DEFAULT_MAX_RCV_SIZE is 100MB."""
        assert Connection.DEFAULT_MAX_RCV_SIZE == 1024 * 1024 * 100

    def test_default_reconnect_interval_constant(self):
        """Test DEFAULT_RECONNECT_INTERVAL_SECONDS is 1."""
        assert Connection.DEFAULT_RECONNECT_INTERVAL_SECONDS == 1
