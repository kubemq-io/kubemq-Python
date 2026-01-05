"""Tests for kubemq.core.config module."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

from kubemq.core.config import (
    ClientConfig,
    KeepAliveConfig,
    TLSConfig,
)


class TestTLSConfig:
    """Tests for TLSConfig dataclass."""

    def test_default_values(self):
        """Test default configuration values."""
        config = TLSConfig()
        assert config.enabled is False
        assert config.cert_file is None
        assert config.key_file is None
        assert config.ca_file is None

    def test_enabled_requires_cert_and_key(self):
        """Test TLS enabled requires cert and key files."""
        with pytest.raises(ValueError, match="cert_file is required"):
            TLSConfig(enabled=True)

        with pytest.raises(ValueError, match="key_file is required"):
            TLSConfig(enabled=True, cert_file=Path("/fake/cert.pem"))

    def test_enabled_with_existing_files(self):
        """Test TLS enabled with existing certificate files."""
        with tempfile.NamedTemporaryFile(suffix=".pem", delete=False) as cert_file:
            cert_path = Path(cert_file.name)
        with tempfile.NamedTemporaryFile(suffix=".pem", delete=False) as key_file:
            key_path = Path(key_file.name)

        try:
            config = TLSConfig(
                enabled=True,
                cert_file=cert_path,
                key_file=key_path,
            )
            assert config.enabled is True
            assert config.cert_file == cert_path
            assert config.key_file == key_path
        finally:
            os.unlink(cert_path)
            os.unlink(key_path)

    def test_frozen(self):
        """Test that TLSConfig is immutable."""
        config = TLSConfig()
        with pytest.raises(AttributeError):
            config.enabled = True  # type: ignore[misc]

    def test_file_not_found_validation(self):
        """Test that non-existent files raise FileNotFoundError."""
        with pytest.raises(FileNotFoundError, match="TLS file not found"):
            TLSConfig(
                enabled=True,
                cert_file=Path("/nonexistent/cert.pem"),
                key_file=Path("/nonexistent/key.pem"),
            )


class TestKeepAliveConfig:
    """Tests for KeepAliveConfig dataclass."""

    def test_default_values(self):
        """Test default configuration values."""
        config = KeepAliveConfig()
        assert config.enabled is False
        assert config.ping_interval_in_seconds == 30
        assert config.ping_timeout_in_seconds == 10

    def test_custom_values(self):
        """Test custom configuration values."""
        config = KeepAliveConfig(
            enabled=True,
            ping_interval_in_seconds=60,
            ping_timeout_in_seconds=20,
        )
        assert config.enabled is True
        assert config.ping_interval_in_seconds == 60
        assert config.ping_timeout_in_seconds == 20

    def test_frozen(self):
        """Test that KeepAliveConfig is immutable."""
        config = KeepAliveConfig()
        with pytest.raises(AttributeError):
            config.enabled = True  # type: ignore[misc]

    def test_validation_when_enabled(self):
        """Test validation of interval values when enabled."""
        with pytest.raises(ValueError, match="ping_interval_in_seconds must be positive"):
            KeepAliveConfig(enabled=True, ping_interval_in_seconds=0)

        with pytest.raises(ValueError, match="ping_timeout_in_seconds must be positive"):
            KeepAliveConfig(enabled=True, ping_timeout_in_seconds=0)


class TestClientConfig:
    """Tests for ClientConfig dataclass."""

    def test_default_values(self):
        """Test default configuration values."""
        config = ClientConfig(address="localhost:50000")
        assert config.address == "localhost:50000"
        assert config.client_id is not None  # Should default to hostname
        assert config.auth_token is None
        assert config.auto_reconnect is True
        assert config.reconnect_interval_seconds == 1
        assert config.max_send_size == ClientConfig.DEFAULT_MAX_MESSAGE_SIZE
        assert config.max_receive_size == ClientConfig.DEFAULT_MAX_MESSAGE_SIZE

    def test_custom_client_id(self):
        """Test custom client ID."""
        config = ClientConfig(
            address="localhost:50000",
            client_id="my-custom-client",
        )
        assert config.client_id == "my-custom-client"

    def test_with_auth_token(self):
        """Test configuration with auth token."""
        config = ClientConfig(
            address="localhost:50000",
            auth_token="secret-token",
        )
        assert config.auth_token == "secret-token"

    def test_tls_config_disabled(self):
        """Test TLS configuration when disabled."""
        tls = TLSConfig(enabled=False)
        config = ClientConfig(
            address="localhost:50000",
            tls=tls,
        )
        assert config.tls.enabled is False

    def test_keep_alive_config(self):
        """Test keep-alive configuration."""
        keep_alive = KeepAliveConfig(enabled=True, ping_interval_in_seconds=45)
        config = ClientConfig(
            address="localhost:50000",
            keep_alive=keep_alive,
        )
        assert config.keep_alive.enabled is True
        assert config.keep_alive.ping_interval_in_seconds == 45

    def test_mutable(self):
        """Test that ClientConfig is mutable."""
        config = ClientConfig(address="localhost:50000")
        config.address = "newhost:50000"
        assert config.address == "newhost:50000"

    def test_from_env(self):
        """Test creating config from environment variables."""
        # Set environment variables
        env_vars = {
            "KUBEMQ_ADDRESS": "envhost:50000",
            "KUBEMQ_CLIENT_ID": "env-client",
            "KUBEMQ_AUTH_TOKEN": "env-token",
        }

        original_env = {}
        for key, value in env_vars.items():
            original_env[key] = os.environ.get(key)
            os.environ[key] = value

        try:
            config = ClientConfig.from_env()
            assert config.address == "envhost:50000"
            assert config.client_id == "env-client"
            assert config.auth_token == "env-token"
        finally:
            # Restore original environment
            for key, value in original_env.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def test_from_env_with_prefix(self):
        """Test creating config with custom prefix."""
        os.environ["MY_APP_ADDRESS"] = "customhost:50000"

        try:
            config = ClientConfig.from_env(prefix="MY_APP_")
            assert config.address == "customhost:50000"
        finally:
            os.environ.pop("MY_APP_ADDRESS", None)

    def test_from_env_missing_address(self):
        """Test that missing ADDRESS env var raises error."""
        # Clear any existing KUBEMQ_ADDRESS
        original = os.environ.pop("KUBEMQ_ADDRESS", None)
        try:
            with pytest.raises(ValueError, match="ADDRESS environment variable is required"):
                ClientConfig.from_env()
        finally:
            if original:
                os.environ["KUBEMQ_ADDRESS"] = original

    def test_from_file_toml(self):
        """Test creating config from TOML file."""
        config_data = """
address = "filehost:50000"
client_id = "file-client"
auth_token = "file-token"
auto_reconnect = false
reconnect_interval_seconds = 5
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(config_data)
            f.flush()

            try:
                config = ClientConfig.from_file(f.name)
                assert config.address == "filehost:50000"
                assert config.client_id == "file-client"
                assert config.auth_token == "file-token"
                assert config.auto_reconnect is False
                assert config.reconnect_interval_seconds == 5
            finally:
                os.unlink(f.name)

    def test_from_file_not_found(self):
        """Test error handling for missing file."""
        with pytest.raises(FileNotFoundError):
            ClientConfig.from_file("/nonexistent/path/config.toml")

    def test_from_dotenv(self):
        """Test creating config from .env file."""
        dotenv_content = """
KUBEMQ_ADDRESS=dotenvhost:50000
KUBEMQ_CLIENT_ID=dotenv-client
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".env", delete=False) as f:
            f.write(dotenv_content)
            f.flush()

            try:
                config = ClientConfig.from_dotenv(f.name)
                assert config.address == "dotenvhost:50000"
                assert config.client_id == "dotenv-client"
            except ImportError:
                # python-dotenv not installed
                pytest.skip("python-dotenv not installed")
            finally:
                os.unlink(f.name)

    def test_to_legacy_connection(self):
        """Test conversion to legacy Connection object."""
        # Create temp files for TLS
        with tempfile.NamedTemporaryFile(suffix=".pem", delete=False) as cert_file:
            cert_path = Path(cert_file.name)
        with tempfile.NamedTemporaryFile(suffix=".pem", delete=False) as key_file:
            key_path = Path(key_file.name)
        with tempfile.NamedTemporaryFile(suffix=".pem", delete=False) as ca_file:
            ca_path = Path(ca_file.name)

        try:
            tls = TLSConfig(
                enabled=True,
                cert_file=cert_path,
                key_file=key_path,
                ca_file=ca_path,
            )
            keep_alive = KeepAliveConfig(
                enabled=True,
                ping_interval_in_seconds=45,
                ping_timeout_in_seconds=15,
            )
            config = ClientConfig(
                address="localhost:50000",
                client_id="test-client",
                auth_token="test-token",
                tls=tls,
                keep_alive=keep_alive,
                max_send_size=2097152,
                max_receive_size=4194304,
                auto_reconnect=False,
                reconnect_interval_seconds=5,
            )

            connection = config.to_legacy_connection()

            assert connection.address == "localhost:50000"
            assert connection.client_id == "test-client"
            assert connection.auth_token == "test-token"
            assert connection.disable_auto_reconnect is True
            assert connection.reconnect_interval_seconds == 5
            assert connection.max_send_size == 2097152
            assert connection.max_receive_size == 4194304
        finally:
            os.unlink(cert_path)
            os.unlink(key_path)
            os.unlink(ca_path)

    def test_default_max_message_size(self):
        """Test the default max message size constant."""
        assert ClientConfig.DEFAULT_MAX_MESSAGE_SIZE == 100 * 1024 * 1024  # 100MB

    def test_address_required(self):
        """Test that address is required."""
        with pytest.raises(ValueError, match="address is required"):
            ClientConfig(address="")


class TestConfigRepr:
    """Tests for configuration repr output."""

    def test_client_config_repr_masks_token(self):
        """Test that repr masks auth token."""
        config = ClientConfig(
            address="localhost:50000",
            auth_token="secret-token-123",
        )
        repr_str = repr(config)
        assert "secret-token-123" not in repr_str
        assert "***" in repr_str
