"""Unit tests for kubemq.transport config classes.

Tests for TlsConfig and KeepAliveConfig.
"""

from __future__ import annotations

import os
import tempfile

import pytest

from kubemq.transport.keep_alive import KeepAliveConfig
from kubemq.transport.tls_config import TlsConfig


class TestTlsConfigDefaults:
    """Tests for TlsConfig default values."""

    def test_default_values(self):
        """Test default values are set correctly."""
        config = TlsConfig()

        assert config.enabled is False
        assert config.cert_file == ""
        assert config.key_file == ""
        assert config.ca_file == ""

    def test_disabled_config_is_valid(self):
        """Test disabled config with empty files is valid."""
        config = TlsConfig(enabled=False)

        assert config.enabled is False


class TestTlsConfigValidation:
    """Tests for TlsConfig validation."""

    def test_enabled_requires_cert_file(self):
        """Test enabled config requires cert_file."""
        with pytest.raises(ValueError, match="Certificate file must be specified"):
            TlsConfig(
                enabled=True,
                cert_file="",
                key_file="",
            )

    def test_enabled_requires_key_file(self):
        """Test enabled config requires key_file."""
        # Create a temporary cert file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pem") as f:
            cert_file = f.name
            f.write(b"fake cert")

        try:
            with pytest.raises(ValueError, match="Key file must be specified"):
                TlsConfig(
                    enabled=True,
                    cert_file=cert_file,
                    key_file="",
                )
        finally:
            os.unlink(cert_file)

    def test_cert_file_not_found_raises(self):
        """Test non-existent cert file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError, match="file was not found"):
            TlsConfig(
                enabled=False,
                cert_file="/nonexistent/path/cert.pem",
            )

    def test_key_file_not_found_raises(self):
        """Test non-existent key file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError, match="file was not found"):
            TlsConfig(
                enabled=False,
                key_file="/nonexistent/path/key.pem",
            )

    def test_ca_file_not_found_raises(self):
        """Test non-existent CA file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError, match="file was not found"):
            TlsConfig(
                enabled=False,
                ca_file="/nonexistent/path/ca.pem",
            )

    def test_valid_enabled_config(self):
        """Test valid enabled config with real files."""
        # Create temporary files
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pem") as f:
            cert_file = f.name
            f.write(b"fake cert")

        with tempfile.NamedTemporaryFile(delete=False, suffix=".key") as f:
            key_file = f.name
            f.write(b"fake key")

        try:
            config = TlsConfig(
                enabled=True,
                cert_file=cert_file,
                key_file=key_file,
            )

            assert config.enabled is True
            assert config.cert_file == cert_file
            assert config.key_file == key_file
        finally:
            os.unlink(cert_file)
            os.unlink(key_file)

    def test_valid_config_with_ca_file(self):
        """Test valid config with CA file."""
        # Create temporary files
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pem") as f:
            cert_file = f.name
            f.write(b"fake cert")

        with tempfile.NamedTemporaryFile(delete=False, suffix=".key") as f:
            key_file = f.name
            f.write(b"fake key")

        with tempfile.NamedTemporaryFile(delete=False, suffix=".ca") as f:
            ca_file = f.name
            f.write(b"fake ca")

        try:
            config = TlsConfig(
                enabled=True,
                cert_file=cert_file,
                key_file=key_file,
                ca_file=ca_file,
            )

            assert config.ca_file == ca_file
        finally:
            os.unlink(cert_file)
            os.unlink(key_file)
            os.unlink(ca_file)


class TestKeepAliveConfigDefaults:
    """Tests for KeepAliveConfig default values."""

    def test_default_values(self):
        """Test default values are GS-compliant (enabled, 10s/5s)."""
        config = KeepAliveConfig()

        assert config.enabled is True
        assert config.ping_interval_in_seconds == 10
        assert config.ping_timeout_in_seconds == 5
        assert config.permit_without_calls is True

    def test_disabled_config_is_valid(self):
        """Test disabled config is valid."""
        config = KeepAliveConfig(enabled=False)

        assert config.enabled is False


class TestKeepAliveConfigValidation:
    """Tests for KeepAliveConfig validation."""

    def test_enabled_requires_positive_ping_interval(self):
        """Test enabled config requires positive ping interval."""
        with pytest.raises(ValueError, match="ping interval must be greater than 0"):
            KeepAliveConfig(
                enabled=True,
                ping_interval_in_seconds=0,
                ping_timeout_in_seconds=10,
            )

    def test_enabled_requires_positive_ping_timeout(self):
        """Test enabled config requires positive ping timeout."""
        with pytest.raises(ValueError, match="ping timeout must be greater than 0"):
            KeepAliveConfig(
                enabled=True,
                ping_interval_in_seconds=30,
                ping_timeout_in_seconds=0,
            )

    def test_negative_ping_interval_raises(self):
        """Test negative ping interval raises ValueError."""
        with pytest.raises(ValueError, match="must be greater than or equal to 0"):
            KeepAliveConfig(
                enabled=False,
                ping_interval_in_seconds=-1,
            )

    def test_negative_ping_timeout_raises(self):
        """Test negative ping timeout raises ValueError."""
        with pytest.raises(ValueError, match="must be greater than or equal to 0"):
            KeepAliveConfig(
                enabled=False,
                ping_timeout_in_seconds=-1,
            )

    def test_valid_enabled_config(self):
        """Test valid enabled config."""
        config = KeepAliveConfig(
            enabled=True,
            ping_interval_in_seconds=30,
            ping_timeout_in_seconds=10,
        )

        assert config.enabled is True
        assert config.ping_interval_in_seconds == 30
        assert config.ping_timeout_in_seconds == 10

    def test_disabled_config_allows_any_values(self):
        """Test disabled config allows any non-negative values."""
        config = KeepAliveConfig(
            enabled=False,
            ping_interval_in_seconds=0,
            ping_timeout_in_seconds=0,
        )

        assert config.enabled is False
        assert config.ping_interval_in_seconds == 0

    def test_custom_values(self):
        """Test custom values are set correctly."""
        config = KeepAliveConfig(
            enabled=True,
            ping_interval_in_seconds=60,
            ping_timeout_in_seconds=20,
        )

        assert config.ping_interval_in_seconds == 60
        assert config.ping_timeout_in_seconds == 20
