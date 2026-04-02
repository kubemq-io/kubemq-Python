"""Tests for kubemq.core.config module."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

from kubemq.core.config import (
    ClientConfig,
    JitterType,
    KeepAliveConfig,
    OperationTimeouts,
    RetryPolicy,
    TLSConfig,
    resolve_timeout,
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

    def test_server_only_tls_no_client_cert_required(self):
        """Test server-only TLS does not require client cert/key files."""
        config = TLSConfig(enabled=True)
        assert config.enabled is True
        assert config.cert_file is None
        assert config.key_file is None

    def test_mtls_requires_both_cert_and_key(self):
        """Test mTLS requires both cert and key (not just one)."""
        with pytest.raises(ValueError, match="Client certificate and key must both be provided"):
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
        """Test default configuration values (GS-compliant: enabled, 10s/5s)."""
        config = KeepAliveConfig()
        assert config.enabled is True
        assert config.ping_interval_in_seconds == 10
        assert config.ping_timeout_in_seconds == 5
        assert config.permit_without_calls is True

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
        with pytest.raises(ValueError, match="ping_interval_in_seconds must be >= 5"):
            KeepAliveConfig(enabled=True, ping_interval_in_seconds=0)

        with pytest.raises(ValueError, match="ping_interval_in_seconds must be >= 5"):
            KeepAliveConfig(enabled=True, ping_interval_in_seconds=4)

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
        assert config.connection_timeout == 10.0
        assert config.wait_for_ready is True
        assert config.drain_timeout == 5.0
        assert config.reconnect_buffer_size == 8 * 1024 * 1024
        assert config.max_reconnect_attempts == -1
        assert config.reconnect_initial_delay_ms == 500
        assert config.reconnect_max_delay_ms == 30_000
        assert config.reconnect_backoff_multiplier == 2.0
        assert config.buffer_overflow_mode == "error"
        assert config.on_buffer_drain is None

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

    def test_default_max_message_size(self):
        """Test the default max message size constant."""
        assert ClientConfig.DEFAULT_MAX_MESSAGE_SIZE == 4 * 1024 * 1024  # 4MB (spec default)

    def test_address_defaults_when_empty(self):
        """Test that empty address defaults to localhost:50000."""
        config = ClientConfig(address="")
        assert config.address == "localhost:50000"

    def test_address_defaults_when_omitted(self):
        """Test that omitted address defaults to localhost:50000."""
        config = ClientConfig()
        assert config.address == "localhost:50000"


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


# -----------------------------------------------------------------------
# REQ-ERR-3: RetryPolicy
# -----------------------------------------------------------------------


class TestJitterType:
    def test_members(self):
        assert set(JitterType) == {JitterType.FULL, JitterType.EQUAL, JitterType.NONE}


class TestRetryPolicy:
    def test_defaults(self):
        policy = RetryPolicy()
        assert policy.max_retries == 3
        assert policy.initial_backoff_ms == 500
        assert policy.max_backoff_ms == 30_000
        assert policy.backoff_multiplier == 2.0
        assert policy.jitter == JitterType.FULL
        assert policy.max_concurrent_retries == 10

    def test_frozen(self):
        policy = RetryPolicy()
        with pytest.raises(AttributeError):
            policy.max_retries = 5  # type: ignore[misc]

    def test_validation_max_retries(self):
        with pytest.raises(ValueError, match="max_retries must be 0–10"):
            RetryPolicy(max_retries=11)
        with pytest.raises(ValueError, match="max_retries must be 0–10"):
            RetryPolicy(max_retries=-1)

    def test_validation_initial_backoff(self):
        with pytest.raises(ValueError, match="initial_backoff_ms must be 50–5000"):
            RetryPolicy(initial_backoff_ms=10)
        with pytest.raises(ValueError, match="initial_backoff_ms must be 50–5000"):
            RetryPolicy(initial_backoff_ms=6000)

    def test_validation_max_backoff(self):
        with pytest.raises(ValueError, match="max_backoff_ms must be 1000–120000"):
            RetryPolicy(max_backoff_ms=500)

    def test_validation_multiplier(self):
        with pytest.raises(ValueError, match="backoff_multiplier must be 1.5–3.0"):
            RetryPolicy(backoff_multiplier=1.0)
        with pytest.raises(ValueError, match="backoff_multiplier must be 1.5–3.0"):
            RetryPolicy(backoff_multiplier=4.0)

    def test_validation_max_concurrent_retries(self):
        with pytest.raises(ValueError, match="max_concurrent_retries must be 0–100"):
            RetryPolicy(max_concurrent_retries=-1)
        with pytest.raises(ValueError, match="max_concurrent_retries must be 0–100"):
            RetryPolicy(max_concurrent_retries=101)

    def test_zero_retries_valid(self):
        policy = RetryPolicy(max_retries=0)
        assert policy.max_retries == 0

    def test_zero_concurrent_retries_valid(self):
        policy = RetryPolicy(max_concurrent_retries=0)
        assert policy.max_concurrent_retries == 0


# -----------------------------------------------------------------------
# REQ-ERR-4: OperationTimeouts
# -----------------------------------------------------------------------


class TestOperationTimeouts:
    def test_defaults(self):
        timeouts = OperationTimeouts()
        assert timeouts.send_publish == 5.0
        assert timeouts.subscribe_initial == 10.0
        assert timeouts.request_query == 10.0
        assert timeouts.queue_receive_single == 10.0
        assert timeouts.queue_receive_streaming == 30.0
        assert timeouts.connection_establishment == 10.0

    def test_frozen(self):
        timeouts = OperationTimeouts()
        with pytest.raises(AttributeError):
            timeouts.send_publish = 99.0  # type: ignore[misc]

    def test_validation_positive(self):
        with pytest.raises(ValueError, match="send_publish must be positive"):
            OperationTimeouts(send_publish=0)
        with pytest.raises(ValueError, match="connection_establishment must be positive"):
            OperationTimeouts(connection_establishment=-1.0)

    def test_legacy_factory(self):
        legacy = OperationTimeouts.legacy()
        assert legacy.send_publish == 30.0
        assert legacy.subscribe_initial == 30.0
        assert legacy.request_query == 30.0
        assert legacy.queue_receive_single == 30.0
        assert legacy.queue_receive_streaming == 30.0
        assert legacy.connection_establishment == 30.0

    def test_custom_values(self):
        timeouts = OperationTimeouts(send_publish=2.0, request_query=15.0)
        assert timeouts.send_publish == 2.0
        assert timeouts.request_query == 15.0


class TestResolveTimeout:
    def test_explicit_wins(self):
        assert resolve_timeout(2.0, 5.0) == 2.0

    def test_none_uses_default(self):
        assert resolve_timeout(None, 5.0) == 5.0

    def test_explicit_zero_is_explicit(self):
        # 0.0 is falsy but is a valid explicit override
        # The spec says "if explicit is not None" so 0.0 should be used
        # But note: 0.0 timeout would be impractical. The function is simple.
        assert resolve_timeout(0.0, 5.0) == 0.0


class TestClientConfigRetryAndTimeouts:
    def test_default_retry_policy(self):
        config = ClientConfig()
        assert isinstance(config.retry_policy, RetryPolicy)
        assert config.retry_policy.max_retries == 3

    def test_custom_retry_policy(self):
        policy = RetryPolicy(max_retries=5)
        config = ClientConfig(retry_policy=policy)
        assert config.retry_policy.max_retries == 5

    def test_default_operation_timeouts(self):
        config = ClientConfig()
        assert isinstance(config.operation_timeouts, OperationTimeouts)
        assert config.operation_timeouts.send_publish == 5.0

    def test_legacy_timeout_mode(self):
        config = ClientConfig(legacy_timeout_mode=True)
        assert config.operation_timeouts.send_publish == 30.0
        assert config.operation_timeouts.subscribe_initial == 30.0
        assert config.operation_timeouts.request_query == 30.0

    def test_legacy_timeout_mode_false(self):
        config = ClientConfig(legacy_timeout_mode=False)
        assert config.operation_timeouts.send_publish == 5.0


# -----------------------------------------------------------------------
# Coverage extension: lines 96, 102, 108, 112, 116, 394, 401, 412, 414,
# 418, 420, 422, 424, 426, 428, 444, 452, 460-466, 603-604
# -----------------------------------------------------------------------


class TestTLSConfigExtendedValidation:
    def test_unreadable_file_raises_permission_error(self):
        with tempfile.NamedTemporaryFile(suffix=".pem", delete=False) as cf:
            cert_path = Path(cf.name)
        with tempfile.NamedTemporaryFile(suffix=".pem", delete=False) as kf:
            key_path = Path(kf.name)

        try:
            from unittest.mock import patch as _patch

            with _patch("kubemq.core.config.os.access", return_value=False):
                with pytest.raises(PermissionError, match="not readable"):
                    TLSConfig(enabled=True, cert_file=cert_path, key_file=key_path)
        finally:
            os.unlink(cert_path)
            os.unlink(key_path)

    def test_invalid_min_tls_version(self):
        with pytest.raises(ValueError, match="min_tls_version must be one of"):
            TLSConfig(enabled=True, min_tls_version="1.0")

    def test_cert_file_and_cert_pem_conflict(self):
        with tempfile.NamedTemporaryFile(suffix=".pem", delete=False) as cf:
            cert_path = Path(cf.name)
        with tempfile.NamedTemporaryFile(suffix=".pem", delete=False) as kf:
            key_path = Path(kf.name)
        try:
            with pytest.raises(ValueError, match="Cannot specify both cert_file and cert_pem"):
                TLSConfig(
                    enabled=True,
                    cert_file=cert_path,
                    cert_pem=b"PEM",
                    key_file=key_path,
                )
        finally:
            os.unlink(cert_path)
            os.unlink(key_path)

    def test_key_file_and_key_pem_conflict(self):
        with tempfile.NamedTemporaryFile(suffix=".pem", delete=False) as cf:
            cert_path = Path(cf.name)
        with tempfile.NamedTemporaryFile(suffix=".pem", delete=False) as kf:
            key_path = Path(kf.name)
        try:
            with pytest.raises(ValueError, match="Cannot specify both key_file and key_pem"):
                TLSConfig(
                    enabled=True,
                    cert_file=cert_path,
                    key_file=key_path,
                    key_pem=b"PEM",
                )
        finally:
            os.unlink(cert_path)
            os.unlink(key_path)

    def test_ca_file_and_ca_pem_conflict(self):
        with tempfile.NamedTemporaryFile(suffix=".pem", delete=False) as caf:
            ca_path = Path(caf.name)
        try:
            with pytest.raises(ValueError, match="Cannot specify both ca_file and ca_pem"):
                TLSConfig(enabled=True, ca_file=ca_path, ca_pem=b"PEM")
        finally:
            os.unlink(ca_path)


class TestClientConfigPostInitValidation:
    def test_empty_auth_token_raises(self):
        with pytest.raises(ValueError, match="auth_token must be non-empty"):
            ClientConfig(address="localhost:50000", auth_token="   ")

    def test_empty_client_id_raises(self):
        with pytest.raises(ValueError, match="client_id must be non-empty"):
            ClientConfig(address="localhost:50000", client_id="   ")

    def test_negative_max_receive_size_defaults(self):
        config = ClientConfig(address="localhost:50000", max_receive_size=-1)
        assert config.max_receive_size == ClientConfig.DEFAULT_MAX_MESSAGE_SIZE

    def test_negative_reconnect_interval_defaults(self):
        config = ClientConfig(address="localhost:50000", reconnect_interval_seconds=-1)
        assert config.reconnect_interval_seconds == 1

    def test_connection_timeout_must_be_positive(self):
        with pytest.raises(ValueError, match="connection_timeout must be positive"):
            ClientConfig(address="localhost:50000", connection_timeout=0)

    def test_drain_timeout_must_be_non_negative(self):
        with pytest.raises(ValueError, match="drain_timeout must be non-negative"):
            ClientConfig(address="localhost:50000", drain_timeout=-1)

    def test_callback_completion_timeout_must_be_non_negative(self):
        with pytest.raises(ValueError, match="callback_completion_timeout must be non-negative"):
            ClientConfig(address="localhost:50000", callback_completion_timeout=-1)

    def test_reconnect_buffer_size_must_be_non_negative(self):
        with pytest.raises(ValueError, match="reconnect_buffer_size must be non-negative"):
            ClientConfig(address="localhost:50000", reconnect_buffer_size=-1)

    def test_buffer_overflow_mode_must_be_valid(self):
        with pytest.raises(ValueError, match="buffer_overflow_mode must be"):
            ClientConfig(address="localhost:50000", buffer_overflow_mode="drop")

    def test_credential_timeout_must_be_positive(self):
        with pytest.raises(ValueError, match="credential_timeout must be positive"):
            ClientConfig(address="localhost:50000", credential_timeout=0)


class TestClientConfigTokenPresent:
    def test_token_present_true(self):
        config = ClientConfig(address="localhost:50000", auth_token="secret")
        assert config.token_present is True

    def test_token_present_false_when_none(self):
        config = ClientConfig(address="localhost:50000")
        assert config.token_present is False


class TestClientConfigIsLocalhost:
    def test_ipv6_bracket_is_localhost(self):
        config = ClientConfig(address="[::1]:50000")
        assert config._is_localhost() is True

    def test_127_is_localhost(self):
        config = ClientConfig(address="127.0.0.1:50000")
        assert config._is_localhost() is True

    def test_remote_is_not_localhost(self):
        config = ClientConfig(address="remote.server:50000")
        assert config._is_localhost() is False

    def test_resolve_tls_enabled_for_remote(self):
        config = ClientConfig(address="remote.server:50000", auto_tls_for_remote=True)
        assert config._resolve_tls_enabled() is True

    def test_resolve_tls_disabled_for_localhost(self):
        config = ClientConfig(address="localhost:50000", auto_tls_for_remote=True)
        assert config._resolve_tls_enabled() is False


class TestClientConfigFromFileWithTLS:
    def test_from_file_with_tls_and_keepalive_sections(self):
        with tempfile.NamedTemporaryFile(suffix=".pem", delete=False) as cf:
            cert_path = cf.name
        with tempfile.NamedTemporaryFile(suffix=".pem", delete=False) as kf:
            key_path = kf.name
        with tempfile.NamedTemporaryFile(suffix=".pem", delete=False) as caf:
            ca_path = caf.name

        config_data = f"""
address = "tls-host:50000"
client_id = "tls-client"

[tls]
enabled = true
cert_file = "{cert_path}"
key_file = "{key_path}"
ca_file = "{ca_path}"

[keep_alive]
enabled = true
ping_interval_in_seconds = 30
ping_timeout_in_seconds = 10
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as toml_f:
            toml_f.write(config_data)
            toml_f.flush()
            toml_path = toml_f.name

        try:
            config = ClientConfig.from_file(toml_path)
            assert config.address == "tls-host:50000"
            assert config.tls.enabled is True
            assert str(config.tls.cert_file) == cert_path
            assert str(config.tls.key_file) == key_path
            assert str(config.tls.ca_file) == ca_path
            assert config.keep_alive.enabled is True
            assert config.keep_alive.ping_interval_in_seconds == 30
            assert config.keep_alive.ping_timeout_in_seconds == 10
        finally:
            os.unlink(cert_path)
            os.unlink(key_path)
            os.unlink(ca_path)
            os.unlink(toml_path)


class TestClientConfigFromDotenvImportError:
    def test_from_dotenv_without_dotenv_raises(self):
        import sys
        from unittest.mock import patch as _patch

        with _patch.dict(sys.modules, {"dotenv": None}):
            with pytest.raises(ImportError, match="python-dotenv is required"):
                ClientConfig.from_dotenv()
