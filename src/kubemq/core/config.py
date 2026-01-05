"""Configuration classes for KubeMQ Python SDK."""

from __future__ import annotations

import os
import socket
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from kubemq.transport.connection import Connection


@dataclass(frozen=True)
class TLSConfig:
    """TLS configuration for secure connections.

    Attributes:
        enabled: Whether TLS is enabled
        cert_file: Path to the client certificate file
        key_file: Path to the client private key file
        ca_file: Path to the CA certificate file (for server verification)
    """

    enabled: bool = False
    cert_file: Path | None = None
    key_file: Path | None = None
    ca_file: Path | None = None

    def __post_init__(self) -> None:
        """Validate TLS configuration."""
        if self.enabled:
            if not self.cert_file:
                raise ValueError("cert_file is required when TLS is enabled")
            if not self.key_file:
                raise ValueError("key_file is required when TLS is enabled")

            # Validate file existence
            for path_attr in ["cert_file", "key_file", "ca_file"]:
                path = getattr(self, path_attr)
                if path and not path.exists():
                    raise FileNotFoundError(f"TLS file not found: {path}")


@dataclass(frozen=True)
class KeepAliveConfig:
    """Keep-alive configuration for gRPC connections.

    Attributes:
        enabled: Whether keep-alive is enabled
        ping_interval_in_seconds: Interval between keep-alive pings
            (matches existing field name in transport/keep_alive.py)
        ping_timeout_in_seconds: Timeout for keep-alive ping response
            (matches existing field name in transport/keep_alive.py)
        permit_without_calls: Allow keep-alive pings without active calls
    """

    enabled: bool = False
    ping_interval_in_seconds: int = 30
    ping_timeout_in_seconds: int = 10
    permit_without_calls: bool = True

    def __post_init__(self) -> None:
        """Validate keep-alive configuration."""
        if self.enabled:
            if self.ping_interval_in_seconds <= 0:
                raise ValueError("ping_interval_in_seconds must be positive when enabled")
            if self.ping_timeout_in_seconds <= 0:
                raise ValueError("ping_timeout_in_seconds must be positive when enabled")


@dataclass
class ClientConfig:
    """Configuration for KubeMQ clients.

    This is the main configuration class used to create KubeMQ clients.
    It supports creation from environment variables, TOML files, and .env files.

    Note: This class is NOT frozen because we need to set default values
    in __post_init__ based on runtime conditions (e.g., hostname).

    Attributes:
        address: KubeMQ server address (host:port)
        client_id: Client identifier (defaults to hostname)
        auth_token: Authentication token for the server
        max_send_size: Maximum message size for sending (bytes)
        max_receive_size: Maximum message size for receiving (bytes)
        default_timeout_seconds: Default timeout for operations
        tls: TLS configuration
        keep_alive: Keep-alive configuration
        auto_reconnect: Whether to automatically reconnect on connection loss
        reconnect_interval_seconds: Interval between reconnection attempts
        log_level: Logging level (None means no logging)
    """

    # Class-level defaults
    DEFAULT_MAX_MESSAGE_SIZE: ClassVar[int] = 100 * 1024 * 1024  # 100MB
    DEFAULT_TIMEOUT_SECONDS: ClassVar[int] = 30

    # Required settings
    address: str

    # Connection identification
    client_id: str | None = None
    auth_token: str | None = None

    # Message size limits
    max_send_size: int = DEFAULT_MAX_MESSAGE_SIZE
    max_receive_size: int = DEFAULT_MAX_MESSAGE_SIZE

    # Timeout settings
    default_timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS

    # TLS settings
    tls: TLSConfig = field(default_factory=TLSConfig)

    # Keep-alive settings
    keep_alive: KeepAliveConfig = field(default_factory=KeepAliveConfig)

    # Reconnection settings
    auto_reconnect: bool = True
    reconnect_interval_seconds: int = 1

    # Logging
    log_level: int | None = None

    def __post_init__(self) -> None:
        """Validate and complete configuration."""
        if not self.address:
            raise ValueError("address is required")

        # Set defaults for mutable fields
        if not self.client_id:
            self.client_id = socket.gethostname()
        if self.max_send_size <= 0:
            self.max_send_size = self.DEFAULT_MAX_MESSAGE_SIZE
        if self.max_receive_size <= 0:
            self.max_receive_size = self.DEFAULT_MAX_MESSAGE_SIZE
        if self.reconnect_interval_seconds <= 0:
            self.reconnect_interval_seconds = 1

    def __repr__(self) -> str:
        """Return string representation with masked auth_token."""
        return (
            f"ClientConfig("
            f"address={self.address!r}, "
            f"client_id={self.client_id!r}, "
            f"auth_token={'***' if self.auth_token else None}, "
            f"tls={self.tls.enabled}, "
            f"keep_alive={self.keep_alive.enabled})"
        )

    @classmethod
    def from_env(cls, prefix: str = "KUBEMQ_") -> ClientConfig:
        """Create configuration from environment variables.

        Environment variables:
            {prefix}ADDRESS: Server address (required)
            {prefix}CLIENT_ID: Client identifier
            {prefix}AUTH_TOKEN: Authentication token
            {prefix}TLS_ENABLED: Whether TLS is enabled (true/false)
            {prefix}TLS_CERT: Path to TLS certificate
            {prefix}TLS_KEY: Path to TLS key
            {prefix}TLS_CA: Path to TLS CA certificate
            {prefix}KEEP_ALIVE_ENABLED: Whether keep-alive is enabled
            {prefix}KEEP_ALIVE_INTERVAL: Keep-alive ping interval (seconds)
            {prefix}KEEP_ALIVE_TIMEOUT: Keep-alive ping timeout (seconds)

        Args:
            prefix: Prefix for environment variable names

        Returns:
            ClientConfig instance

        Raises:
            ValueError: If required environment variables are missing
        """

        def get_bool(key: str, default: bool = False) -> bool:
            value = os.environ.get(f"{prefix}{key}", "").lower()
            return value in ("true", "1", "yes", "on")

        def get_int(key: str, default: int) -> int:
            value = os.environ.get(f"{prefix}{key}")
            return int(value) if value else default

        def get_path(key: str) -> Path | None:
            value = os.environ.get(f"{prefix}{key}")
            return Path(value) if value else None

        address = os.environ.get(f"{prefix}ADDRESS", "")
        if not address:
            raise ValueError(f"{prefix}ADDRESS environment variable is required")

        tls_enabled = get_bool("TLS_ENABLED")
        tls_config = TLSConfig(
            enabled=tls_enabled,
            cert_file=get_path("TLS_CERT") if tls_enabled else None,
            key_file=get_path("TLS_KEY") if tls_enabled else None,
            ca_file=get_path("TLS_CA"),
        )

        keep_alive_enabled = get_bool("KEEP_ALIVE_ENABLED")
        keep_alive_config = KeepAliveConfig(
            enabled=keep_alive_enabled,
            ping_interval_in_seconds=get_int("KEEP_ALIVE_INTERVAL", 30),
            ping_timeout_in_seconds=get_int("KEEP_ALIVE_TIMEOUT", 10),
        )

        return cls(
            address=address,
            client_id=os.environ.get(f"{prefix}CLIENT_ID"),
            auth_token=os.environ.get(f"{prefix}AUTH_TOKEN"),
            tls=tls_config,
            keep_alive=keep_alive_config,
            auto_reconnect=not get_bool("DISABLE_AUTO_RECONNECT"),
            reconnect_interval_seconds=get_int("RECONNECT_INTERVAL", 1),
        )

    @classmethod
    def from_file(cls, path: str) -> ClientConfig:
        """Create configuration from a TOML file.

        Example TOML file:
            address = "localhost:50000"
            client_id = "my-service"
            auth_token = "secret-token"

            [tls]
            enabled = true
            cert_file = "/path/to/cert.pem"
            key_file = "/path/to/key.pem"

            [keep_alive]
            enabled = true
            ping_interval_in_seconds = 30

        Args:
            path: Path to the TOML configuration file

        Returns:
            ClientConfig instance

        Raises:
            FileNotFoundError: If the file doesn't exist
            ValueError: If the file is invalid
        """
        # tomllib is built-in for Python 3.11+
        # tomli is the fallback for Python 3.9-3.10 (declared in pyproject.toml)
        try:
            import tomllib
        except ImportError:
            import tomli as tomllib  # type: ignore[import-not-found]

        with open(path, "rb") as f:
            data = tomllib.load(f)

        tls_data = data.get("tls", {})
        keep_alive_data = data.get("keep_alive", {})

        tls_config = TLSConfig(
            enabled=tls_data.get("enabled", False),
            cert_file=Path(tls_data["cert_file"]) if tls_data.get("cert_file") else None,
            key_file=Path(tls_data["key_file"]) if tls_data.get("key_file") else None,
            ca_file=Path(tls_data["ca_file"]) if tls_data.get("ca_file") else None,
        )

        keep_alive_config = KeepAliveConfig(
            enabled=keep_alive_data.get("enabled", False),
            ping_interval_in_seconds=keep_alive_data.get("ping_interval_in_seconds", 30),
            ping_timeout_in_seconds=keep_alive_data.get("ping_timeout_in_seconds", 10),
        )

        return cls(
            address=data.get("address", ""),
            client_id=data.get("client_id"),
            auth_token=data.get("auth_token"),
            tls=tls_config,
            keep_alive=keep_alive_config,
            auto_reconnect=data.get("auto_reconnect", True),
            reconnect_interval_seconds=data.get("reconnect_interval_seconds", 1),
            log_level=data.get("log_level"),
        )

    @classmethod
    def from_dotenv(cls, path: str = ".env", prefix: str = "KUBEMQ_") -> ClientConfig:
        """Create configuration from a .env file.

        Requires the python-dotenv package to be installed.
        Install with: pip install kubemq[config]

        Args:
            path: Path to the .env file (default: ".env")
            prefix: Prefix for environment variable names

        Returns:
            ClientConfig instance
        """
        try:
            from dotenv import load_dotenv  # type: ignore[import-not-found]
        except ImportError as e:
            raise ImportError(
                "python-dotenv is required for from_dotenv(). "
                "Install it with: pip install kubemq[config]"
            ) from e

        load_dotenv(path)
        return cls.from_env(prefix)

    def to_legacy_connection(self) -> Connection:
        """Convert to legacy Connection object for backward compatibility.

        Returns:
            A Connection instance compatible with existing transport layer
        """
        from kubemq.transport.connection import Connection
        from kubemq.transport.keep_alive import KeepAliveConfig as LegacyKeepAlive
        from kubemq.transport.tls_config import TlsConfig as LegacyTls

        return Connection(
            address=self.address,
            client_id=self.client_id or socket.gethostname(),
            auth_token=self.auth_token or "",
            max_send_size=self.max_send_size,
            max_receive_size=self.max_receive_size,
            disable_auto_reconnect=not self.auto_reconnect,
            reconnect_interval_seconds=self.reconnect_interval_seconds,
            tls=LegacyTls(
                enabled=self.tls.enabled,
                cert_file=str(self.tls.cert_file) if self.tls.cert_file else "",
                key_file=str(self.tls.key_file) if self.tls.key_file else "",
                ca_file=str(self.tls.ca_file) if self.tls.ca_file else "",
            ),
            keep_alive=LegacyKeepAlive(
                enabled=self.keep_alive.enabled,
                ping_interval_in_seconds=self.keep_alive.ping_interval_in_seconds,
                ping_timeout_in_seconds=self.keep_alive.ping_timeout_in_seconds,
            ),
            log_level=self.log_level,
        )
