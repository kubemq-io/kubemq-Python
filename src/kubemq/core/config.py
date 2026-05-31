"""Configuration classes for KubeMQ Python SDK."""

from __future__ import annotations

import os
import re
import socket
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum, unique
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from kubemq.core.types import AsyncCredentialProvider, CredentialProvider


@dataclass(frozen=True)
class TLSConfig:
    """TLS configuration for secure connections.

    Supports three modes:
        1. Server-only TLS: ``TLSConfig(enabled=True)`` — uses system CA bundle.
        2. Server-only TLS with custom CA: ``TLSConfig(enabled=True, ca_file=...)``
        3. Mutual TLS (mTLS): ``TLSConfig(enabled=True, cert_file=..., key_file=..., ca_file=...)``

    Credentials can be provided as file paths OR PEM bytes, but not both
    for the same credential type (e.g. ``cert_file`` and ``cert_pem``
    are mutually exclusive).

    Attributes:
        enabled: Whether TLS is enabled.
        cert_file: Path to the client certificate file (mTLS).
        key_file: Path to the client private key file (mTLS).
        ca_file: Path to the CA certificate file (server verification).
        insecure_skip_verify: Disable certificate verification (dev only).
        min_tls_version: Minimum TLS version ("1.2" or "1.3").
        server_name_override: Override server name for TLS verification.
        ca_pem: CA certificate as PEM bytes (alternative to ca_file).
        cert_pem: Client certificate as PEM bytes (alternative to cert_file).
        key_pem: Client private key as PEM bytes (alternative to key_file).

    Defaults:
        | Field                  | Default             |
        |------------------------|---------------------|
        | enabled                | False               |
        | cert_file              | None                |
        | key_file               | None                |
        | ca_file                | None                |
        | insecure_skip_verify   | False               |
        | min_tls_version        | "1.2"               |
        | server_name_override   | None                |
        | ca_pem                 | None                |
        | cert_pem               | None                |
        | key_pem                | None                |

    Thread Safety:
        This class is thread-safe (immutable frozen dataclass).
    """

    enabled: bool = False
    cert_file: Path | None = None
    key_file: Path | None = None
    ca_file: Path | None = None
    insecure_skip_verify: bool = False
    min_tls_version: str = "1.2"
    server_name_override: str | None = None
    ca_pem: bytes | None = None
    cert_pem: bytes | None = None
    key_pem: bytes | None = None

    def __post_init__(self) -> None:
        """Validate TLS configuration at construction time (fail-fast).

        Client cert + key are only required for mTLS, not server-only TLS.
        Server-only TLS (the most common deployment pattern) only needs
        TLS enabled — the system CA bundle is used by default.
        """
        if self.enabled:
            has_any_client_cert = any((self.cert_file, self.cert_pem))
            has_any_client_key = any((self.key_file, self.key_pem))
            if has_any_client_cert != has_any_client_key:
                raise ValueError(
                    "Client certificate and key must both be provided for mTLS, "
                    "or both omitted for server-only TLS."
                )

            for attr_name in ("cert_file", "key_file", "ca_file"):
                path = getattr(self, attr_name)
                if path is not None:
                    p = Path(path) if not isinstance(path, Path) else path
                    if not p.exists():
                        raise FileNotFoundError(
                            f"TLS file not found: {p} — verify the path is correct"
                        )
                    if not os.access(p, os.R_OK):
                        raise PermissionError(
                            f"TLS file not readable: {p} — check file permissions"
                        )

            valid_versions = ("1.2", "1.3")
            if self.min_tls_version not in valid_versions:
                raise ValueError(
                    f"min_tls_version must be one of {valid_versions}, got {self.min_tls_version!r}"
                )

            if self.cert_file and self.cert_pem:
                raise ValueError("Cannot specify both cert_file and cert_pem — choose one")
            if self.key_file and self.key_pem:
                raise ValueError("Cannot specify both key_file and key_pem — choose one")
            if self.ca_file and self.ca_pem:
                raise ValueError("Cannot specify both ca_file and ca_pem — choose one")


@dataclass(frozen=True)
class KeepAliveConfig:
    """Keep-alive configuration for gRPC connections.

    Defaults match Golden Standard: 10s interval, 5s timeout,
    permit without calls. Dead connections detected within 15s.

    Cloud load balancer advisory: The 10s keepalive default is
    optimized for direct and in-cluster Kubernetes connections.
    When connecting through cloud load balancers (AWS ALB/NLB,
    GCP CLB, Azure LB), verify the load balancer's idle timeout
    exceeds the keepalive interval.

    Defaults:
        | Field                      | Default |
        |----------------------------|---------|
        | enabled                    | True    |
        | ping_interval_in_seconds   | 10      |
        | ping_timeout_in_seconds    | 5       |
        | permit_without_calls       | True    |

    Thread Safety:
        This class is thread-safe (immutable frozen dataclass).
    """

    enabled: bool = True
    ping_interval_in_seconds: int = 10
    ping_timeout_in_seconds: int = 5
    permit_without_calls: bool = True

    def __post_init__(self) -> None:
        """Validate keep-alive configuration."""
        if self.enabled:
            if self.ping_interval_in_seconds < 5:
                raise ValueError(
                    "ping_interval_in_seconds must be >= 5 when enabled "
                    "(server enforces minimum 5s client ping interval)"
                )
            if self.ping_timeout_in_seconds <= 0:
                raise ValueError("ping_timeout_in_seconds must be positive when enabled")


@unique
class JitterType(Enum):
    """Jitter strategy for retry backoff."""

    FULL = "FULL"
    EQUAL = "EQUAL"
    NONE = "NONE"


@dataclass(frozen=True)
class RetryPolicy:
    """Immutable retry policy configuration.

    Set at client construction. Cannot be modified after creation.

    Attributes:
        max_retries: Maximum retry attempts. 0 disables retries.
        initial_backoff_ms: Initial backoff delay in milliseconds.
        max_backoff_ms: Maximum backoff cap in milliseconds.
        backoff_multiplier: Multiplier applied per attempt.
        jitter: Jitter strategy to reduce thundering herd.
        max_concurrent_retries: Concurrent retry limit per client (SPEC-ERR-7).
    """

    max_retries: int = 3
    initial_backoff_ms: int = 500
    max_backoff_ms: int = 30_000
    backoff_multiplier: float = 2.0
    jitter: JitterType = JitterType.FULL
    max_concurrent_retries: int = 10

    def __post_init__(self) -> None:
        if not (0 <= self.max_retries <= 10):
            raise ValueError(f"max_retries must be 0–10, got {self.max_retries}")
        if not (50 <= self.initial_backoff_ms <= 5000):
            raise ValueError(f"initial_backoff_ms must be 50–5000, got {self.initial_backoff_ms}")
        if not (1000 <= self.max_backoff_ms <= 120_000):
            raise ValueError(f"max_backoff_ms must be 1000–120000, got {self.max_backoff_ms}")
        if not (1.5 <= self.backoff_multiplier <= 3.0):
            raise ValueError(f"backoff_multiplier must be 1.5–3.0, got {self.backoff_multiplier}")
        if not (0 <= self.max_concurrent_retries <= 100):
            raise ValueError(
                f"max_concurrent_retries must be 0–100, got {self.max_concurrent_retries}"
            )


@dataclass(frozen=True)
class OperationTimeouts:
    """Per-operation default timeouts in seconds.

    These defaults follow the Golden Standard specification.
    Users can override individual timeouts at call site via ``timeout`` kwarg.
    """

    send_publish: float = 5.0
    subscribe_initial: float = 10.0
    request_query: float = 10.0
    queue_receive_single: float = 10.0
    queue_receive_streaming: float = 30.0
    connection_establishment: float = 10.0

    def __post_init__(self) -> None:
        for field_name in (
            "send_publish",
            "subscribe_initial",
            "request_query",
            "queue_receive_single",
            "queue_receive_streaming",
            "connection_establishment",
        ):
            value = getattr(self, field_name)
            if value <= 0:
                raise ValueError(f"{field_name} must be positive, got {value}")

    @classmethod
    def legacy(cls) -> OperationTimeouts:
        """Create timeouts with uniform 30s for backward compatibility."""
        return cls(
            send_publish=30.0,
            subscribe_initial=30.0,
            request_query=30.0,
            queue_receive_single=30.0,
            queue_receive_streaming=30.0,
            connection_establishment=30.0,
        )


def resolve_timeout(
    explicit: float | None,
    per_operation_default: float,
) -> float:
    """Resolve the effective timeout for an operation.

    Priority: explicit kwarg > per-operation default.
    """
    if explicit is not None:
        return explicit
    return per_operation_default


@dataclass
class ClientConfig:
    """Configuration for KubeMQ clients.

    A single client instance uses one gRPC channel and is safe to share
    across threads (sync) or tasks (async). Create one client and reuse it;
    do not create a new client per operation.

    This class supports creation from environment variables, TOML files,
    and .env files.

    Note: This class is NOT frozen because we need to set default values
    in __post_init__ based on runtime conditions (e.g., hostname).

    Defaults:
        | Field                        | Default                  |
        |------------------------------|--------------------------|
        | address                      | "localhost:50000"        |
        | client_id                    | socket.gethostname()     |
        | auth_token                   | None (no auth)           |
        | max_send_size                | 4_194_304 (4 MB)         |
        | max_receive_size             | 4_194_304 (4 MB)         |
        | default_timeout_seconds      | 30                       |
        | connection_timeout           | 10.0                     |
        | tls                          | TLSConfig() (disabled)   |
        | keep_alive                   | KeepAliveConfig() (enabled, 10s/5s) |
        | auto_reconnect               | True                     |
        | reconnect_interval_seconds   | 1                        |
        | max_reconnect_attempts       | -1 (unlimited)           |
        | drain_timeout                | 5.0                      |
        | wait_for_ready               | True                     |
        | log_level                    | None (no logging)        |

    Thread Safety:
        This class is **not** thread-safe. Configure before passing to a
        client constructor. Do not modify after client creation.

    Example:
        >>> from kubemq.core.config import ClientConfig, TLSConfig, KeepAliveConfig
        >>> config = ClientConfig(
        ...     address="kubemq.prod.example.com:50000",
        ...     client_id="order-service",
        ...     auth_token="my-secret-token",
        ...     tls=TLSConfig(
        ...         enabled=True,
        ...         cert_file="/certs/client.pem",
        ...         key_file="/certs/client-key.pem",
        ...         ca_file="/certs/ca.pem",
        ...     ),
        ...     keep_alive=KeepAliveConfig(
        ...         enabled=True,
        ...         ping_interval_in_seconds=15,
        ...         ping_timeout_in_seconds=5,
        ...     ),
        ... )
    """

    # Class-level defaults
    DEFAULT_MAX_MESSAGE_SIZE: ClassVar[int] = 4 * 1024 * 1024  # 4MB (spec default)
    DEFAULT_TIMEOUT_SECONDS: ClassVar[int] = 30
    DEFAULT_CONNECTION_TIMEOUT: ClassVar[float] = 10.0
    DEFAULT_DRAIN_TIMEOUT: ClassVar[float] = 5.0
    DEFAULT_RECONNECT_BUFFER_SIZE: ClassVar[int] = 8 * 1024 * 1024  # 8MB

    # Required settings — with default for zero-config local dev
    address: str = "localhost:50000"

    # Connection identification
    client_id: str | None = None
    auth_token: str | None = None

    # Message size limits
    max_send_size: int = DEFAULT_MAX_MESSAGE_SIZE
    max_receive_size: int = DEFAULT_MAX_MESSAGE_SIZE

    # Timeout settings
    default_timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS
    connection_timeout: float = DEFAULT_CONNECTION_TIMEOUT

    # TLS settings
    tls: TLSConfig = field(default_factory=TLSConfig)

    # Keep-alive settings
    keep_alive: KeepAliveConfig = field(default_factory=KeepAliveConfig)

    # Reconnection settings
    auto_reconnect: bool = True
    reconnect_interval_seconds: int = 1
    max_reconnect_attempts: int = -1
    reconnect_initial_delay_ms: int = 500
    reconnect_max_delay_ms: int = 30_000
    reconnect_backoff_multiplier: float = 2.0
    reconnect_buffer_size: int = DEFAULT_RECONNECT_BUFFER_SIZE
    buffer_overflow_mode: str = "error"

    # Shutdown settings
    drain_timeout: float = DEFAULT_DRAIN_TIMEOUT
    callback_completion_timeout: float = 30.0

    # Behavior settings
    wait_for_ready: bool = True

    # Auto-TLS for remote addresses (opt-in for v4, default True planned for v5)
    auto_tls_for_remote: bool = False

    # Retry policy
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)

    # Per-operation timeouts
    legacy_timeout_mode: bool = False
    operation_timeouts: OperationTimeouts = field(default_factory=OperationTimeouts)

    # Logging
    log_level: int | None = None

    # Structured logger injection (satisfies Logger Protocol from core/types.py)
    logger: Any = None

    # Observability — OTel provider injection
    # Types are Any at runtime to avoid hard dependency on opentelemetry-api.
    tracer_provider: Any = None
    meter_provider: Any = None

    # Metrics cardinality management (per-client: each client has its own
    # KubeMQMetrics instance with its own meter)
    max_channel_cardinality: int = 100
    channel_allowlist: list[str] = field(default_factory=list)

    # Credential provider (AUTH-4)
    credential_provider: CredentialProvider | AsyncCredentialProvider | None = field(
        default=None, repr=False
    )
    credential_timeout: float = 5.0

    # gRPC connection pool size (number of parallel HTTP/2 connections).
    # Default 5 distributes send operations across multiple TCP connections.
    # Set to 1 to disable pooling (single connection, legacy behavior).
    connection_pool_size: int = 5

    # Internal send queue depth (bounded queue for backpressure)
    max_send_queue_size: int = 10_000

    # Callbacks (not serializable — set programmatically only)
    on_buffer_drain: Callable[[int], None] | None = field(default=None, repr=False)

    def __post_init__(self) -> None:  # noqa: C901
        """Validate and complete configuration."""
        if not self.address:
            self.address = "localhost:50000"

        # Fail-fast: reject empty-string auth_token (DX-4.1)
        if self.auth_token is not None and not self.auth_token.strip():
            raise ValueError(
                "auth_token must be non-empty when provided. Use None to disable authentication."
            )

        # Fail-fast: reject empty-string client_id (DX-4.3)
        if self.client_id is not None and not self.client_id.strip():
            raise ValueError(
                "client_id must be non-empty when provided. "
                "Use None to auto-generate from hostname."
            )

        # Set defaults for mutable fields
        if not self.client_id:
            hostname = socket.gethostname()
            self.client_id = re.sub(r"[^a-zA-Z0-9_-]", "_", hostname)
        if self.max_send_size <= 0:
            self.max_send_size = self.DEFAULT_MAX_MESSAGE_SIZE
        if self.max_receive_size <= 0:
            self.max_receive_size = self.DEFAULT_MAX_MESSAGE_SIZE
        if self.reconnect_interval_seconds <= 0:
            self.reconnect_interval_seconds = 1

        # Validate new fields
        if self.connection_timeout <= 0:
            raise ValueError("connection_timeout must be positive")
        if self.drain_timeout < 0:
            raise ValueError("drain_timeout must be non-negative")
        if self.callback_completion_timeout < 0:
            raise ValueError("callback_completion_timeout must be non-negative")
        if self.reconnect_buffer_size < 0:
            raise ValueError("reconnect_buffer_size must be non-negative")
        if self.buffer_overflow_mode not in ("error", "block"):
            raise ValueError("buffer_overflow_mode must be 'error' or 'block'")
        if self.credential_timeout <= 0:
            raise ValueError("credential_timeout must be positive")

        if self.legacy_timeout_mode:
            self.operation_timeouts = OperationTimeouts.legacy()

        if self.auth_token and not self.credential_provider:
            from kubemq._internal.auth import StaticTokenProvider

            self.credential_provider = StaticTokenProvider(self.auth_token)

    @property
    def token_present(self) -> bool:
        """Whether an auth token is configured.

        Use this for debug logging instead of logging the token value.
        """
        return bool(self.auth_token and self.auth_token.strip())

    def _resolve_tls_enabled(self) -> bool:
        """Determine effective TLS state based on address and config."""
        if self.tls.enabled:
            return True
        if not self.auto_tls_for_remote:
            return False
        return not self._is_localhost()

    def _is_localhost(self) -> bool:
        """Check if the configured address targets localhost.

        Handles IPv6 bracketed notation: ``[::1]:50000`` is correctly
        recognised as localhost by extracting the host inside brackets.
        """
        addr = self.address.strip().lower()
        if addr.startswith("["):
            bracket_end = addr.find("]")
            host = addr[1:bracket_end] if bracket_end > 0 else addr
        else:
            host = addr.split(":")[0].strip()
        return host in ("localhost", "127.0.0.1", "::1", "")

    def __repr__(self) -> str:
        """Return string representation with masked auth_token."""
        return (
            f"ClientConfig("
            f"address={self.address!r}, "
            f"client_id={self.client_id!r}, "
            f"auth_token={'***' if self.auth_token else None}, "
            f"tls={self.tls.enabled}, "
            f"keep_alive={self.keep_alive.enabled}, "
            f"connection_timeout={self.connection_timeout}, "
            f"wait_for_ready={self.wait_for_ready})"
        )

    __str__ = __repr__

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

        keep_alive_enabled_raw = os.environ.get(f"{prefix}KEEP_ALIVE_ENABLED")
        keep_alive_enabled = (
            keep_alive_enabled_raw.lower() in ("true", "1", "yes", "on")
            if keep_alive_enabled_raw is not None
            else True
        )
        keep_alive_config = KeepAliveConfig(
            enabled=keep_alive_enabled,
            ping_interval_in_seconds=get_int("KEEP_ALIVE_INTERVAL", 10),
            ping_timeout_in_seconds=get_int("KEEP_ALIVE_TIMEOUT", 5),
        )

        def get_float(key: str, default: float) -> float:
            value = os.environ.get(f"{prefix}{key}")
            return float(value) if value else default

        wait_for_ready_raw = os.environ.get(f"{prefix}WAIT_FOR_READY")
        wait_for_ready = (
            wait_for_ready_raw.lower() in ("true", "1", "yes", "on")
            if wait_for_ready_raw is not None
            else True
        )

        return cls(
            address=address,
            client_id=os.environ.get(f"{prefix}CLIENT_ID"),
            auth_token=os.environ.get(f"{prefix}AUTH_TOKEN"),
            tls=tls_config,
            keep_alive=keep_alive_config,
            auto_reconnect=not get_bool("DISABLE_AUTO_RECONNECT"),
            reconnect_interval_seconds=get_int("RECONNECT_INTERVAL", 1),
            connection_timeout=get_float("CONNECTION_TIMEOUT", 10.0),
            wait_for_ready=wait_for_ready,
            reconnect_buffer_size=get_int("RECONNECT_BUFFER_SIZE", 8 * 1024 * 1024),
            drain_timeout=get_float("DRAIN_TIMEOUT", 5.0),
            max_reconnect_attempts=get_int("MAX_RECONNECT_ATTEMPTS", -1),
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
            import tomli as tomllib

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
            enabled=keep_alive_data.get("enabled", True),
            ping_interval_in_seconds=keep_alive_data.get("ping_interval_in_seconds", 10),
            ping_timeout_in_seconds=keep_alive_data.get("ping_timeout_in_seconds", 5),
        )

        reconnect_data = data.get("reconnect", {})

        return cls(
            address=data.get("address", "localhost:50000"),
            client_id=data.get("client_id"),
            auth_token=data.get("auth_token"),
            tls=tls_config,
            keep_alive=keep_alive_config,
            auto_reconnect=data.get("auto_reconnect", True),
            reconnect_interval_seconds=data.get("reconnect_interval_seconds", 1),
            connection_timeout=float(data.get("connection_timeout", 10.0)),
            wait_for_ready=data.get("wait_for_ready", True),
            drain_timeout=float(data.get("drain_timeout", 5.0)),
            max_reconnect_attempts=reconnect_data.get(
                "max_attempts", data.get("max_reconnect_attempts", -1)
            ),
            reconnect_buffer_size=reconnect_data.get(
                "buffer_size", data.get("reconnect_buffer_size", 8 * 1024 * 1024)
            ),
            reconnect_initial_delay_ms=reconnect_data.get(
                "initial_delay_ms", data.get("reconnect_initial_delay_ms", 500)
            ),
            reconnect_max_delay_ms=reconnect_data.get(
                "max_delay_ms", data.get("reconnect_max_delay_ms", 30_000)
            ),
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
            from dotenv import load_dotenv
        except ImportError as e:
            raise ImportError(
                "python-dotenv is required for from_dotenv(). "
                "Install it with: pip install kubemq[config]"
            ) from e

        load_dotenv(path)
        return cls.from_env(prefix)
