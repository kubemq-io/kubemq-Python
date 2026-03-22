from dataclasses import dataclass, field
from typing import ClassVar

from kubemq.transport.keep_alive import KeepAliveConfig
from kubemq.transport.tls_config import TlsConfig


@dataclass
class Connection:
    """Connection configuration for the KubeMQ transport layer."""

    DEFAULT_MAX_SEND_SIZE: ClassVar[int] = 1024 * 1024 * 100  # 100MB
    DEFAULT_MAX_RCV_SIZE: ClassVar[int] = 1024 * 1024 * 100  # 100MB
    DEFAULT_RECONNECT_INTERVAL_SECONDS: ClassVar[int] = 1

    address: str = ""
    client_id: str = ""
    auth_token: str = field(default="", repr=False)
    max_send_size: int = DEFAULT_MAX_SEND_SIZE
    max_receive_size: int = DEFAULT_MAX_RCV_SIZE
    disable_auto_reconnect: bool = False
    reconnect_interval_seconds: int = DEFAULT_RECONNECT_INTERVAL_SECONDS
    tls: TlsConfig = field(default_factory=TlsConfig)
    keep_alive: KeepAliveConfig = field(default_factory=KeepAliveConfig)
    log_level: int | None = None

    def __post_init__(self) -> None:
        """Validate connection configuration."""
        if self.max_send_size < 0:
            raise ValueError("max_send_size must be greater than or equal to 0")
        if self.max_receive_size < 0:
            raise ValueError("max_receive_size must be greater than or equal to 0")
        if self.reconnect_interval_seconds < 0:
            raise ValueError("reconnect_interval_seconds must be greater than or equal to 0")

    def get_reconnect_delay(self) -> int:
        """Get the reconnect delay in seconds."""
        return self.reconnect_interval_seconds

    def set_log_level(self, value: int) -> "Connection":
        """Set the logging level for the connection."""
        self.log_level = value
        return self

    def complete(self) -> "Connection":
        """Complete the connection with default values for unset fields."""
        if self.max_send_size == 0:
            self.max_send_size = self.DEFAULT_MAX_SEND_SIZE
        if self.max_receive_size == 0:
            self.max_receive_size = self.DEFAULT_MAX_RCV_SIZE
        if self.reconnect_interval_seconds == 0:
            self.reconnect_interval_seconds = self.DEFAULT_RECONNECT_INTERVAL_SECONDS
        return self
