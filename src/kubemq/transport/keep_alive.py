from dataclasses import dataclass


@dataclass
class KeepAliveConfig:
    """Represents the configuration for keep alive functionality."""

    enabled: bool = True
    ping_interval_in_seconds: int = 10
    ping_timeout_in_seconds: int = 5
    permit_without_calls: bool = True

    def __post_init__(self) -> None:
        """Validate keep-alive configuration."""
        if self.ping_interval_in_seconds < 0:
            raise ValueError("ping_interval_in_seconds must be greater than or equal to 0")
        if self.ping_timeout_in_seconds < 0:
            raise ValueError("ping_timeout_in_seconds must be greater than or equal to 0")
        if self.enabled:
            if self.ping_interval_in_seconds <= 0:
                raise ValueError("Keep alive ping interval must be greater than 0 when enabled")
            if self.ping_timeout_in_seconds <= 0:
                raise ValueError("Keep alive ping timeout must be greater than 0 when enabled")
