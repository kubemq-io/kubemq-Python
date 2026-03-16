from pydantic import BaseModel, Field, ValidationInfo, field_validator, model_validator


class KeepAliveConfig(BaseModel):
    """Represents the configuration for keep alive functionality."""

    enabled: bool = Field(default=True, description="Specifies if keep alive is enabled")
    ping_interval_in_seconds: int = Field(
        default=10, description="The interval at which ping requests are sent in seconds"
    )
    ping_timeout_in_seconds: int = Field(
        default=5, description="The timeout for ping requests in seconds"
    )
    permit_without_calls: bool = Field(
        default=True,
        description="Allow keep-alive pings even when there are no active calls",
    )

    @field_validator("ping_interval_in_seconds", "ping_timeout_in_seconds")
    def validate_positive_values(cls, v: int, info: ValidationInfo) -> int:
        """Validate that ping intervals and timeouts are non-negative."""
        if v < 0:
            raise ValueError(f"{info.field_name} must be greater than or equal to 0")
        return v

    @model_validator(mode="after")
    def validate_enabled_config(self) -> "KeepAliveConfig":
        """Validate that intervals are positive when keep-alive is enabled."""
        if self.enabled:
            if self.ping_interval_in_seconds <= 0:
                raise ValueError("Keep alive ping interval must be greater than 0 when enabled")
            if self.ping_timeout_in_seconds <= 0:
                raise ValueError("Keep alive ping timeout must be greater than 0 when enabled")
        return self
