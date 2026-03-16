from typing import ClassVar

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

from kubemq.transport.keep_alive import KeepAliveConfig
from kubemq.transport.tls_config import TlsConfig


class Connection(BaseModel):
    """Connection configuration for the KubeMQ transport layer."""

    DEFAULT_MAX_SEND_SIZE: ClassVar[int] = 1024 * 1024 * 100  # 100MB
    DEFAULT_MAX_RCV_SIZE: ClassVar[int] = 1024 * 1024 * 100  # 100MB
    DEFAULT_RECONNECT_INTERVAL_SECONDS: ClassVar[int] = 1

    address: str = Field(default="")
    client_id: str = Field(default="")
    auth_token: str = Field(default="", repr=False)
    max_send_size: int = Field(default=DEFAULT_MAX_SEND_SIZE)
    max_receive_size: int = Field(default=DEFAULT_MAX_RCV_SIZE)
    disable_auto_reconnect: bool = Field(default=False)
    reconnect_interval_seconds: int = Field(default=DEFAULT_RECONNECT_INTERVAL_SECONDS)
    tls: TlsConfig = Field(default_factory=TlsConfig)
    keep_alive: KeepAliveConfig = Field(default_factory=KeepAliveConfig)
    log_level: int | None = Field(default=None)

    model_config = ConfigDict(validate_assignment=True, arbitrary_types_allowed=True)

    @field_validator("address")
    def validate_address(cls, v: str) -> str:
        """Validate that the address is not empty."""
        if not v:
            raise ValueError("Connection must have an address")
        return v

    @field_validator("client_id")
    def validate_client_id(cls, v: str) -> str:
        """Validate that the client_id is not empty."""
        if not v:
            raise ValueError("Connection must have a client_id")
        return v

    @field_validator("max_send_size", "max_receive_size", "reconnect_interval_seconds")
    def validate_positive_values(cls, v: int, info: ValidationInfo) -> int:
        """Validate that size and interval values are non-negative."""
        if v < 0:
            raise ValueError(f"{info.field_name} must be greater than or equal to 0")
        return v

    @model_validator(mode="after")
    def validate_tls_and_keep_alive(self) -> "Connection":
        """Validate TLS and keep-alive configurations."""
        # TLS and KeepAlive configs are automatically validated by Pydantic
        return self

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
