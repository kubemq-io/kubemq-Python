from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict
from typing import Optional, ClassVar
from kubemq.transport.keep_alive import KeepAliveConfig
from kubemq.transport.tls_config import TlsConfig


class Connection(BaseModel):
    DEFAULT_MAX_SEND_SIZE: ClassVar[int] = 1024 * 1024 * 100  # 100MB
    DEFAULT_MAX_RCV_SIZE: ClassVar[int] = 1024 * 1024 * 100  # 100MB
    DEFAULT_RECONNECT_INTERVAL_SECONDS: ClassVar[int] = 1

    address: str = Field(default="")
    client_id: str = Field(default="")
    auth_token: str = Field(default="")
    max_send_size: int = Field(default=DEFAULT_MAX_SEND_SIZE)
    max_receive_size: int = Field(default=DEFAULT_MAX_RCV_SIZE)
    disable_auto_reconnect: bool = Field(default=False)
    reconnect_interval_seconds: int = Field(default=DEFAULT_RECONNECT_INTERVAL_SECONDS)
    tls: TlsConfig = Field(default_factory=TlsConfig)
    keep_alive: KeepAliveConfig = Field(default_factory=KeepAliveConfig)
    log_level: Optional[int] = Field(default=None)

    model_config = ConfigDict(validate_assignment=True, arbitrary_types_allowed=True)

    @field_validator("address")
    def validate_address(cls, v: str) -> str:
        if not v:
            raise ValueError("Connection must have an address")
        return v

    @field_validator("client_id")
    def validate_client_id(cls, v: str) -> str:
        if not v:
            raise ValueError("Connection must have a client_id")
        return v

    @field_validator("max_send_size", "max_receive_size", "reconnect_interval_seconds")
    def validate_positive_values(cls, v: int, info) -> int:
        if v < 0:
            raise ValueError(f"{info.field_name} must be greater than or equal to 0")
        return v

    @model_validator(mode="after")
    def validate_tls_and_keep_alive(self) -> "Connection":
        # TLS and KeepAlive configs are automatically validated by Pydantic
        return self

    def get_reconnect_delay(self) -> int:
        return self.reconnect_interval_seconds

    def set_log_level(self, value: int) -> "Connection":
        self.log_level = value
        return self

    def complete(self) -> "Connection":
        if self.max_send_size == 0:
            self.max_send_size = self.DEFAULT_MAX_SEND_SIZE
        if self.max_receive_size == 0:
            self.max_receive_size = self.DEFAULT_MAX_RCV_SIZE
        if self.reconnect_interval_seconds == 0:
            self.reconnect_interval_seconds = self.DEFAULT_RECONNECT_INTERVAL_SECONDS
        return self
