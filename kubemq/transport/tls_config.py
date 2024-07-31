from pydantic import BaseModel, Field, field_validator, model_validator
import os


class TlsConfig(BaseModel):
    """
    Handles the configuration settings for TLS (Transport Layer Security) encryption.
    """

    enabled: bool = Field(
        default=False, description="Indicates whether TLS is enabled or not"
    )
    cert_file: str = Field(default="", description="Path to the TLS certificate file")
    key_file: str = Field(default="", description="Path to the TLS private key file")
    ca_file: str = Field(
        default="", description="Path to the TLS CA (Certificate Authority) file"
    )

    @field_validator("cert_file", "key_file", "ca_file")
    def validate_file_path(cls, v: str) -> str:
        if v and not os.path.isfile(v):
            raise FileNotFoundError(f"The file was not found: {v}")
        return v

    @model_validator(mode="after")
    def validate_enabled_config(self) -> "TlsConfig":
        if self.enabled:
            if not self.cert_file:
                raise ValueError(
                    "Certificate file must be specified when TLS is enabled"
                )
            if not self.key_file:
                raise ValueError("Key file must be specified when TLS is enabled")
        return self
