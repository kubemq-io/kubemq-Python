from pydantic import BaseModel, Field, field_validator


class ServerInfo(BaseModel):
    """
    Represents information about a server.

    Attributes:
        host (str): The host of the server.
        version (str): The version of the server.
        server_start_time (int): The start time of the server (in seconds).
        server_up_time_seconds (int): The uptime of the server (in seconds).
    """

    host: str = Field(..., description="The host of the server")
    version: str = Field(..., description="The version of the server")
    server_start_time: int = Field(
        ..., description="The start time of the server (in seconds)"
    )
    server_up_time_seconds: int = Field(
        ..., description="The uptime of the server (in seconds)"
    )

    @field_validator("server_start_time", "server_up_time_seconds")
    def validate_positive_time(cls, v: int) -> int:
        if v < 0:
            raise ValueError("Time values must be non-negative")
        return v

    def __str__(self) -> str:
        return (
            f"ServerInfo(host={self.host}, version={self.version}, "
            f"server_start_time={self.server_start_time}, server_up_time_seconds={self.server_up_time_seconds})"
        )

    class Config:
        frozen = True  # This makes the model immutable
