from dataclasses import dataclass


@dataclass(frozen=True)
class ServerInfo:
    """Represents information about a server.

    Attributes:
        host (str): The host of the server.
        version (str): The version of the server.
        server_start_time (int): The start time of the server (in seconds).
        server_up_time_seconds (int): The uptime of the server (in seconds).
    """

    host: str
    version: str
    server_start_time: int
    server_up_time_seconds: int

    def __post_init__(self) -> None:
        """Validate that time values are non-negative."""
        if self.server_start_time < 0:
            raise ValueError("Time values must be non-negative")
        if self.server_up_time_seconds < 0:
            raise ValueError("Time values must be non-negative")

    def __str__(self) -> str:
        return (
            f"ServerInfo(host={self.host}, version={self.version}, "
            f"server_start_time={self.server_start_time}, server_up_time_seconds={self.server_up_time_seconds})"
        )
