class KeepAliveConfig:
    """
    Represents the configuration for keep alive functionality.

    Args:
        enabled (bool, optional): Specifies if keep alive is enabled. Default is False.
        ping_interval_in_seconds (int, optional): The interval at which ping requests are sent in seconds.
                                                  Default is 0.
        ping_timeout_in_seconds (int, optional): The timeout for ping requests in seconds. Default is 0.

    Attributes:
        enabled (bool): Specifies if keep alive is enabled.
        ping_interval_in_seconds (int): The interval at which ping requests are sent in seconds.
        ping_timeout_in_seconds (int): The timeout for ping requests in seconds.

    Methods:
        validate(): Validates the keep alive configuration.

    Raises:
        ValueError: If enabled is True but ping_interval_in_seconds or ping_timeout_in_seconds is less than or equal to 0.
    """
    def __init__(self, enabled: bool = False,
                 ping_interval_in_seconds: int = 0,
                 ping_timeout_in_seconds: int = 0) -> None:
        self._enabled: bool = enabled
        self._ping_interval_in_seconds: int = ping_interval_in_seconds
        self._ping_timeout_in_seconds: int = ping_timeout_in_seconds

    @property
    def enabled(self) -> bool:
        return self._enabled

    @enabled.setter
    def enabled(self, value: bool) -> None:
        self._enabled = value

    @property
    def ping_interval_in_seconds(self) -> int:
        return self._ping_interval_in_seconds

    @ping_interval_in_seconds.setter
    def ping_interval_in_seconds(self, value: int) -> None:
        self._ping_interval_in_seconds = value

    @property
    def ping_timeout_in_seconds(self) -> int:
        return self._ping_timeout_in_seconds

    @ping_timeout_in_seconds.setter
    def ping_timeout_in_seconds(self, value: int) -> None:
        self._ping_timeout_in_seconds = value

    def validate(self) -> None:
        if not self._enabled:
            return
        if self._ping_interval_in_seconds <= 0:
            raise ValueError("Keep alive ping interval must be greater than 0")
        if self._ping_timeout_in_seconds <= 0:
            raise ValueError("Keep alive ping timeout must be greater than 0")
