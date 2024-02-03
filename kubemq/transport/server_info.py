class ServerInfo:
    """

    The ServerInfo class represents information about a server.

    Attributes:
        _host (str): The host of the server.
        _version (str): The version of the server.
        _server_start_time (int): The start time of the server (in seconds).
        _server_up_time_seconds (int): The uptime of the server (in seconds).

    Methods:
        host (property): Getter and setter for the _host attribute.
        version (property): Getter and setter for the _version attribute.
        server_start_time (property): Getter and setter for the _server_start_time attribute.
        server_up_time_seconds (property): Getter and setter for the _server_up_time_seconds attribute.
        __str__(): Returns a string representation of the ServerInfo object.

    """
    def __init__(self, host: str, version: str, server_start_time: int, server_up_time_seconds: int):
        self._host = host
        self._version = version
        self._server_start_time = server_start_time
        self._server_up_time_seconds = server_up_time_seconds

    @property
    def host(self) -> str:
        return self._host

    @host.setter
    def host(self, value: str) -> None:
        self._host = value

    @property
    def version(self) -> str:
        return self._version

    @version.setter
    def version(self, value: str) -> None:
        self._version = value

    @property
    def server_start_time(self) -> int:
        return self._server_start_time

    @server_start_time.setter
    def server_start_time(self, value: int) -> None:
        """

        """
        self._server_start_time = value

    @property
    def server_up_time_seconds(self) -> int:
        return self._server_up_time_seconds

    @server_up_time_seconds.setter
    def server_up_time_seconds(self, value: int) -> None:
        self._server_up_time_seconds = value

    def __str__(self):
        return (f"ServerInfo(host={self._host}, version={self._version}, "
                f"server_start_time={self._server_start_time}, server_up_time_seconds={self._server_up_time_seconds})")
