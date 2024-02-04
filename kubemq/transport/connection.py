from kubemq.transport.keep_alive import KeepAliveConfig
from kubemq.transport.tls_config import TlsConfig


class Connection:
    """

    The `Connection` class represents a connection object that can be used to establish and manage a connection to a remote server.

    Attributes:
        DEFAULT_MAX_SEND_SIZE (int): The default maximum size for sending data (100MB).
        DEFAULT_MAX_RCV_SIZE (int): The default maximum size for receiving data (100MB).
        DEFAULT_RECONNECT_INTERVAL_SECONDS (int): The default interval in seconds for reconnecting to the server (5 seconds).

    __init__ method:
        Initializes a new instance of the Connection class.

        Parameters:
            address (str): The address of the remote server.
            client_id (str): The client ID.
            auth_token (str): The authentication token.
            max_send_size (int): The maximum size for sending data.
            max_receive_size (int): The maximum size for receiving data.
            disable_auto_reconnect (bool): Flag indicating whether auto reconnect is disabled.
            reconnect_interval_seconds (int): The interval in seconds for reconnecting to the server.
            tls (TlsConfig): The TLS configuration.
            keep_alive (KeepAliveConfig): The keep-alive configuration.

    address property:
        Gets or sets the address of the remote server.

    client_id property:
        Gets or sets the client ID.

    auth_token property:
        Gets or sets the authentication token.

    max_send_size property:
        Gets or sets the maximum size for sending data.

    max_receive_size property:
        Gets or sets the maximum size for receiving data.

    disable_auto_reconnect property:
        Gets or sets a flag indicating whether auto reconnect is disabled.

    reconnect_interval_seconds property:
        Gets or sets the interval in seconds for reconnecting to the server.

    tls property:
        Gets or sets the TLS configuration.

    keep_alive property:
        Gets or sets the keep-alive configuration.

    get_reconnect_interval_duration method:
        Gets the reconnect interval duration in milliseconds.

    complete method:
        Completes the connection by setting default values for certain properties if they are not already set.

    validate method:
        Validates the connection by checking if all required properties are set and if the values of certain properties are valid.

    Note: Please make sure to call the `validate` method before establishing a connection to ensure all required properties are set and valid.

    """
    DEFAULT_MAX_SEND_SIZE = 1024 * 1024 * 100  # 100MB
    DEFAULT_MAX_RCV_SIZE = 1024 * 1024 * 100  # 100MB
    DEFAULT_RECONNECT_INTERVAL_SECONDS = 5

    def __init__(self, address: str = "", client_id: str = "", auth_token: str = "",
                 max_send_size: int = DEFAULT_MAX_SEND_SIZE,
                 max_receive_size: int = DEFAULT_MAX_RCV_SIZE,
                 disable_auto_reconnect: bool = False,
                 reconnect_interval_seconds: int = DEFAULT_RECONNECT_INTERVAL_SECONDS,
                 tls: TlsConfig = None,
                 keep_alive: KeepAliveConfig = None,
                 log_level: int = None) -> None:
        self._address: str = address
        self._client_id: str = client_id
        self._auth_token: str = auth_token
        self._max_send_size: int = max_send_size
        self._max_receive_size: int = max_receive_size
        self._disable_auto_reconnect: bool = disable_auto_reconnect
        self._reconnect_interval_seconds: int = reconnect_interval_seconds
        self._tls: TlsConfig = tls if tls else TlsConfig()
        self._keep_alive: KeepAliveConfig = keep_alive if keep_alive else KeepAliveConfig()
        self._log_level: int = log_level

    @property
    def address(self) -> str:
        return self._address

    @address.setter
    def address(self, value: str) -> None:
        self._address = value

    @property
    def client_id(self) -> str:
        return self._client_id

    @client_id.setter
    def client_id(self, value: str) -> None:
        self._client_id = value

    @property
    def auth_token(self) -> str:
        return self._auth_token

    @auth_token.setter
    def auth_token(self, value: str) -> None:
        self._auth_token = value

    @property
    def max_send_size(self) -> int:
        return self._max_send_size

    @max_send_size.setter
    def max_send_size(self, value: int) -> None:
        self._max_send_size = value

    @property
    def max_receive_size(self) -> int:
        return self._max_receive_size

    @max_receive_size.setter
    def max_receive_size(self, value: int) -> None:
        self._max_receive_size = value

    @property
    def disable_auto_reconnect(self) -> bool:
        return self._disable_auto_reconnect

    @disable_auto_reconnect.setter
    def disable_auto_reconnect(self, value: bool) -> None:
        self._disable_auto_reconnect = value

    @property
    def reconnect_interval_seconds(self) -> int:
        return self._reconnect_interval_seconds

    @reconnect_interval_seconds.setter
    def reconnect_interval_seconds(self, value: int) -> None:
        self._reconnect_interval_seconds = value

    @property
    def tls(self) -> TlsConfig:
        return self._tls

    @tls.setter
    def tls(self, value: TlsConfig) -> None:
        self._tls = value

    @property
    def keep_alive(self) -> KeepAliveConfig:
        return self._keep_alive

    @keep_alive.setter
    def keep_alive(self, value: KeepAliveConfig) -> None:
        self._keep_alive = value

    def get_reconnect_interval_duration(self) -> int:
        return self._reconnect_interval_seconds * 1000

    def set_log_level(self, value: int) -> 'Connection':
        self._log_level = value
        return self

    def complete(self) -> 'Connection':
        if self._max_send_size == 0:
            self._max_send_size = self.DEFAULT_MAX_SEND_SIZE
        if self._max_receive_size == 0:
            self._max_receive_size = self.DEFAULT_MAX_RCV_SIZE
        if self._reconnect_interval_seconds == 0:
            self._reconnect_interval_seconds = self.DEFAULT_RECONNECT_INTERVAL_SECONDS
        return self

    def validate(self) -> None:
        if not self._address:
            raise ValueError("Connection must have an address")
        if not self._client_id:
            raise ValueError("Connection must have a client_id")
        if self._max_send_size < 0:
            raise ValueError("Connection max send size must be greater than 0")
        if self._max_receive_size < 0:
            raise ValueError("Connection max receive size must be greater than 0")
        if self._reconnect_interval_seconds < 0:
            raise ValueError("Connection reconnect interval must be greater than 0")
        if self._tls is not None:
            self._tls.validate()
        if self._keep_alive is not None:
            self._keep_alive.validate()
