import os

class TlsConfig:
    """

    Class TlsConfig handles the configuration settings for TLS (Transport Layer Security) encryption. It provides properties for enabling/disabling TLS, setting the certificate file, key
    * file, and CA (Certificate Authority) file.

    Attributes:
        _enabled (bool): Indicates whether TLS is enabled or not.
        _cert_file (str): Path to the TLS certificate file.
        _key_file (str): Path to the TLS private key file.
        _ca_file (str): Path to the TLS CA (Certificate Authority) file.

    Methods:
        __init__(self, enabled: bool = False, cert_file: str = "", key_file: str = "", ca_file: str = "") -> None:
            Initializes a new instance of TlsConfig with the given configuration settings.

            Args:
                enabled (bool): Indicates whether TLS is enabled or not. Default is False.
                cert_file (str): Path to the TLS certificate file. Default is an empty string.
                key_file (str): Path to the TLS private key file. Default is an empty string.
                ca_file (str): Path to the TLS CA (Certificate Authority) file. Default is an empty string.

        enabled(self) -> bool:
            Gets the value indicating whether TLS is enabled or not.

            Returns:
                bool: True if TLS is enabled, False otherwise.

        enabled(self, value: bool) -> None:
            Sets the value indicating whether TLS is enabled or not.

            Args:
                value (bool): True to enable TLS, False to disable.

        cert_file(self) -> str:
            Gets the path to the TLS certificate file.

            Returns:
                str: The path to the certificate file.

        cert_file(self, value: str) -> None:
            Sets the path to the TLS certificate file.

            Args:
                value (str): The path to the certificate file.

        key_file(self) -> str:
            Gets the path to the TLS private key file.

            Returns:
                str: The path to the private key file.

        key_file(self, value: str) -> None:
            Sets the path to the TLS private key file.

            Args:
                value (str): The path to the private key file.

        ca_file(self) -> str:
            Gets the path to the TLS CA (Certificate Authority) file.

            Returns:
                str: The path to the CA file.

        ca_file(self, value: str) -> None:
            Sets the path to the TLS CA (Certificate Authority) file.

            Args:
                value (str): The path to the CA file.

        validate(self) -> None:
            Performs validation on the TLS configuration settings. Raises FileNotFoundError if any of the specified files (certificate, key, or CA) do not exist.

    """
    def __init__(self, enabled: bool = False,
                 cert_file: str = "",
                 key_file: str = "",
                 ca_file: str = "") -> None:
        self._enabled: bool = enabled
        self._cert_file: str = cert_file
        self._key_file: str = key_file
        self._ca_file: str = ca_file

    @property
    def enabled(self) -> bool:
        return self._enabled

    @enabled.setter
    def enabled(self, value: bool) -> None:
        self._enabled = value

    @property
    def cert_file(self) -> str:
        return self._cert_file

    @cert_file.setter
    def cert_file(self, value: str) -> None:
        self._cert_file = value

    @property
    def key_file(self) -> str:
        return self._key_file

    @key_file.setter
    def key_file(self, value: str) -> None:
        self._key_file = value

    @property
    def ca_file(self) -> str:
        return self._ca_file

    @ca_file.setter
    def ca_file(self, value: str) -> None:
        self._ca_file = value

    def validate(self) -> None:
        if self._enabled:
            if self._cert_file and not os.path.isfile(self._cert_file):
                raise FileNotFoundError(f"The certificate file was not found: {self._cert_file}")

            if self._key_file and not os.path.isfile(self._key_file):
                raise FileNotFoundError(f"The key file was not found: {self._key_file}")

            if self._ca_file and not os.path.isfile(self._ca_file):
                raise FileNotFoundError(f"The CA file was not found: {self._ca_file}")
