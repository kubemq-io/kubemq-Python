import os
from dataclasses import dataclass


@dataclass
class TlsConfig:
    """Handles the configuration settings for TLS (Transport Layer Security) encryption.

    Provides mutual-TLS (mTLS) or server-only TLS for gRPC connections to a
    KubeMQ server.  When ``enabled`` is ``True``, ``cert_file`` and ``key_file``
    are mandatory; ``ca_file`` is optional and used for custom CA verification.

    Attributes:
        enabled: Whether TLS is active for the connection.
        cert_file: Filesystem path to the PEM-encoded client certificate.
        key_file: Filesystem path to the PEM-encoded client private key.
        ca_file: Filesystem path to the PEM-encoded CA bundle.  Leave empty
            to use the system trust store.

    Raises:
        FileNotFoundError: If any provided file path does not point to an
            existing file on disk.
        ValueError: If ``enabled`` is ``True`` but ``cert_file`` or
            ``key_file`` is empty.

    See Also:
        ``kubemq.transport.transport.Transport``: Consumes this config to
            build a secure gRPC channel.
    """

    enabled: bool = False
    cert_file: str = ""
    key_file: str = ""
    ca_file: str = ""

    def __post_init__(self) -> None:
        """Validate TLS configuration."""
        for attr_name in ("cert_file", "key_file", "ca_file"):
            v = getattr(self, attr_name)
            if v and not os.path.isfile(v):
                raise FileNotFoundError(f"The file was not found: {v}")
        if self.enabled:
            if not self.cert_file:
                raise ValueError("Certificate file must be specified when TLS is enabled")
            if not self.key_file:
                raise ValueError("Key file must be specified when TLS is enabled")
