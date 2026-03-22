"""Example: TLS configuration — connect to KubeMQ with TLS encryption."""

from __future__ import annotations

from kubemq import Client, EventMessage, TLSConfig


def main() -> None:
    # TLS with server certificate verification (one-way TLS)
    tls_config = TLSConfig(
        enabled=True,
        ca_file="/path/to/ca.pem",
    )

    with Client(
        address="kubemq-server:50000",
        client_id="python-tls-tls-setup-client",
        tls=tls_config,
    ) as client:
        info = client.ping()
        print(f"Connected via TLS to {info.host}")

        client.send_event(
            EventMessage(
                channel="python-tls.tls-setup",
                body=b"Encrypted message",
            )
        )
        print("Message sent over TLS connection")


if __name__ == "__main__":
    main()
