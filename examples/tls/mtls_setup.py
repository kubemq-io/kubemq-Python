"""Example: Mutual TLS (mTLS) — client and server authenticate each other."""

from __future__ import annotations

from kubemq import Client, EventMessage, TLSConfig


def main() -> None:
    # Mutual TLS: both client cert+key and CA cert are provided
    tls_config = TLSConfig(
        enabled=True,
        cert_file="/path/to/client-cert.pem",
        key_file="/path/to/client-key.pem",
        ca_file="/path/to/ca.pem",
    )

    with Client(
        address="kubemq-server:50000",
        client_id="python-tls-mtls-setup-client",
        tls=tls_config,
    ) as client:
        info = client.ping()
        print(f"Connected via mTLS to {info.host}")

        client.send_event(
            EventMessage(
                channel="python-tls.mtls-setup",
                body=b"mTLS secured message",
            )
        )
        print("Message sent over mTLS connection")


if __name__ == "__main__":
    main()
