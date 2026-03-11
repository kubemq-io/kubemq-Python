"""Example: Mutual TLS (mTLS) — client and server authenticate each other."""

from __future__ import annotations

from kubemq import EventMessage, PubSubClient, TLSConfig


def main() -> None:
    # Mutual TLS: both client cert+key and CA cert are provided
    tls_config = TLSConfig(
        enabled=True,
        cert_file="/path/to/client-cert.pem",
        key_file="/path/to/client-key.pem",
        ca_file="/path/to/ca.pem",
    )

    with PubSubClient(
        address="kubemq-server:50000",
        tls=tls_config,
    ) as client:
        info = client.ping()
        print(f"Connected via mTLS to {info.host}")

        client.publish_event(
            EventMessage(channel="mtls-channel", body=b"mTLS secured message")
        )
        print("Message sent over mTLS connection")


if __name__ == "__main__":
    main()
