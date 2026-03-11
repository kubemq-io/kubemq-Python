"""Example: TLS configuration — connect to KubeMQ with TLS encryption."""

from __future__ import annotations

from kubemq import EventMessage, PubSubClient, TLSConfig


def main() -> None:
    # TLS with server certificate verification (one-way TLS)
    tls_config = TLSConfig(
        enabled=True,
        ca_file="/path/to/ca.pem",
    )

    with PubSubClient(
        address="kubemq-server:50000",
        tls=tls_config,
    ) as client:
        info = client.ping()
        print(f"Connected via TLS to {info.host}")

        client.publish_event(
            EventMessage(channel="secure-channel", body=b"Encrypted message")
        )
        print("Message sent over TLS connection")


if __name__ == "__main__":
    main()
