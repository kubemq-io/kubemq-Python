"""Example: Mutual TLS (mTLS) — client and server authenticate each other."""

from __future__ import annotations

import asyncio

from kubemq import AsyncPubSubClient, EventMessage, TLSConfig


async def main() -> None:
    tls_config = TLSConfig(
        enabled=True,
        cert_file="/path/to/client-cert.pem",
        key_file="/path/to/client-key.pem",
        ca_file="/path/to/ca.pem",
    )

    async with AsyncPubSubClient(
        address="kubemq-server:50000",
        client_id="python-tls-mtls-setup-client",
        tls=tls_config,
    ) as client:
        info = await client.ping()
        print(f"Connected via mTLS to {info.host}")

        await client.publish_event(
            EventMessage(
                channel="python-tls.mtls-setup",
                body=b"mTLS secured message",
            )
        )
        print("Message sent over mTLS connection")


if __name__ == "__main__":
    asyncio.run(main())
