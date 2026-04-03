"""Example: TLS configuration — connect to KubeMQ with TLS encryption."""

from __future__ import annotations

import asyncio

from kubemq import AsyncPubSubClient, EventMessage, TLSConfig


async def main() -> None:
    tls_config = TLSConfig(
        enabled=True,
        ca_file="/path/to/ca.pem",
    )

    async with AsyncPubSubClient(
        address="kubemq-server:50000",
        client_id="python-tls-tls-setup-client",
        tls=tls_config,
    ) as client:
        info = await client.ping()
        print(f"Connected via TLS to {info.host}")

        await client.publish_event(
            EventMessage(
                channel="python-tls.tls-setup",
                body=b"Encrypted message",
            )
        )
        print("Message sent over TLS connection")


if __name__ == "__main__":
    asyncio.run(main())
