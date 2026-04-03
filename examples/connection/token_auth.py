"""Example: Token authentication — connect with an auth token."""

from __future__ import annotations

import asyncio

from kubemq import AsyncPubSubClient, EventMessage


async def main() -> None:
    async with AsyncPubSubClient(
        address="localhost:50000",
        auth_token="your-authentication-token",
        client_id="python-connection-token-auth-client",
    ) as client:
        info = await client.ping()
        print(f"Authenticated and connected to {info.host}")

        await client.publish_event(
            EventMessage(
                channel="python-connection.token-auth",
                body=b"Authenticated message",
            )
        )
        print("Message sent with authentication")


if __name__ == "__main__":
    asyncio.run(main())
