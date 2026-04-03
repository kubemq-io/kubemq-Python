"""Example: Reconnection — demonstrate auto-reconnect configuration."""

from __future__ import annotations

import asyncio

from kubemq import AsyncPubSubClient, ClientConfig, EventMessage, KeepAliveConfig


async def main() -> None:
    config = ClientConfig(
        address="localhost:50000",
        client_id="python-error-handling-reconnection-client",
        auto_reconnect=True,
        reconnect_interval_seconds=2,
        max_reconnect_attempts=5,
        reconnect_initial_delay_ms=500,
        reconnect_max_delay_ms=10_000,
        reconnect_backoff_multiplier=2.0,
        keep_alive=KeepAliveConfig(
            enabled=True,
            ping_interval_in_seconds=10,
            ping_timeout_in_seconds=5,
        ),
    )

    try:
        async with AsyncPubSubClient(config=config) as client:
            info = await client.ping()
            print(f"Connected to {info.host}")
            print(f"Auto-reconnect: {config.auto_reconnect}")
            print(f"Max reconnect attempts: {config.max_reconnect_attempts}")
            print(f"Reconnect interval: {config.reconnect_interval_seconds}s")

            await client.publish_event(
                EventMessage(
                    channel="python-error-handling.reconnection",
                    body=b"message with reconnection configured",
                )
            )
            print("Message sent successfully")
            print("Client configured for automatic reconnection on failure")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
