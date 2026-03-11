"""Example: Token authentication — connect with an auth token."""

from __future__ import annotations

from kubemq import EventMessage, PubSubClient


def main() -> None:
    with PubSubClient(
        address="kubemq-server:50000",
        auth_token="your-authentication-token",
        client_id="authenticated-service",
    ) as client:
        info = client.ping()
        print(f"Authenticated and connected to {info.host}")

        client.publish_event(
            EventMessage(channel="secure-events", body=b"Authenticated message")
        )
        print("Message sent with authentication")


if __name__ == "__main__":
    main()
