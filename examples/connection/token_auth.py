"""Example: Token authentication — connect with an auth token."""

from __future__ import annotations

from kubemq import Client, EventMessage


def main() -> None:
    with Client(
        address="localhost:50000",
        auth_token="your-authentication-token",
        client_id="python-connection-token-auth-client",
    ) as client:
        info = client.ping()
        print(f"Authenticated and connected to {info.host}")

        client.send_event(
            EventMessage(
                channel="python-connection.token-auth",
                body=b"Authenticated message",
            )
        )
        print("Message sent with authentication")


if __name__ == "__main__":
    main()
