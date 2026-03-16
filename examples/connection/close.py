"""Example: Close — demonstrate graceful client close."""

from __future__ import annotations

from kubemq import EventMessage, PubSubClient


def main() -> None:
    client = PubSubClient(
        address="localhost:50000",
        client_id="python-connection-close-client",
    )
    try:
        client.publish_event(
            EventMessage(channel="python-connection.close", body=b"Hello before close")
        )
        print("Event sent successfully")
    finally:
        client.close()
        print("Client closed gracefully")


if __name__ == "__main__":
    main()
