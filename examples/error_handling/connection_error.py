"""Example: Connection error — handle connection failures gracefully."""

from __future__ import annotations

from kubemq import Client, KubeMQConnectionError


def main() -> None:
    try:
        # Attempt to connect to a non-existent server
        client = Client(
            address="localhost:59999",
            client_id="python-error-handling-connection-error-client",
        )
        server_info = client.ping()
        print(f"Connected: {server_info}")
    except KubeMQConnectionError as e:
        print(f"Connection error (expected): {e}")
        print("  Tip: Verify the KubeMQ server is running at the specified address")
    except Exception as e:
        print(f"Unexpected error: {type(e).__name__}: {e}")


if __name__ == "__main__":
    main()
