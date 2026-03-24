"""Example: Connection error — handle connection failures gracefully."""

from __future__ import annotations

from kubemq import KubeMQConnectionError
from kubemq.queues import Client as QueuesClient


def main() -> None:
    try:
        # Attempt to connect to a non-existent server
        client = QueuesClient(
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
