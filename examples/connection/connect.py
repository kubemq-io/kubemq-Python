"""Example: Basic connection — connect to KubeMQ and verify with ping."""

from __future__ import annotations

from kubemq.queues import Client as QueuesClient


def main() -> None:
    with QueuesClient(
        address="localhost:50000",  # TODO: Replace with your KubeMQ server address
        client_id="python-connection-connect-client",
    ) as client:
        server_info = client.ping()
        print(f"Connected to KubeMQ server: {server_info}")


if __name__ == "__main__":
    main()

# Expected output:
# Connected to KubeMQ server: <server-info>
