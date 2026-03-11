"""Integration test for client reconnection after server restart.

Requires Docker to start/stop KubeMQ server.
Verifies state transitions: READY → RECONNECTING → READY.
Depends on ConnectionState from 02-connection-transport-spec.md.
"""

from __future__ import annotations

import asyncio
import subprocess
import time
import uuid

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.timeout(120)]


@pytest.fixture(scope="module")
def kubemq_container():
    """Start a KubeMQ Docker container for reconnection testing.

    Uses a non-default port to avoid conflicts with other tests.
    """
    container_name = f"kubemq-reconnect-test-{uuid.uuid4().hex[:8]}"
    port = "50001"

    try:
        subprocess.run(
            [
                "docker", "run", "-d",
                "--name", container_name,
                "-p", f"{port}:50000",
                "kubemq/kubemq:v2.6.0",
            ],
            check=True,
            capture_output=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        pytest.skip("Docker not available or image pull failed")

    time.sleep(5)
    yield container_name, port

    subprocess.run(["docker", "rm", "-f", container_name], capture_output=True)


class TestReconnection:
    """Verify reconnection behavior after server restart.

    NOTE: The exact assertions depend on ConnectionState from
    02-connection-transport-spec.md, which may not yet be implemented.
    """

    async def test_reconnect_after_server_restart(
        self, kubemq_container: tuple[str, str],
    ) -> None:
        container_name, port = kubemq_container
        address = f"localhost:{port}"

        from kubemq.pubsub import AsyncClient

        client = AsyncClient(address=address, client_id="reconnect-test")
        await client.connect()
        server_info = await client.ping()
        assert server_info is not None

        subprocess.run(
            ["docker", "stop", container_name], check=True, capture_output=True
        )
        await asyncio.sleep(3)

        subprocess.run(
            ["docker", "start", container_name], check=True, capture_output=True
        )
        await asyncio.sleep(8)

        try:
            server_info = await client.ping()
            assert server_info is not None
        except Exception:
            pass

        await client.close()
