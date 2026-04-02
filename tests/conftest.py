"""
KubeMQ Python SDK Test Configuration

This module contains pytest fixtures and configuration for all tests.
"""

import os
import uuid
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest

# Configuration from environment
KUBEMQ_ADDRESS = os.environ.get("KUBEMQ_ADDRESS", "localhost:50000")
KUBEMQ_AUTH_TOKEN = os.environ.get("KUBEMQ_AUTH_TOKEN", "")


@pytest.fixture(scope="session")
def kubemq_address() -> str:
    """Get KubeMQ server address from environment or use default."""
    return KUBEMQ_ADDRESS


@pytest.fixture(scope="session")
def kubemq_auth_token() -> str:
    """Get KubeMQ auth token from environment."""
    return KUBEMQ_AUTH_TOKEN


@pytest.fixture
def unique_channel() -> str:
    """Generate unique channel name for test isolation."""
    return f"test-channel-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def unique_client_id() -> str:
    """Generate unique client ID for test isolation."""
    return f"test-client-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def sync_pubsub_client(kubemq_address: str, unique_client_id: str):
    """
    Sync PubSub client fixture.

    Yields a connected PubSub client and closes it after the test.
    Skips if KubeMQ server is not available.
    """
    from kubemq.pubsub import Client

    try:
        client = Client(address=kubemq_address, client_id=unique_client_id)
        # Test connection
        client.ping()
        yield client
        client.close()
    except Exception as e:
        pytest.skip(f"KubeMQ server not available: {e}")


@pytest.fixture
def sync_queues_client(kubemq_address: str, unique_client_id: str):
    """
    Sync Queues client fixture.

    Yields a connected Queues client and closes it after the test.
    Skips if KubeMQ server is not available.
    """
    from kubemq.queues import Client

    try:
        client = Client(address=kubemq_address, client_id=unique_client_id)
        # Test connection
        client.ping()
        yield client
        client.close()
    except Exception as e:
        pytest.skip(f"KubeMQ server not available: {e}")


@pytest.fixture
def sync_cq_client(kubemq_address: str, unique_client_id: str):
    """
    Sync CQ (Commands/Queries) client fixture.

    Yields a connected CQ client and closes it after the test.
    Skips if KubeMQ server is not available.
    """
    from kubemq.cq import Client

    try:
        client = Client(address=kubemq_address, client_id=unique_client_id)
        # Test connection
        client.ping()
        yield client
        client.close()
    except Exception as e:
        pytest.skip(f"KubeMQ server not available: {e}")


@pytest.fixture
async def async_pubsub_client(kubemq_address: str, unique_client_id: str):
    """
    Async PubSub client fixture.

    Yields a connected async PubSub client and closes it after the test.
    Skips if KubeMQ server is not available.
    """
    from kubemq.pubsub import Client

    try:
        client = Client(address=kubemq_address, client_id=unique_client_id)
        # Test connection
        await client.ping_async()
        yield client
        await client.close_async()
    except Exception as e:
        pytest.skip(f"KubeMQ server not available: {e}")


@pytest.fixture
async def async_queues_client(kubemq_address: str, unique_client_id: str):
    """
    Async Queues client fixture.

    Yields a connected async Queues client and closes it after the test.
    Skips if KubeMQ server is not available.
    """
    from kubemq.queues import Client

    try:
        client = Client(address=kubemq_address, client_id=unique_client_id)
        # Test connection
        await client.ping_async()
        yield client
        await client.close_async()
    except Exception as e:
        pytest.skip(f"KubeMQ server not available: {e}")


@pytest.fixture
async def async_cq_client(kubemq_address: str, unique_client_id: str):
    """
    Async CQ client fixture.

    Yields a connected async CQ client and closes it after the test.
    Skips if KubeMQ server is not available.
    """
    from kubemq.cq import Client

    try:
        client = Client(address=kubemq_address, client_id=unique_client_id)
        # Test connection
        await client.ping_async()
        yield client
        await client.close_async()
    except Exception as e:
        pytest.skip(f"KubeMQ server not available: {e}")


# ==============================================================================
# Integration Test Cleanup
# ==============================================================================


@pytest.fixture(autouse=True)
def _integration_cleanup(request):
    """Auto-cleanup for integration tests.

    Ensures any client fixtures created during the test are properly closed
    even if the test fails.
    """
    if "integration" not in [m.name for m in request.node.iter_markers()]:
        yield
        return

    yield

    for fixture_name in request.fixturenames:
        try:
            client = request.getfixturevalue(fixture_name)
        except Exception:
            continue
        if client is not None and hasattr(client, "close"):
            try:
                client.close()
            except Exception:
                pass


# ==============================================================================
# Shared Unit Test Fixtures
# ==============================================================================


@pytest.fixture
def mock_client_config():
    """Reusable ClientConfig for testing."""
    from kubemq.core.config import ClientConfig

    return ClientConfig(
        address="localhost:50000",
        client_id="test-client",
    )


@pytest.fixture
def mock_server_info():
    """Reusable ServerInfo for testing."""
    from kubemq.transport.server_info import ServerInfo

    return ServerInfo(
        host="localhost",
        version="2.3.0",
        server_start_time=1704067200,
        server_up_time_seconds=3600,
    )


@pytest.fixture
def mock_grpc_stub():
    """Reusable mock for gRPC stubs."""
    stub = MagicMock()
    stub.Ping = Mock(return_value=Mock(Host="localhost", Version="2.3.0"))
    return stub


@pytest.fixture
def mock_async_grpc_stub():
    """Reusable async mock for gRPC stubs."""
    stub = MagicMock()
    stub.Ping = AsyncMock(return_value=Mock(Host="localhost", Version="2.3.0"))
    return stub


@pytest.fixture
def mock_async_transport():
    """Reusable mock async transport."""
    from kubemq.transport.async_transport import AsyncTransport

    transport = MagicMock(spec=AsyncTransport)
    transport.ping = AsyncMock()
    transport.is_connected = True
    transport._connected = True
    return transport


@pytest.fixture
def mock_sync_transport():
    """Reusable mock sync transport."""
    from kubemq.transport.transport import SyncTransport

    transport = MagicMock(spec=SyncTransport)
    transport.ping = Mock()
    transport.is_connected = True
    return transport


@pytest.fixture
def sample_queue_message():
    """Factory for QueueMessage test instances."""
    from kubemq.queues import QueueMessage

    def _factory(channel="test-channel", body=b"test body", **kwargs):
        return QueueMessage(channel=channel, body=body, **kwargs)

    return _factory


@pytest.fixture
def sample_event_message():
    """Factory for EventMessage test instances."""
    from kubemq.pubsub import EventMessage

    def _factory(channel="test-channel", body=b"test body", **kwargs):
        return EventMessage(channel=channel, body=body, **kwargs)

    return _factory


@pytest.fixture
def sample_event_store_message():
    """Factory for EventStoreMessage test instances."""
    from kubemq.pubsub import EventStoreMessage

    def _factory(channel="test-channel", body=b"test body", **kwargs):
        return EventStoreMessage(channel=channel, body=body, **kwargs)

    return _factory


@pytest.fixture
def sample_command_message():
    """Factory for CommandMessage test instances."""
    from kubemq.cq import CommandMessage

    def _factory(channel="test-channel", body=b"test body", timeout_in_seconds=10, **kwargs):
        return CommandMessage(
            channel=channel, body=body, timeout_in_seconds=timeout_in_seconds, **kwargs
        )

    return _factory


@pytest.fixture
def sample_query_message():
    """Factory for QueryMessage test instances."""
    from kubemq.cq import QueryMessage

    def _factory(channel="test-channel", body=b"test body", timeout_in_seconds=10, **kwargs):
        return QueryMessage(
            channel=channel, body=body, timeout_in_seconds=timeout_in_seconds, **kwargs
        )

    return _factory
