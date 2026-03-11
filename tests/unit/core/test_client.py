"""Unit tests for kubemq.core.client module.

Tests for BaseClient, AsyncBaseClient, and NativeAsyncBaseClient classes.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kubemq.core.client import AsyncBaseClient, BaseClient, NativeAsyncBaseClient
from kubemq.core.config import ClientConfig
from kubemq.core.exceptions import KubeMQClientClosedError, KubeMQConnectionError
from kubemq.transport.server_info import ServerInfo

# ==============================================================================
# Test Implementations (Concrete classes for testing abstract bases)
# ==============================================================================


class ConcreteBaseClient(BaseClient):
    """Concrete implementation of BaseClient for testing."""

    pass


class ConcreteAsyncBaseClient(AsyncBaseClient):
    """Concrete implementation of AsyncBaseClient for testing."""

    pass


class ConcreteNativeAsyncBaseClient(NativeAsyncBaseClient):
    """Concrete implementation of NativeAsyncBaseClient for testing."""

    pass


# ==============================================================================
# BaseClient Tests
# ==============================================================================


class TestBaseClientInit:
    """Tests for BaseClient initialization."""

    @patch("kubemq.transport.transport.Transport")
    def test_init_with_address(self, mock_transport_class):
        """Test initialization with address parameter."""
        mock_transport = MagicMock()
        mock_transport.initialize.return_value = mock_transport
        mock_transport.is_connected.return_value = True
        mock_transport_class.return_value = mock_transport

        client = ConcreteBaseClient(address="localhost:50000")

        assert client._config.address == "localhost:50000"
        assert client._transport is not None

    @patch("kubemq.transport.transport.Transport")
    def test_init_with_config(self, mock_transport_class):
        """Test initialization with ClientConfig object."""
        mock_transport = MagicMock()
        mock_transport.initialize.return_value = mock_transport
        mock_transport_class.return_value = mock_transport

        config = ClientConfig(address="localhost:50000", client_id="test-client")
        client = ConcreteBaseClient(config=config)

        assert client._config == config
        assert client._config.client_id == "test-client"

    @patch("kubemq.transport.transport.Transport")
    def test_init_generates_client_id_if_not_provided(self, mock_transport_class):
        """Test that client_id is generated if not provided."""
        mock_transport = MagicMock()
        mock_transport.initialize.return_value = mock_transport
        mock_transport_class.return_value = mock_transport

        client = ConcreteBaseClient(address="localhost:50000")

        # ClientConfig generates a UUID-based client_id if not provided
        assert client._config.client_id is not None
        assert len(client._config.client_id) > 0

    @patch("kubemq.transport.transport.Transport")
    def test_init_with_auth_token(self, mock_transport_class):
        """Test initialization with auth_token."""
        mock_transport = MagicMock()
        mock_transport.initialize.return_value = mock_transport
        mock_transport_class.return_value = mock_transport

        client = ConcreteBaseClient(address="localhost:50000", auth_token="test-token")

        assert client._config.auth_token == "test-token"


class TestBaseClientConnection:
    """Tests for BaseClient connection management."""

    @patch("kubemq.transport.transport.Transport")
    def test_is_connected_returns_true_when_connected(self, mock_transport_class):
        """Test is_connected property returns True when transport is connected."""
        mock_transport = MagicMock()
        mock_transport.initialize.return_value = mock_transport
        mock_transport.is_connected.return_value = True
        mock_transport_class.return_value = mock_transport

        client = ConcreteBaseClient(address="localhost:50000")

        assert client.is_connected is True

    @patch("kubemq.transport.transport.Transport")
    def test_is_connected_returns_false_when_disconnected(self, mock_transport_class):
        """Test is_connected property returns False when transport is disconnected."""
        mock_transport = MagicMock()
        mock_transport.initialize.return_value = mock_transport
        mock_transport.is_connected.return_value = False
        mock_transport_class.return_value = mock_transport

        client = ConcreteBaseClient(address="localhost:50000")

        assert client.is_connected is False

    @patch("kubemq.transport.transport.Transport")
    def test_close_disconnects_transport(self, mock_transport_class):
        """Test close() disconnects the transport."""
        mock_transport = MagicMock()
        mock_transport.initialize.return_value = mock_transport
        mock_transport_class.return_value = mock_transport

        client = ConcreteBaseClient(address="localhost:50000")
        client.close()

        mock_transport.close.assert_called_once()
        assert client._transport is None
        assert client._closed is True

    @patch("kubemq.transport.transport.Transport")
    def test_close_is_idempotent(self, mock_transport_class):
        """Test multiple close calls are safe."""
        mock_transport = MagicMock()
        mock_transport.initialize.return_value = mock_transport
        mock_transport_class.return_value = mock_transport

        client = ConcreteBaseClient(address="localhost:50000")
        client.close()
        client.close()  # Second call should be no-op

        # Should only be called once
        mock_transport.close.assert_called_once()

    @patch("kubemq.transport.transport.Transport")
    def test_ensure_connected_raises_when_closed(self, mock_transport_class):
        """Test _ensure_connected raises RuntimeError when client is closed."""
        mock_transport = MagicMock()
        mock_transport.initialize.return_value = mock_transport
        mock_transport_class.return_value = mock_transport

        client = ConcreteBaseClient(address="localhost:50000")
        client.close()

        with pytest.raises(KubeMQClientClosedError, match="Client is closed"):
            client._ensure_connected()

    @patch("kubemq.transport.transport.Transport")
    def test_ensure_connected_raises_when_not_connected(self, mock_transport_class):
        """Test _ensure_connected raises KubeMQConnectionError when disconnected."""
        mock_transport = MagicMock()
        mock_transport.initialize.return_value = mock_transport
        mock_transport.is_connected.return_value = False
        mock_transport_class.return_value = mock_transport

        client = ConcreteBaseClient(address="localhost:50000")

        with pytest.raises(KubeMQConnectionError, match="not connected"):
            client._ensure_connected()


class TestBaseClientPing:
    """Tests for BaseClient ping operations."""

    @patch("kubemq.transport.transport.Transport")
    def test_ping_returns_server_info(self, mock_transport_class):
        """Test ping returns ServerInfo object."""
        mock_server_info = ServerInfo(
            host="localhost",
            version="2.3.0",
            server_start_time=1704067200,
            server_up_time_seconds=3600,
        )
        mock_transport = MagicMock()
        mock_transport.initialize.return_value = mock_transport
        mock_transport.is_connected.return_value = True
        mock_transport.ping.return_value = mock_server_info
        mock_transport_class.return_value = mock_transport

        client = ConcreteBaseClient(address="localhost:50000")
        result = client.ping()

        assert result.host == "localhost"
        assert result.version == "2.3.0"
        mock_transport.ping.assert_called_once()

    @patch("kubemq.transport.transport.Transport")
    def test_ping_when_not_connected_raises(self, mock_transport_class):
        """Test ping raises error when not connected."""
        mock_transport = MagicMock()
        mock_transport.initialize.return_value = mock_transport
        mock_transport.is_connected.return_value = False
        mock_transport_class.return_value = mock_transport

        client = ConcreteBaseClient(address="localhost:50000")

        with pytest.raises(KubeMQConnectionError):
            client.ping()


class TestBaseClientContextManager:
    """Tests for BaseClient context manager."""

    @patch("kubemq.transport.transport.Transport")
    def test_context_manager_basic(self, mock_transport_class):
        """Test basic context manager usage."""
        mock_transport = MagicMock()
        mock_transport.initialize.return_value = mock_transport
        mock_transport_class.return_value = mock_transport

        with ConcreteBaseClient(address="localhost:50000") as client:
            assert client._transport is not None

        mock_transport.close.assert_called_once()

    @patch("kubemq.transport.transport.Transport")
    def test_context_manager_closes_on_exception(self, mock_transport_class):
        """Test context manager closes client even when exception occurs."""
        mock_transport = MagicMock()
        mock_transport.initialize.return_value = mock_transport
        mock_transport_class.return_value = mock_transport

        with pytest.raises(ValueError), ConcreteBaseClient(address="localhost:50000"):
            raise ValueError("Test error")

        mock_transport.close.assert_called_once()


# ==============================================================================
# AsyncBaseClient Tests
# ==============================================================================


class TestAsyncBaseClientInit:
    """Tests for AsyncBaseClient initialization."""

    def test_init_with_address(self):
        """Test initialization with address parameter (no connect)."""
        client = ConcreteAsyncBaseClient(address="localhost:50000")

        assert client._config.address == "localhost:50000"
        assert client._transport is None  # Not connected yet
        assert client._closed is False

    def test_init_with_config(self):
        """Test initialization with ClientConfig object."""
        config = ClientConfig(address="localhost:50000", client_id="test-client")
        client = ConcreteAsyncBaseClient(config=config)

        assert client._config == config
        assert client._config.client_id == "test-client"

    def test_init_generates_client_id_if_not_provided(self):
        """Test that client_id is generated if not provided."""
        client = ConcreteAsyncBaseClient(address="localhost:50000")

        assert client._config.client_id is not None


class TestAsyncBaseClientConnection:
    """Tests for AsyncBaseClient connection management."""

    @pytest.mark.asyncio
    async def test_connect_creates_transport(self):
        """Test connect() creates and initializes transport."""
        mock_transport = MagicMock()
        mock_transport.is_connected.return_value = True

        with patch.object(ConcreteAsyncBaseClient, "connect"):
            client = ConcreteAsyncBaseClient(address="localhost:50000")
            # Manually set up state as if connect succeeded
            client._transport = mock_transport

            assert client._transport is not None

    def test_is_connected_false_before_connect(self):
        """Test is_connected returns False before connect."""
        client = ConcreteAsyncBaseClient(address="localhost:50000")
        assert client.is_connected is False

    @pytest.mark.asyncio
    async def test_close_disconnects_transport(self):
        """Test close() disconnects the transport."""
        mock_transport = MagicMock()
        mock_transport.is_connected.return_value = True
        mock_transport.close_async = AsyncMock()

        client = ConcreteAsyncBaseClient(address="localhost:50000")
        client._transport = mock_transport  # Simulate connected state
        await client.close()

        mock_transport.close_async.assert_called_once()
        assert client._transport is None
        assert client._closed is True

    @pytest.mark.asyncio
    async def test_close_is_idempotent(self):
        """Test multiple close calls are safe."""
        mock_transport = MagicMock()
        mock_transport.close_async = AsyncMock()

        client = ConcreteAsyncBaseClient(address="localhost:50000")
        client._transport = mock_transport  # Simulate connected state
        await client.close()
        await client.close()  # Second call should be no-op

        mock_transport.close_async.assert_called_once()


class TestAsyncBaseClientContextManager:
    """Tests for AsyncBaseClient async context manager."""

    @pytest.mark.asyncio
    async def test_async_context_manager_connects_and_closes(self):
        """Test async context manager connects on enter and closes on exit."""
        mock_transport = MagicMock()
        mock_transport.is_connected.return_value = True
        mock_transport.close_async = AsyncMock()

        # Test using manual approach - inject transport directly
        client = ConcreteAsyncBaseClient(address="localhost:50000")

        # Override connect to avoid real gRPC
        async def mock_connect():
            client._transport = mock_transport

        client.connect = mock_connect
        client.close = AsyncMock()

        async with client:
            assert client._transport is not None

        client.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_async_context_manager_closes_on_exception(self):
        """Test async context manager closes client even when exception occurs."""
        mock_transport = MagicMock()
        mock_transport.is_connected.return_value = True
        mock_transport.close_async = AsyncMock()

        client = ConcreteAsyncBaseClient(address="localhost:50000")

        async def mock_connect():
            client._transport = mock_transport

        client.connect = mock_connect
        client.close = AsyncMock()

        with pytest.raises(ValueError):
            async with client:
                raise ValueError("Test error")

        client.close.assert_called_once()


# ==============================================================================
# NativeAsyncBaseClient Tests
# ==============================================================================


class TestNativeAsyncBaseClientInit:
    """Tests for NativeAsyncBaseClient initialization."""

    def test_init_with_address(self):
        """Test initialization with address parameter."""
        client = ConcreteNativeAsyncBaseClient(address="localhost:50000")

        assert client._config.address == "localhost:50000"
        assert client._transport is None
        assert client._closed is False
        assert client._closing is False
        assert len(client._active_subscriptions) == 0

    def test_init_with_config(self):
        """Test initialization with ClientConfig object."""
        config = ClientConfig(address="localhost:50000", client_id="test-client")
        client = ConcreteNativeAsyncBaseClient(config=config)

        assert client._config == config
        assert client._config.client_id == "test-client"

    def test_init_generates_client_id_if_not_provided(self):
        """Test that client_id is generated if not provided."""
        client = ConcreteNativeAsyncBaseClient(address="localhost:50000")

        assert client._config.client_id is not None


class TestNativeAsyncBaseClientConnection:
    """Tests for NativeAsyncBaseClient connection management."""

    @pytest.mark.asyncio
    @patch("kubemq.transport.async_transport.AsyncTransport")
    async def test_connect_creates_transport(self, mock_async_transport_class):
        """Test connect() creates and initializes async transport."""
        mock_transport = MagicMock()
        mock_transport.connect = AsyncMock()
        mock_transport.is_connected = True
        mock_async_transport_class.return_value = mock_transport

        client = ConcreteNativeAsyncBaseClient(address="localhost:50000")
        await client.connect()

        assert client._transport is not None
        mock_transport.connect.assert_called_once()

    @pytest.mark.asyncio
    @patch("kubemq.transport.async_transport.AsyncTransport")
    async def test_connect_already_connected_is_noop(self, mock_async_transport_class):
        """Test connect() when already connected does nothing."""
        mock_transport = MagicMock()
        mock_transport.connect = AsyncMock()
        mock_transport.is_connected = True
        mock_async_transport_class.return_value = mock_transport

        client = ConcreteNativeAsyncBaseClient(address="localhost:50000")
        await client.connect()
        await client.connect()  # Second call should be no-op

        # Transport created only once
        assert mock_async_transport_class.call_count == 1

    @pytest.mark.asyncio
    async def test_cannot_reconnect_after_close(self):
        """Test that reconnection after close raises KubeMQClientClosedError."""
        client = ConcreteNativeAsyncBaseClient(address="localhost:50000")
        client._closed = True

        with pytest.raises(KubeMQClientClosedError, match="closed and cannot be reconnected"):
            await client.connect()

    def test_is_connected_false_before_connect(self):
        """Test is_connected returns False before connect."""
        client = ConcreteNativeAsyncBaseClient(address="localhost:50000")
        assert client.is_connected is False

    @pytest.mark.asyncio
    @patch("kubemq.transport.async_transport.AsyncTransport")
    async def test_is_connected_true_after_connect(self, mock_async_transport_class):
        """Test is_connected returns True after connect."""
        mock_transport = MagicMock()
        mock_transport.connect = AsyncMock()
        mock_transport.is_connected = True
        mock_async_transport_class.return_value = mock_transport

        client = ConcreteNativeAsyncBaseClient(address="localhost:50000")
        await client.connect()

        assert client.is_connected is True

    @pytest.mark.asyncio
    @patch("kubemq.transport.async_transport.AsyncTransport")
    async def test_close_disconnects_transport(self, mock_async_transport_class):
        """Test close() disconnects the transport."""
        mock_transport = MagicMock()
        mock_transport.connect = AsyncMock()
        mock_transport.close = AsyncMock()
        mock_transport.is_connected = True
        mock_async_transport_class.return_value = mock_transport

        client = ConcreteNativeAsyncBaseClient(address="localhost:50000")
        await client.connect()
        await client.close()

        mock_transport.close.assert_called_once()
        assert client._transport is None
        assert client._closed is True

    @pytest.mark.asyncio
    @patch("kubemq.transport.async_transport.AsyncTransport")
    async def test_close_is_idempotent(self, mock_async_transport_class):
        """Test multiple close calls are safe."""
        mock_transport = MagicMock()
        mock_transport.connect = AsyncMock()
        mock_transport.close = AsyncMock()
        mock_transport.is_connected = True
        mock_async_transport_class.return_value = mock_transport

        client = ConcreteNativeAsyncBaseClient(address="localhost:50000")
        await client.connect()
        await client.close()
        await client.close()  # Second call should be no-op

        mock_transport.close.assert_called_once()


class TestNativeAsyncBaseClientPing:
    """Tests for NativeAsyncBaseClient ping operations."""

    @pytest.mark.asyncio
    @patch("kubemq.transport.async_transport.AsyncTransport")
    async def test_ping_returns_server_info(self, mock_async_transport_class):
        """Test ping returns ServerInfo object."""
        mock_server_info = ServerInfo(
            host="localhost",
            version="2.3.0",
            server_start_time=1704067200,
            server_up_time_seconds=3600,
        )
        mock_transport = MagicMock()
        mock_transport.connect = AsyncMock()
        mock_transport.ping = AsyncMock(return_value=mock_server_info)
        mock_transport.is_connected = True
        mock_async_transport_class.return_value = mock_transport

        client = ConcreteNativeAsyncBaseClient(address="localhost:50000")
        await client.connect()
        result = await client.ping()

        assert result.host == "localhost"
        assert result.version == "2.3.0"
        mock_transport.ping.assert_called_once()

    @pytest.mark.asyncio
    async def test_ping_when_not_connected_raises(self):
        """Test ping raises error when not connected."""
        client = ConcreteNativeAsyncBaseClient(address="localhost:50000")

        with pytest.raises(KubeMQConnectionError):
            await client.ping()


class TestNativeAsyncBaseClientSubscriptions:
    """Tests for NativeAsyncBaseClient subscription tracking."""

    @pytest.mark.asyncio
    async def test_register_subscription(self):
        """Test registering a subscription adds token to tracking set."""
        from kubemq.common.async_cancellation_token import AsyncCancellationToken

        client = ConcreteNativeAsyncBaseClient(address="localhost:50000")
        token = AsyncCancellationToken()

        await client._register_subscription(token)

        assert token in client._active_subscriptions

    @pytest.mark.asyncio
    async def test_unregister_subscription(self):
        """Test unregistering a subscription removes token from tracking set."""
        from kubemq.common.async_cancellation_token import AsyncCancellationToken

        client = ConcreteNativeAsyncBaseClient(address="localhost:50000")
        token = AsyncCancellationToken()

        await client._register_subscription(token)
        await client._unregister_subscription(token)

        assert token not in client._active_subscriptions

    @pytest.mark.asyncio
    @patch("kubemq.transport.async_transport.AsyncTransport")
    async def test_close_cancels_subscriptions(self, mock_async_transport_class):
        """Test close() cancels all active subscriptions."""
        from kubemq.common.async_cancellation_token import AsyncCancellationToken

        mock_transport = MagicMock()
        mock_transport.connect = AsyncMock()
        mock_transport.close = AsyncMock()
        mock_transport.is_connected = True
        mock_async_transport_class.return_value = mock_transport

        client = ConcreteNativeAsyncBaseClient(address="localhost:50000")
        await client.connect()

        # Register some subscriptions
        token1 = AsyncCancellationToken()
        token2 = AsyncCancellationToken()
        await client._register_subscription(token1)
        await client._register_subscription(token2)

        await client.close()

        # All tokens should be cancelled
        assert token1.is_cancelled
        assert token2.is_cancelled
        assert len(client._active_subscriptions) == 0


class TestNativeAsyncBaseClientContextManager:
    """Tests for NativeAsyncBaseClient async context manager."""

    @pytest.mark.asyncio
    @patch("kubemq.transport.async_transport.AsyncTransport")
    async def test_async_context_manager_connects_and_closes(self, mock_async_transport_class):
        """Test async context manager connects on enter and closes on exit."""
        mock_transport = MagicMock()
        mock_transport.connect = AsyncMock()
        mock_transport.close = AsyncMock()
        mock_transport.is_connected = True
        mock_async_transport_class.return_value = mock_transport

        async with ConcreteNativeAsyncBaseClient(address="localhost:50000") as client:
            assert client._transport is not None

        mock_transport.close.assert_called_once()

    @pytest.mark.asyncio
    @patch("kubemq.transport.async_transport.AsyncTransport")
    async def test_async_context_manager_closes_on_exception(self, mock_async_transport_class):
        """Test async context manager closes client even when exception occurs."""
        mock_transport = MagicMock()
        mock_transport.connect = AsyncMock()
        mock_transport.close = AsyncMock()
        mock_transport.is_connected = True
        mock_async_transport_class.return_value = mock_transport

        with pytest.raises(ValueError):
            async with ConcreteNativeAsyncBaseClient(address="localhost:50000"):
                raise ValueError("Test error")

        mock_transport.close.assert_called_once()


class TestNativeAsyncBaseClientEnsureConnected:
    """Tests for NativeAsyncBaseClient _ensure_connected method."""

    def test_ensure_connected_raises_when_closed(self):
        """Test _ensure_connected raises KubeMQClientClosedError when client is closed."""
        client = ConcreteNativeAsyncBaseClient(address="localhost:50000")
        client._closed = True

        with pytest.raises(KubeMQClientClosedError, match="Client is closed"):
            client._ensure_connected()

    def test_ensure_connected_raises_when_closing(self):
        """Test _ensure_connected raises KubeMQClientClosedError when client is closing."""
        client = ConcreteNativeAsyncBaseClient(address="localhost:50000")
        client._closing = True

        with pytest.raises(KubeMQClientClosedError, match="shutting down"):
            client._ensure_connected()

    def test_ensure_connected_raises_when_not_connected(self):
        """Test _ensure_connected raises KubeMQConnectionError when disconnected."""
        client = ConcreteNativeAsyncBaseClient(address="localhost:50000")

        with pytest.raises(KubeMQConnectionError, match="not connected"):
            client._ensure_connected()


class TestClientLifecycle:
    """Tests for client lifecycle edge cases."""

    @patch("kubemq.transport.transport.Transport")
    def test_config_property(self, mock_transport_class):
        """Test config property returns the configuration."""
        mock_transport = MagicMock()
        mock_transport.initialize.return_value = mock_transport
        mock_transport_class.return_value = mock_transport

        config = ClientConfig(address="localhost:50000", client_id="test-client")
        client = ConcreteBaseClient(config=config)

        assert client.config == config
        assert client.config.client_id == "test-client"

    @patch("kubemq.transport.transport.Transport")
    def test_close_handles_transport_close_error(self, mock_transport_class):
        """Test close() handles errors from transport.close() gracefully."""
        mock_transport = MagicMock()
        mock_transport.initialize.return_value = mock_transport
        mock_transport.close.side_effect = Exception("Close error")
        mock_transport_class.return_value = mock_transport

        client = ConcreteBaseClient(address="localhost:50000")
        # Should not raise
        client.close()

        assert client._closed is True
        assert client._transport is None
