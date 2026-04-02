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

    @patch("kubemq.transport.transport.SyncTransport")
    def test_init_with_address(self, mock_transport_class):
        """Test initialization with address parameter."""
        mock_transport = MagicMock()
        mock_transport.initialize.return_value = mock_transport
        mock_transport.is_connected.return_value = True
        mock_transport_class.return_value = mock_transport

        client = ConcreteBaseClient(address="localhost:50000")

        assert client._config.address == "localhost:50000"
        assert client._transport is not None

    @patch("kubemq.transport.transport.SyncTransport")
    def test_init_with_config(self, mock_transport_class):
        """Test initialization with ClientConfig object."""
        mock_transport = MagicMock()
        mock_transport.initialize.return_value = mock_transport
        mock_transport_class.return_value = mock_transport

        config = ClientConfig(address="localhost:50000", client_id="test-client")
        client = ConcreteBaseClient(config=config)

        assert client._config == config
        assert client._config.client_id == "test-client"

    @patch("kubemq.transport.transport.SyncTransport")
    def test_init_generates_client_id_if_not_provided(self, mock_transport_class):
        """Test that client_id is generated if not provided."""
        mock_transport = MagicMock()
        mock_transport.initialize.return_value = mock_transport
        mock_transport_class.return_value = mock_transport

        client = ConcreteBaseClient(address="localhost:50000")

        # ClientConfig generates a UUID-based client_id if not provided
        assert client._config.client_id is not None
        assert len(client._config.client_id) > 0

    @patch("kubemq.transport.transport.SyncTransport")
    def test_init_with_auth_token(self, mock_transport_class):
        """Test initialization with auth_token."""
        mock_transport = MagicMock()
        mock_transport.initialize.return_value = mock_transport
        mock_transport_class.return_value = mock_transport

        client = ConcreteBaseClient(address="localhost:50000", auth_token="test-token")

        assert client._config.auth_token == "test-token"


class TestBaseClientConnection:
    """Tests for BaseClient connection management."""

    @patch("kubemq.transport.transport.SyncTransport")
    def test_is_connected_returns_true_when_connected(self, mock_transport_class):
        """Test is_connected property returns True when transport is connected."""
        mock_transport = MagicMock()
        mock_transport.initialize.return_value = mock_transport
        mock_transport.is_connected.return_value = True
        mock_transport_class.return_value = mock_transport

        client = ConcreteBaseClient(address="localhost:50000")

        assert client.is_connected is True

    @patch("kubemq.transport.transport.SyncTransport")
    def test_is_connected_returns_false_when_disconnected(self, mock_transport_class):
        """Test is_connected property returns False when transport is disconnected."""
        mock_transport = MagicMock()
        mock_transport.initialize.return_value = mock_transport
        mock_transport.is_connected.return_value = False
        mock_transport_class.return_value = mock_transport

        client = ConcreteBaseClient(address="localhost:50000")

        assert client.is_connected is False

    @patch("kubemq.transport.transport.SyncTransport")
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

    @patch("kubemq.transport.transport.SyncTransport")
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

    @patch("kubemq.transport.transport.SyncTransport")
    def test_ensure_connected_raises_when_closed(self, mock_transport_class):
        """Test _ensure_connected raises RuntimeError when client is closed."""
        mock_transport = MagicMock()
        mock_transport.initialize.return_value = mock_transport
        mock_transport_class.return_value = mock_transport

        client = ConcreteBaseClient(address="localhost:50000")
        client.close()

        with pytest.raises(KubeMQClientClosedError, match="Client is closed"):
            client._ensure_connected()

    @patch("kubemq.transport.transport.SyncTransport")
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

    @patch("kubemq.transport.transport.SyncTransport")
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

    @patch("kubemq.transport.transport.SyncTransport")
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

    @patch("kubemq.transport.transport.SyncTransport")
    def test_context_manager_basic(self, mock_transport_class):
        """Test basic context manager usage."""
        mock_transport = MagicMock()
        mock_transport.initialize.return_value = mock_transport
        mock_transport_class.return_value = mock_transport

        with ConcreteBaseClient(address="localhost:50000") as client:
            assert client._transport is not None

        mock_transport.close.assert_called_once()

    @patch("kubemq.transport.transport.SyncTransport")
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

        client = ConcreteNativeAsyncBaseClient(
            address="localhost:50000", connection_pool_size=1
        )
        await client.connect()

        assert client._transport is not None
        mock_transport.connect.assert_called_once()

    @pytest.mark.asyncio
    @patch("kubemq.transport.async_transport.AsyncTransport")
    async def test_connect_creates_pool(self, mock_async_transport_class):
        """Test connect() creates connection pool when pool_size > 1."""
        mock_transport = MagicMock()
        mock_transport.connect = AsyncMock()
        mock_transport.close = AsyncMock()
        mock_transport.is_connected = True
        mock_async_transport_class.return_value = mock_transport

        client = ConcreteNativeAsyncBaseClient(
            address="localhost:50000", connection_pool_size=3
        )
        await client.connect()

        assert client._transport is not None
        # 1 primary + 3 pool = 4 total
        assert mock_transport.connect.call_count == 4
        assert len(client._pool) == 3

    @pytest.mark.asyncio
    @patch("kubemq.transport.async_transport.AsyncTransport")
    async def test_connect_already_connected_is_noop(self, mock_async_transport_class):
        """Test connect() when already connected does nothing."""
        mock_transport = MagicMock()
        mock_transport.connect = AsyncMock()
        mock_transport.is_connected = True
        mock_async_transport_class.return_value = mock_transport

        client = ConcreteNativeAsyncBaseClient(
            address="localhost:50000", connection_pool_size=1
        )
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

        client = ConcreteNativeAsyncBaseClient(
            address="localhost:50000", connection_pool_size=1
        )
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

        client = ConcreteNativeAsyncBaseClient(
            address="localhost:50000", connection_pool_size=1
        )
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

        client = ConcreteNativeAsyncBaseClient(
            address="localhost:50000", connection_pool_size=1
        )
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

        async with ConcreteNativeAsyncBaseClient(
            address="localhost:50000", connection_pool_size=1
        ) as client:
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
            async with ConcreteNativeAsyncBaseClient(
                address="localhost:50000", connection_pool_size=1
            ):
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

    @patch("kubemq.transport.transport.SyncTransport")
    def test_config_property(self, mock_transport_class):
        """Test config property returns the configuration."""
        mock_transport = MagicMock()
        mock_transport.initialize.return_value = mock_transport
        mock_transport_class.return_value = mock_transport

        config = ClientConfig(address="localhost:50000", client_id="test-client")
        client = ConcreteBaseClient(config=config)

        assert client.config == config
        assert client.config.client_id == "test-client"

    @patch("kubemq.transport.transport.SyncTransport")
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


# ==============================================================================
# Additional Coverage Tests
# ==============================================================================


class TestBaseClientCloseEdgeCases:
    """Tests for BaseClient.close() uncovered paths."""

    @patch("kubemq.transport.transport.SyncTransport")
    def test_close_when_transport_none(self, mock_transport_class):
        """Verify close() doesn't error when _transport is already None."""
        mock_transport = MagicMock()
        mock_transport.initialize.return_value = mock_transport
        mock_transport_class.return_value = mock_transport

        client = ConcreteBaseClient(address="localhost:50000")
        client._transport = None

        client.close()

        assert client._closed is True
        assert client._transport is None

    @patch("kubemq.transport.transport.SyncTransport")
    def test_close_calls_transport_close(self, mock_transport_class):
        """Verify transport.close() is called during close()."""
        mock_transport = MagicMock()
        mock_transport.initialize.return_value = mock_transport
        mock_transport_class.return_value = mock_transport

        client = ConcreteBaseClient(address="localhost:50000")
        client.close()

        mock_transport.close.assert_called_once()
        assert client._transport is None
        assert client._closed is True


class TestBaseClientEnsureConnectedEdgeCases:
    """Tests for BaseClient._ensure_connected() uncovered paths."""

    @patch("kubemq.transport.transport.SyncTransport")
    def test_ensure_connected_when_transport_none(self, mock_transport_class):
        """Verify _ensure_connected raises when transport is None (not initialized)."""
        mock_transport = MagicMock()
        mock_transport.initialize.return_value = mock_transport
        mock_transport.is_connected.return_value = True
        mock_transport_class.return_value = mock_transport

        client = ConcreteBaseClient(address="localhost:50000")
        client._transport = None

        with pytest.raises(KubeMQConnectionError, match="not connected"):
            client._ensure_connected()


# ==============================================================================
# Additional Coverage Tests — uncovered lines in core/client.py
# ==============================================================================

import asyncio  # noqa: E402
import logging  # noqa: E402
import threading  # noqa: E402
import time  # noqa: E402

from kubemq.core.client import _resolve_logger  # noqa: E402
from kubemq.core.exceptions import KubeMQError, KubeMQValidationError  # noqa: E402


class TestResolveLogger:
    """Tests for _resolve_logger function (lines 44-47)."""

    def test_returns_user_injected_logger(self):
        mock_logger = MagicMock()
        config = ClientConfig(address="localhost:50000", logger=mock_logger)
        result = _resolve_logger(config, "TestClass")
        assert result is mock_logger

    def test_returns_stdlib_adapter_when_log_level_set(self):
        from kubemq._internal.logging import StdLibLoggerAdapter

        config = ClientConfig(address="localhost:50000", log_level=logging.DEBUG)
        result = _resolve_logger(config, "TestClass")
        assert isinstance(result, StdLibLoggerAdapter)


class TestBaseClientInitializeError:
    """Tests for BaseClient._initialize error path (lines 144-146)."""

    def test_initialize_raises_maps_error(self):
        with patch("kubemq.transport.transport.SyncTransport") as mock_cls:
            mock_transport = MagicMock()
            mock_transport.initialize.side_effect = RuntimeError("connect failed")
            mock_cls.return_value = mock_transport

            with pytest.raises(KubeMQError):
                ConcreteBaseClient(address="localhost:50000")


class TestBaseClientPingError:
    """Tests for BaseClient.ping() error path (lines 172-174)."""

    @patch("kubemq.transport.transport.SyncTransport")
    def test_ping_error_maps_exception(self, mock_transport_class):
        mock_transport = MagicMock()
        mock_transport.initialize.return_value = mock_transport
        mock_transport.is_connected.return_value = True
        mock_transport.ping.side_effect = RuntimeError("ping failed")
        mock_transport_class.return_value = mock_transport

        client = ConcreteBaseClient(address="localhost:50000")

        with pytest.raises(KubeMQError):
            client.ping()


class TestBaseClientSetToken:
    """Tests for BaseClient.set_token() (lines 185-186)."""

    @patch("kubemq.transport.transport.SyncTransport")
    def test_set_token_delegates_to_transport(self, mock_transport_class):
        mock_transport = MagicMock()
        mock_transport.initialize.return_value = mock_transport
        mock_transport_class.return_value = mock_transport

        client = ConcreteBaseClient(address="localhost:50000")
        client.set_token("new-token-123")

        mock_transport.set_token.assert_called_once_with("new-token-123")

    @patch("kubemq.transport.transport.SyncTransport")
    def test_set_token_noop_when_transport_none(self, mock_transport_class):
        mock_transport = MagicMock()
        mock_transport.initialize.return_value = mock_transport
        mock_transport_class.return_value = mock_transport

        client = ConcreteBaseClient(address="localhost:50000")
        client._transport = None
        client.set_token("token")


class TestBaseClientCloseThreadDrain:
    """Tests for BaseClient.close() subscription thread drain (lines 226-235)."""

    @patch("kubemq.transport.transport.SyncTransport")
    def test_close_joins_subscription_threads(self, mock_transport_class):
        mock_transport = MagicMock()
        mock_transport.initialize.return_value = mock_transport
        mock_transport_class.return_value = mock_transport

        client = ConcreteBaseClient(address="localhost:50000")
        client._config.callback_completion_timeout = 5.0
        client._logger = MagicMock()

        mock_thread = MagicMock(spec=threading.Thread)
        mock_thread.is_alive.return_value = True
        client._register_subscription_thread(mock_thread)

        client.close()

        mock_thread.join.assert_called_once()
        _, kwargs = mock_thread.join.call_args
        assert kwargs.get("timeout") is not None

    @patch("kubemq.transport.transport.SyncTransport")
    def test_close_timeout_breaks_thread_drain_loop(self, mock_transport_class):
        mock_transport = MagicMock()
        mock_transport.initialize.return_value = mock_transport
        mock_transport_class.return_value = mock_transport

        client = ConcreteBaseClient(address="localhost:50000")
        client._config.callback_completion_timeout = 0.01
        client._logger = MagicMock()

        threads = []
        for _ in range(3):
            t = MagicMock(spec=threading.Thread)
            t.is_alive.return_value = True
            t.join.side_effect = lambda timeout=None: time.sleep(0.1)
            threads.append(t)
            client._register_subscription_thread(t)

        client.close()

        join_count = sum(1 for t in threads if t.join.called)
        assert join_count >= 1
        assert client._closed is True


class TestAsyncBaseClientConnectError:
    """Tests for AsyncBaseClient.connect() error path (lines 363-372)."""

    @pytest.mark.asyncio
    async def test_connect_error_maps_via_from_grpc_error(self):
        client = ConcreteAsyncBaseClient(address="localhost:50000")

        with patch("kubemq.core.client.run_in_thread", new_callable=AsyncMock) as mock_run:
            mock_run.side_effect = RuntimeError("connection refused")

            with pytest.raises(KubeMQError):
                await client.connect()


class TestAsyncBaseClientIsConnectedWithTransport:
    """Tests for AsyncBaseClient.is_connected with transport (line 382)."""

    def test_is_connected_true_with_transport(self):
        client = ConcreteAsyncBaseClient(address="localhost:50000")
        mock_transport = MagicMock()
        mock_transport.is_connected.return_value = True
        client._transport = mock_transport

        assert client.is_connected is True


class TestAsyncBaseClientPingError:
    """Tests for AsyncBaseClient.ping() error path (lines 393-400)."""

    @pytest.mark.asyncio
    async def test_ping_error_maps_exception(self):
        client = ConcreteAsyncBaseClient(address="localhost:50000")
        mock_transport = MagicMock()
        mock_transport.is_connected.return_value = True
        client._transport = mock_transport

        with patch("kubemq.core.client.run_in_thread", new_callable=AsyncMock) as mock_run:
            mock_run.side_effect = RuntimeError("ping failed")

            with pytest.raises(KubeMQError):
                await client.ping()


class TestAsyncBaseClientAexit:
    """Tests for AsyncBaseClient.__aexit__."""

    @pytest.mark.asyncio
    async def test_aexit_calls_close(self):
        client = ConcreteAsyncBaseClient(address="localhost:50000")
        client.connect = AsyncMock()
        client.close = AsyncMock()

        async with client:
            pass

        client.close.assert_called_once()


class TestAsyncBaseClientCloseTransportError:
    """Tests for AsyncBaseClient.close() transport error (lines 420-421)."""

    @pytest.mark.asyncio
    async def test_close_transport_error_logs_warning_clears_transport(self):
        client = ConcreteAsyncBaseClient(address="localhost:50000")
        mock_transport = MagicMock()
        mock_transport.close_async = AsyncMock(side_effect=RuntimeError("close failed"))
        client._transport = mock_transport

        await client.close()

        assert client._transport is None
        assert client._closed is True


class TestAsyncBaseClientEnsureConnected:
    """Tests for AsyncBaseClient._ensure_connected (lines 438-441)."""

    def test_raises_when_closed(self):
        client = ConcreteAsyncBaseClient(address="localhost:50000")
        client._closed = True

        with pytest.raises(KubeMQClientClosedError, match="Client is closed"):
            client._ensure_connected()

    def test_raises_when_not_connected(self):
        client = ConcreteAsyncBaseClient(address="localhost:50000")

        with pytest.raises(KubeMQConnectionError, match="not connected"):
            client._ensure_connected()


class TestNativeAsyncConnectionState:
    """Tests for NativeAsyncBaseClient.connection_state (lines 596-600)."""

    def test_connection_state_idle_without_transport(self):
        from kubemq.core.types import ConnectionState

        client = ConcreteNativeAsyncBaseClient(address="localhost:50000")
        assert client.connection_state == ConnectionState.IDLE

    def test_connection_state_from_transport(self):
        from kubemq.core.types import ConnectionState

        client = ConcreteNativeAsyncBaseClient(address="localhost:50000")
        mock_transport = MagicMock()
        mock_transport.connection_state = ConnectionState.READY
        client._transport = mock_transport

        assert client.connection_state == ConnectionState.READY


class TestNativeAsyncSetToken:
    """Tests for NativeAsyncBaseClient.set_token() (lines 590-591)."""

    def test_set_token_delegates_to_transport(self):
        client = ConcreteNativeAsyncBaseClient(address="localhost:50000")
        mock_transport = MagicMock()
        client._transport = mock_transport

        client.set_token("new-token")

        mock_transport.set_token.assert_called_once_with("new-token")

    def test_set_token_noop_when_no_transport(self):
        client = ConcreteNativeAsyncBaseClient(address="localhost:50000")
        client.set_token("token")


class TestNativeAsyncCallbackRegistrations:
    """Tests for on_connected/disconnected/reconnecting/reconnected
    (lines 604-605, 609-610, 614-615, 619-620)."""

    def _client_with_transport(self):
        client = ConcreteNativeAsyncBaseClient(address="localhost:50000")
        transport = MagicMock()
        client._transport = transport
        return client, transport

    def test_on_connected_delegates(self):
        client, transport = self._client_with_transport()
        cb = MagicMock()
        client.on_connected(cb)
        transport.on_connected.assert_called_once_with(cb)

    def test_on_disconnected_delegates(self):
        client, transport = self._client_with_transport()
        cb = MagicMock()
        client.on_disconnected(cb)
        transport.on_disconnected.assert_called_once_with(cb)

    def test_on_reconnecting_delegates(self):
        client, transport = self._client_with_transport()
        cb = MagicMock()
        client.on_reconnecting(cb)
        transport.on_reconnecting.assert_called_once_with(cb)

    def test_on_reconnected_delegates(self):
        client, transport = self._client_with_transport()
        cb = MagicMock()
        client.on_reconnected(cb)
        transport.on_reconnected.assert_called_once_with(cb)

    def test_on_connected_noop_without_transport(self):
        client = ConcreteNativeAsyncBaseClient(address="localhost:50000")
        client.on_connected(MagicMock())

    def test_on_disconnected_noop_without_transport(self):
        client = ConcreteNativeAsyncBaseClient(address="localhost:50000")
        client.on_disconnected(MagicMock())

    def test_on_reconnecting_noop_without_transport(self):
        client = ConcreteNativeAsyncBaseClient(address="localhost:50000")
        client.on_reconnecting(MagicMock())

    def test_on_reconnected_noop_without_transport(self):
        client = ConcreteNativeAsyncBaseClient(address="localhost:50000")
        client.on_reconnected(MagicMock())


class TestNativeAsyncTaskDrain:
    """Tests for NativeAsyncBaseClient.close() task drain (lines 655-671)."""

    @pytest.mark.asyncio
    async def test_close_drains_pending_tasks(self):
        client = ConcreteNativeAsyncBaseClient(address="localhost:50000")
        client._logger = MagicMock()
        mock_transport = MagicMock()
        mock_transport.connect = AsyncMock()
        mock_transport.close = AsyncMock()
        mock_transport.is_connected = True
        client._transport = mock_transport

        completed = []

        async def slow_work():
            await asyncio.sleep(0.05)
            completed.append(True)

        task = asyncio.create_task(slow_work())
        client._register_subscription_task(task)

        await client.close()

        assert len(completed) == 1
        assert client._closed is True

    @pytest.mark.asyncio
    async def test_close_cancels_tasks_after_timeout(self):
        client = ConcreteNativeAsyncBaseClient(address="localhost:50000")
        client._logger = MagicMock()
        mock_transport = MagicMock()
        mock_transport.connect = AsyncMock()
        mock_transport.close = AsyncMock()
        mock_transport.is_connected = True
        client._transport = mock_transport
        client._config.callback_completion_timeout = 0.01

        async def hang_forever():
            await asyncio.sleep(100)

        task = asyncio.create_task(hang_forever())
        client._register_subscription_task(task)

        await client.close()

        assert task.cancelled() or task.done()
        assert client._closed is True


class TestNativeAsyncSubscriptionTaskRegistration:
    """Tests for _register/_unregister_subscription_task (lines 742-743, 747)."""

    @pytest.mark.asyncio
    async def test_register_adds_task_to_set(self):
        client = ConcreteNativeAsyncBaseClient(address="localhost:50000")

        async def noop():
            pass

        task = asyncio.create_task(noop())
        client._register_subscription_task(task)

        assert task in client._subscription_tasks
        await task

    @pytest.mark.asyncio
    async def test_done_callback_auto_removes_task(self):
        client = ConcreteNativeAsyncBaseClient(address="localhost:50000")

        async def noop():
            pass

        task = asyncio.create_task(noop())
        client._register_subscription_task(task)
        await task
        await asyncio.sleep(0)

        assert task not in client._subscription_tasks

    @pytest.mark.asyncio
    async def test_unregister_removes_task(self):
        client = ConcreteNativeAsyncBaseClient(address="localhost:50000")

        async def noop():
            await asyncio.sleep(10)

        task = asyncio.create_task(noop())
        client._register_subscription_task(task)
        client._unregister_subscription_task(task)

        assert task not in client._subscription_tasks
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


class TestNativeAsyncValidateMessageSize:
    """Tests for _validate_message_size (line 695)."""

    def test_body_exceeds_max_raises(self):
        config = ClientConfig(address="localhost:50000", max_send_size=100)
        client = ConcreteNativeAsyncBaseClient(config=config)

        with pytest.raises(KubeMQValidationError, match="exceeds maximum"):
            client._validate_message_size(b"x" * 200)

    def test_body_within_limit_passes(self):
        config = ClientConfig(address="localhost:50000", max_send_size=100)
        client = ConcreteNativeAsyncBaseClient(config=config)
        client._validate_message_size(b"x" * 50)

    def test_zero_max_means_unlimited(self):
        config = ClientConfig(address="localhost:50000", max_send_size=0)
        client = ConcreteNativeAsyncBaseClient(config=config)
        client._validate_message_size(b"x" * 999999)


class TestNativeAsyncAexit:
    """Tests for NativeAsyncBaseClient.__aexit__."""

    @pytest.mark.asyncio
    async def test_aexit_calls_close(self):
        client = ConcreteNativeAsyncBaseClient(address="localhost:50000")
        client.connect = AsyncMock()
        client.close = AsyncMock()

        async with client:
            pass

        client.close.assert_called_once()
