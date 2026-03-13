"""Unit tests for kubemq.transport.transport module.

Tests for SyncTransport class and helper functions.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from kubemq.transport.connection import Connection
from kubemq.transport.keep_alive import KeepAliveConfig
from kubemq.transport.server_info import ServerInfo
from kubemq.transport.tls_config import TlsConfig
from kubemq.transport.transport import (
    SyncTransport,
    Transport,
    _get_call_options,
    _get_ssl_credentials,
    _read_file,
)

# ==============================================================================
# Helper Function Tests
# ==============================================================================


class TestReadFile:
    """Tests for _read_file helper function."""

    def test_read_file_returns_bytes(self, tmp_path):
        """Test _read_file returns file contents as bytes."""
        # Create a temp file
        test_file = tmp_path / "test.txt"
        test_file.write_bytes(b"test content")

        result = _read_file(str(test_file))

        assert result == b"test content"

    def test_read_file_raises_on_missing_file(self):
        """Test _read_file raises FileNotFoundError for missing file."""
        with pytest.raises(FileNotFoundError):
            _read_file("/nonexistent/path/file.txt")


class TestGetCallOptions:
    """Tests for _get_call_options helper function."""

    def test_basic_options_without_keepalive(self):
        """Test basic options are set without keep-alive."""
        keep_alive = KeepAliveConfig(enabled=False)
        connection = Connection(address="localhost:50000", keep_alive=keep_alive)

        options = _get_call_options(connection)

        # Check that basic options are present
        options_dict = dict(options)
        assert "grpc.max_send_message_length" in options_dict
        assert "grpc.max_receive_message_length" in options_dict
        # Keep-alive options should NOT be present when explicitly disabled
        assert "grpc.keepalive_time_ms" not in options_dict

    def test_options_with_keepalive_enabled(self):
        """Test keep-alive options are set when enabled."""
        keep_alive = KeepAliveConfig(
            enabled=True,
            ping_interval_in_seconds=30,
            ping_timeout_in_seconds=10,
        )
        connection = Connection(
            address="localhost:50000",
            keep_alive=keep_alive,
        )

        options = _get_call_options(connection)

        options_dict = dict(options)
        assert options_dict.get("grpc.keepalive_time_ms") == 30000
        assert options_dict.get("grpc.keepalive_timeout_ms") == 10000
        assert options_dict.get("grpc.keepalive_permit_without_calls") == 1

    def test_options_with_keepalive_disabled(self):
        """Test keep-alive options are not set when disabled."""
        keep_alive = KeepAliveConfig(
            enabled=False,
            ping_interval_in_seconds=30,
            ping_timeout_in_seconds=10,
        )
        connection = Connection(
            address="localhost:50000",
            keep_alive=keep_alive,
        )

        options = _get_call_options(connection)

        options_dict = dict(options)
        assert "grpc.keepalive_time_ms" not in options_dict


class TestGetSslCredentials:
    """Tests for _get_ssl_credentials helper function."""

    def test_ssl_credentials_with_all_files(self, tmp_path):
        """Test SSL credentials with all certificate files."""
        ca_file = tmp_path / "ca.pem"
        ca_file.write_bytes(b"-----BEGIN CERTIFICATE-----\nca content\n-----END CERTIFICATE-----")
        cert_file = tmp_path / "cert.pem"
        cert_file.write_bytes(
            b"-----BEGIN CERTIFICATE-----\ncert content\n-----END CERTIFICATE-----"
        )
        key_file = tmp_path / "key.pem"
        key_file.write_bytes(b"-----BEGIN PRIVATE KEY-----\nkey content\n-----END PRIVATE KEY-----")

        tls_config = TlsConfig(
            enabled=True,
            ca_file=str(ca_file),
            cert_file=str(cert_file),
            key_file=str(key_file),
        )

        with patch("kubemq.transport.transport.grpc.ssl_channel_credentials") as mock_ssl:
            mock_ssl.return_value = MagicMock()
            _get_ssl_credentials(tls_config)

            mock_ssl.assert_called_once()
            # All certs should be passed
            call_args = mock_ssl.call_args
            assert call_args[0][0] is not None  # root_certificates
            assert call_args[0][1] is not None  # private_key
            assert call_args[0][2] is not None  # certificate_chain


# ==============================================================================
# SyncTransport Initialization Tests
# ==============================================================================


class TestSyncTransportInit:
    """Tests for SyncTransport initialization."""

    def test_init_stores_connection(self):
        """Test __init__ stores connection options."""
        connection = Connection(address="localhost:50000")

        transport = SyncTransport(connection)

        assert transport._opts is not None
        assert transport._opts.address == "localhost:50000"
        assert transport._is_connected is False

    def test_init_creates_logger(self):
        """Test __init__ creates a logger."""
        connection = Connection(address="localhost:50000")

        transport = SyncTransport(connection)

        assert transport._logger is not None


class TestSyncTransportInitialize:
    """Tests for SyncTransport.initialize() method."""

    @patch("kubemq.transport.transport.ChannelManager")
    def test_initialize_creates_channel_manager(self, mock_channel_manager_class):
        """Test initialize() creates a ChannelManager."""
        mock_channel_manager = MagicMock()
        mock_channel_manager.get_client.return_value = MagicMock()
        mock_channel_manager_class.return_value = mock_channel_manager

        connection = Connection(address="localhost:50000")
        transport = SyncTransport(connection)

        with patch.object(transport, "_initialize_async"):
            result = transport.initialize()

        assert result is transport
        assert transport._is_connected is True
        mock_channel_manager_class.assert_called_once()

    @patch("kubemq.transport.transport.ChannelManager")
    def test_initialize_sets_connected_to_false_on_error(self, mock_channel_manager_class):
        """Test initialize() sets _is_connected to False on error."""
        mock_channel_manager_class.side_effect = Exception("Connection failed")

        connection = Connection(address="localhost:50000")
        transport = SyncTransport(connection)

        with pytest.raises(Exception, match="Connection failed"):
            transport.initialize()

        assert transport._is_connected is False


class TestSyncTransportConnection:
    """Tests for SyncTransport connection management."""

    def test_is_connected_returns_channel_manager_state(self):
        """Test is_connected returns state from channel manager."""
        connection = Connection(address="localhost:50000")
        transport = SyncTransport(connection)

        mock_channel_manager = MagicMock()
        mock_channel_manager.connection_state.is_accepting_requests.return_value = True
        transport._channel_manager = mock_channel_manager

        assert transport.is_connected() is True

    def test_is_connected_without_channel_manager(self):
        """Test is_connected returns internal state without channel manager."""
        connection = Connection(address="localhost:50000")
        transport = SyncTransport(connection)
        transport._is_connected = True

        assert transport.is_connected() is True

    def test_close_closes_channel_manager(self):
        """Test close() closes the channel manager."""
        connection = Connection(address="localhost:50000")
        transport = SyncTransport(connection)

        mock_channel_manager = MagicMock()
        transport._channel_manager = mock_channel_manager
        transport._is_connected = True
        transport._async_channel = None  # No async channel

        transport.close()

        mock_channel_manager.close.assert_called_once()
        assert transport._is_connected is False

    @pytest.mark.asyncio
    async def test_close_async_closes_async_channel(self):
        """Test close_async() closes the async channel."""
        from unittest.mock import AsyncMock

        connection = Connection(address="localhost:50000")
        transport = SyncTransport(connection)

        mock_async_channel = MagicMock()
        mock_async_channel.close = AsyncMock()
        transport._async_channel = mock_async_channel
        transport._channel_manager = None

        await transport.close_async()

        mock_async_channel.close.assert_called_once()
        assert transport._async_channel is None


class TestSyncTransportPing:
    """Tests for SyncTransport.ping() method."""

    def test_ping_returns_server_info(self):
        """Test ping() returns ServerInfo from response."""
        connection = Connection(address="localhost:50000")
        transport = SyncTransport(connection)

        mock_response = MagicMock()
        mock_response.Host = "localhost"
        mock_response.Version = "2.3.0"
        mock_response.ServerStartTime = 1704067200
        mock_response.ServerUpTimeSeconds = 3600

        mock_client = MagicMock()
        mock_client.Ping.return_value = mock_response
        transport._client = mock_client

        result = transport.ping()

        assert isinstance(result, ServerInfo)
        assert result.host == "localhost"
        assert result.version == "2.3.0"
        assert result.server_start_time == 1704067200
        assert result.server_up_time_seconds == 3600


class TestSyncTransportClients:
    """Tests for SyncTransport client accessor methods."""

    def test_kubemq_client_returns_from_channel_manager(self):
        """Test kubemq_client() returns client from channel manager."""
        connection = Connection(address="localhost:50000")
        transport = SyncTransport(connection)

        mock_client = MagicMock()
        mock_channel_manager = MagicMock()
        mock_channel_manager.get_client.return_value = mock_client
        transport._channel_manager = mock_channel_manager

        result = transport.kubemq_client()

        assert result == mock_client

    def test_kubemq_client_returns_fallback_client(self):
        """Test kubemq_client() returns _client when no channel manager."""
        connection = Connection(address="localhost:50000")
        transport = SyncTransport(connection)

        mock_client = MagicMock()
        transport._client = mock_client
        transport._channel_manager = None

        result = transport.kubemq_client()

        assert result == mock_client

    def test_kubemq_async_client_returns_async_client(self):
        """Test kubemq_async_client() returns the async client."""
        connection = Connection(address="localhost:50000")
        transport = SyncTransport(connection)

        mock_async_client = MagicMock()
        transport._async_client = mock_async_client

        result = transport.kubemq_async_client()

        assert result == mock_async_client


class TestSyncTransportRecreateChannel:
    """Tests for SyncTransport.recreate_channel() method."""

    def test_recreate_channel_uses_channel_manager(self):
        """Test recreate_channel() uses channel manager."""
        connection = Connection(address="localhost:50000")
        transport = SyncTransport(connection)

        mock_client = MagicMock()
        mock_channel_manager = MagicMock()
        mock_channel_manager.recreate_channel.return_value = mock_client
        transport._channel_manager = mock_channel_manager

        result = transport.recreate_channel()

        assert result == mock_client
        mock_channel_manager.recreate_channel.assert_called_once()

    def test_recreate_channel_raises_without_channel_manager(self):
        """Test recreate_channel() raises when no channel manager."""
        connection = Connection(address="localhost:50000")
        transport = SyncTransport(connection)
        transport._channel_manager = None

        with pytest.raises(ConnectionError, match="Channel manager not initialized"):
            transport.recreate_channel()


class TestTransportAlias:
    """Tests for Transport backward compatibility alias."""

    def test_transport_is_sync_transport(self):
        """Test Transport alias points to SyncTransport."""
        assert Transport is SyncTransport


# ==============================================================================
# Additional Coverage Tests
# ==============================================================================


class TestSyncTransportAsyncClientLazy:
    """Tests for kubemq_async_client() lazy initialization (line 161)."""

    def test_kubemq_async_client_lazy_init(self):
        """Verify kubemq_async_client() calls _initialize_async() when _async_client is None."""
        connection = Connection(address="localhost:50000")
        transport = SyncTransport(connection)

        mock_async_client = MagicMock()

        def side_effect():
            transport._async_client = mock_async_client

        with patch.object(transport, "_initialize_async", side_effect=side_effect) as mock_init:
            result = transport.kubemq_async_client()

        mock_init.assert_called_once()
        assert result is mock_async_client

    def test_kubemq_async_client_returns_existing(self):
        """Set _async_client to a mock, verify returned without calling _initialize_async."""
        connection = Connection(address="localhost:50000")
        transport = SyncTransport(connection)

        mock_async_client = MagicMock()
        transport._async_client = mock_async_client

        with patch.object(transport, "_initialize_async") as mock_init:
            result = transport.kubemq_async_client()

        mock_init.assert_not_called()
        assert result is mock_async_client


class TestSyncTransportPingNotInitialized:
    """Tests for ping() when client is None (line 145)."""

    def test_ping_when_not_initialized(self):
        """_client is None, verify RuntimeError raised."""
        connection = Connection(address="localhost:50000")
        transport = SyncTransport(connection)
        transport._client = None

        with pytest.raises(RuntimeError, match="Transport not initialized"):
            transport.ping()


class TestSyncTransportCloseEdgeCases:
    """Tests for close() method edge cases (lines 207-226)."""

    def test_close_with_channel_manager(self):
        """Set _channel_manager to mock, verify close() calls it."""
        connection = Connection(address="localhost:50000")
        transport = SyncTransport(connection)

        mock_channel_manager = MagicMock()
        transport._channel_manager = mock_channel_manager
        transport._async_channel = None
        transport._is_connected = True

        transport.close()

        mock_channel_manager.close.assert_called_once()
        assert transport._is_connected is False

    def test_close_with_async_channel_in_sync_context(self):
        """Set _async_channel to mock, verify it's cleaned up via new event loop."""
        connection = Connection(address="localhost:50000")
        transport = SyncTransport(connection)

        mock_async_channel = MagicMock()
        mock_async_channel.close = MagicMock(return_value=MagicMock())
        transport._async_channel = mock_async_channel
        transport._async_client = MagicMock()
        transport._channel_manager = None

        with patch("kubemq.transport.transport.asyncio.get_running_loop", side_effect=RuntimeError):
            mock_loop = MagicMock()
            with patch("kubemq.transport.transport.asyncio.new_event_loop", return_value=mock_loop):
                transport.close()

        mock_loop.run_until_complete.assert_called_once_with(mock_async_channel.close())
        mock_loop.close.assert_called_once()
        assert transport._async_channel is None
        assert transport._async_client is None

    def test_close_without_anything(self):
        """No channel_manager, no async_channel, verify no errors."""
        connection = Connection(address="localhost:50000")
        transport = SyncTransport(connection)
        transport._channel_manager = None
        transport._async_channel = None

        transport.close()

        assert transport._is_connected is False


class TestSyncTransportCloseAsyncEdgeCases:
    """Tests for close_async() edge cases (lines 191-203)."""

    @pytest.mark.asyncio
    async def test_close_async_with_channel_manager(self):
        """Verify close_async closes channel_manager and sets _is_connected False."""
        connection = Connection(address="localhost:50000")
        transport = SyncTransport(connection)

        mock_channel_manager = MagicMock()
        transport._channel_manager = mock_channel_manager
        transport._async_channel = None
        transport._is_connected = True

        await transport.close_async()

        mock_channel_manager.close.assert_called_once()
        assert transport._is_connected is False

    @pytest.mark.asyncio
    async def test_close_async_with_both_channel_manager_and_async_channel(self):
        """Verify close_async handles both channel_manager and async_channel."""
        from unittest.mock import AsyncMock

        connection = Connection(address="localhost:50000")
        transport = SyncTransport(connection)

        mock_channel_manager = MagicMock()
        transport._channel_manager = mock_channel_manager
        transport._is_connected = True

        mock_async_channel = MagicMock()
        mock_async_channel.close = AsyncMock()
        transport._async_channel = mock_async_channel
        transport._async_client = MagicMock()

        await transport.close_async()

        mock_channel_manager.close.assert_called_once()
        mock_async_channel.close.assert_called_once()
        assert transport._async_channel is None
        assert transport._async_client is None
        assert transport._is_connected is False


class TestSyncTransportInitializeAsync:
    """Tests for _initialize_async() method (lines 121-141)."""

    @patch("kubemq.transport.transport.kubemq_pb2_grpc.kubemqStub")
    @patch("kubemq.transport.transport.grpc.aio.insecure_channel")
    def test_initialize_async_non_tls(self, mock_insecure_channel, mock_stub):
        """Verify _initialize_async creates insecure async channel when TLS disabled."""
        connection = Connection(address="localhost:50000")
        transport = SyncTransport(connection)

        mock_channel = MagicMock()
        mock_insecure_channel.return_value = mock_channel
        mock_stub.return_value = MagicMock()

        transport._initialize_async()

        mock_insecure_channel.assert_called_once()
        assert transport._async_channel is mock_channel
        assert transport._async_client is not None

    @patch("kubemq.transport.transport.kubemq_pb2_grpc.kubemqStub")
    @patch("kubemq.transport.transport._get_ssl_credentials")
    @patch("kubemq.transport.transport.grpc.aio.secure_channel")
    def test_initialize_async_tls(self, mock_secure_channel, mock_ssl_creds, mock_stub, tmp_path):
        """Verify _initialize_async creates secure async channel when TLS enabled."""
        ca_file = tmp_path / "ca.pem"
        ca_file.write_bytes(b"ca-data")
        cert_file = tmp_path / "cert.pem"
        cert_file.write_bytes(b"cert-data")
        key_file = tmp_path / "key.pem"
        key_file.write_bytes(b"key-data")

        connection = Connection(
            address="localhost:50000",
            tls=TlsConfig(
                enabled=True,
                ca_file=str(ca_file),
                cert_file=str(cert_file),
                key_file=str(key_file),
            ),
        )
        transport = SyncTransport(connection)

        mock_channel = MagicMock()
        mock_secure_channel.return_value = mock_channel
        mock_ssl_creds.return_value = MagicMock()
        mock_stub.return_value = MagicMock()

        transport._initialize_async()

        mock_secure_channel.assert_called_once()
        assert transport._async_channel is mock_channel
        assert transport._async_client is not None
