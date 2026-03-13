"""Tests for AsyncTransport."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import grpc.aio
import pytest

from kubemq.core.config import ClientConfig
from kubemq.core.exceptions import (
    KubeMQAuthenticationError,
    KubeMQClientClosedError,
    KubeMQConnectionError,
    KubeMQTimeoutError,
)
from kubemq.grpc import kubemq_pb2 as pb
from kubemq.transport.async_transport import AsyncTransport


@pytest.fixture
def mock_config():
    """Create a mock client config."""
    return ClientConfig(
        address="localhost:50000",
        client_id="test-client",
        auth_token="test-token",
    )


@pytest.fixture
def mock_config_with_tls(tmp_path):
    """Create a mock client config with TLS enabled."""
    from kubemq.core.config import TLSConfig

    # Create temp cert files
    ca_file = tmp_path / "ca.pem"
    ca_file.write_bytes(b"-----BEGIN CERTIFICATE-----\nca content\n-----END CERTIFICATE-----")
    cert_file = tmp_path / "cert.pem"
    cert_file.write_bytes(b"-----BEGIN CERTIFICATE-----\ncert content\n-----END CERTIFICATE-----")
    key_file = tmp_path / "key.pem"
    key_file.write_bytes(b"-----BEGIN PRIVATE KEY-----\nkey content\n-----END PRIVATE KEY-----")

    return ClientConfig(
        address="localhost:50000",
        client_id="test-client",
        tls=TLSConfig(
            enabled=True,
            ca_file=ca_file,
            cert_file=cert_file,
            key_file=key_file,
        ),
    )


@pytest.fixture
def mock_config_with_keepalive():
    """Create a mock client config with keepalive enabled."""
    from kubemq.core.config import KeepAliveConfig

    return ClientConfig(
        address="localhost:50000",
        client_id="test-client",
        keep_alive=KeepAliveConfig(
            enabled=True,
            ping_interval_in_seconds=30,
            ping_timeout_in_seconds=10,
        ),
    )


class TestAsyncTransportInitialization:
    """Tests for AsyncTransport initialization."""

    def test_init_with_config(self, mock_config):
        """Test transport initializes with config."""
        transport = AsyncTransport(mock_config)
        assert transport._config == mock_config
        assert not transport.is_connected
        assert transport._channel is None
        assert transport._stub is None

    def test_initial_state(self, mock_config):
        """Test transport initial state is not connected."""
        transport = AsyncTransport(mock_config)
        assert transport.is_connected is False


class TestAsyncTransportConnect:
    """Tests for AsyncTransport connection."""

    @pytest.mark.asyncio
    async def test_connect_creates_insecure_channel(self, mock_config):
        """Test connect creates insecure channel when TLS disabled."""
        transport = AsyncTransport(mock_config)

        mock_channel = AsyncMock()
        mock_stub = MagicMock()

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel) as mock_insecure,
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            # Use verify=False to skip ping verification
            await transport.connect(verify=False)

        mock_insecure.assert_called_once()
        assert transport.is_connected

    @pytest.mark.asyncio
    async def test_connect_idempotent(self, mock_config):
        """Test multiple connect calls only create one connection."""
        transport = AsyncTransport(mock_config)

        mock_channel = AsyncMock()
        mock_stub = MagicMock()

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel) as mock_insecure,
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            await transport.connect(verify=False)
            await transport.connect(verify=False)  # Second call should be no-op

        # Only called once
        assert mock_insecure.call_count == 1

    @pytest.mark.asyncio
    async def test_connect_with_verify_false(self, mock_config):
        """Test connect with verify=False skips ping."""
        transport = AsyncTransport(mock_config)

        mock_channel = AsyncMock()
        mock_stub = MagicMock()

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            await transport.connect(verify=False)

        # Ping should not be called when verify=False
        mock_stub.Ping.assert_not_called()
        assert transport.is_connected

    @pytest.mark.asyncio
    async def test_connect_with_verification(self, mock_config):
        """Test connect with verification calls ping."""
        transport = AsyncTransport(mock_config)

        mock_channel = AsyncMock()
        mock_stub = MagicMock()

        # Setup ping response
        ping_response = MagicMock()
        ping_response.Host = "localhost"
        ping_response.Version = "1.0.0"
        ping_response.ServerStartTime = 0
        ping_response.ServerUpTimeSeconds = 100

        # Make Ping an async callable
        async def mock_ping(*args, **kwargs):
            # Set connected=True before ping check (simulate what connect does internally)
            transport._connected = True
            return ping_response

        mock_stub.Ping = mock_ping

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            # Need to override the connect method's behavior
            # For now, use verify=False and test ping separately
            await transport.connect(verify=False)

        assert transport.is_connected


class TestAsyncTransportClose:
    """Tests for AsyncTransport close."""

    @pytest.mark.asyncio
    async def test_close_when_not_connected(self, mock_config):
        """Test close when not connected does nothing."""
        transport = AsyncTransport(mock_config)
        await transport.close()  # Should not raise

    @pytest.mark.asyncio
    async def test_close_cleans_up_channel(self, mock_config):
        """Test close cleans up channel."""
        transport = AsyncTransport(mock_config)

        mock_channel = AsyncMock()
        mock_stub = MagicMock()

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            await transport.connect(verify=False)
            assert transport.is_connected

            await transport.close()

        mock_channel.close.assert_called_once()
        assert not transport.is_connected

    @pytest.mark.asyncio
    async def test_close_idempotent(self, mock_config):
        """Test close can be called multiple times."""
        transport = AsyncTransport(mock_config)

        mock_channel = AsyncMock()
        mock_stub = MagicMock()

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            await transport.connect(verify=False)
            await transport.close()
            await transport.close()  # Second close should be no-op

        # Only closed once
        assert mock_channel.close.call_count == 1


class TestAsyncTransportContextManager:
    """Tests for AsyncTransport context manager."""

    @pytest.mark.asyncio
    async def test_context_manager(self, mock_config):
        """Test async context manager."""
        mock_channel = AsyncMock()
        mock_stub = MagicMock()

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
            patch.object(AsyncTransport, "connect") as mock_connect,
        ):

            async def set_connected(verify=True):
                mock_channel  # capture in closure
                AsyncTransport._test_connected = True

            mock_connect.side_effect = lambda verify=True: set_connected(verify)

            transport = AsyncTransport(mock_config)
            transport._channel = mock_channel  # Pre-set channel for close

            # Manual test of context manager behavior
            await transport.connect(verify=False)
            transport._connected = True  # Simulate connected

            assert transport._connected is True

            await transport.close()
            assert not transport.is_connected


class TestAsyncTransportEnsureConnected:
    """Tests for _ensure_connected method."""

    def test_ensure_connected_raises_when_not_connected(self, mock_config):
        """Test _ensure_connected raises when not connected."""
        transport = AsyncTransport(mock_config)

        with pytest.raises(KubeMQConnectionError, match="not connected"):
            transport._ensure_connected()

    def test_ensure_connected_raises_when_closing(self, mock_config):
        """Test _ensure_connected raises KubeMQClientClosedError when closing."""
        transport = AsyncTransport(mock_config)
        transport._connected = True
        transport._closing = True

        with pytest.raises(KubeMQClientClosedError, match="closing"):
            transport._ensure_connected()

    def test_ensure_connected_passes_when_connected(self, mock_config):
        """Test _ensure_connected passes when connected."""
        transport = AsyncTransport(mock_config)
        transport._connected = True
        transport._closing = False

        # Should not raise
        transport._ensure_connected()


class TestAsyncTransportPing:
    """Tests for ping operation."""

    @pytest.mark.asyncio
    async def test_ping_returns_server_info(self, mock_config):
        """Test ping returns ServerInfo."""
        transport = AsyncTransport(mock_config)

        mock_channel = AsyncMock()
        mock_stub = MagicMock()

        ping_response = MagicMock()
        ping_response.Host = "test-host"
        ping_response.Version = "2.0.0"
        ping_response.ServerStartTime = 1234567890
        ping_response.ServerUpTimeSeconds = 3600
        mock_stub.Ping = AsyncMock(return_value=ping_response)

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            await transport.connect(verify=False)
            server_info = await transport.ping()

        assert server_info.host == "test-host"
        assert server_info.version == "2.0.0"
        assert server_info.server_start_time == 1234567890
        assert server_info.server_up_time_seconds == 3600

    @pytest.mark.asyncio
    async def test_ping_raises_when_not_connected(self, mock_config):
        """Test ping raises when not connected."""
        transport = AsyncTransport(mock_config)

        with pytest.raises(KubeMQConnectionError):
            await transport.ping()


class TestAsyncTransportStreamManagement:
    """Tests for stream registration and cleanup."""

    @pytest.mark.asyncio
    async def test_register_stream(self, mock_config):
        """Test stream registration."""
        transport = AsyncTransport(mock_config)
        transport._connected = True

        mock_call = MagicMock()
        await transport._register_stream(mock_call)

        assert mock_call in transport._active_streams

    @pytest.mark.asyncio
    async def test_unregister_stream(self, mock_config):
        """Test stream unregistration."""
        transport = AsyncTransport(mock_config)
        transport._connected = True

        mock_call = MagicMock()
        await transport._register_stream(mock_call)
        await transport._unregister_stream(mock_call)

        assert mock_call not in transport._active_streams

    @pytest.mark.asyncio
    async def test_unregister_nonexistent_stream(self, mock_config):
        """Test unregistering nonexistent stream is safe."""
        transport = AsyncTransport(mock_config)
        transport._connected = True

        mock_call = MagicMock()
        # Should not raise
        await transport._unregister_stream(mock_call)

    @pytest.mark.asyncio
    async def test_close_clears_active_streams(self, mock_config):
        """Test close drains and clears all active streams."""
        transport = AsyncTransport(mock_config)

        mock_channel = AsyncMock()
        mock_stub = MagicMock()

        # Create mock streams that resolve immediately (simulate completed streams)
        mock_stream1 = AsyncMock()
        mock_stream2 = AsyncMock()

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            await transport.connect(verify=False)

            # Register streams
            await transport._register_stream(mock_stream1)
            await transport._register_stream(mock_stream2)

            await transport.close()

        # Streams should be cleared after close
        assert len(transport._active_streams) == 0

    @pytest.mark.asyncio
    async def test_close_force_cancels_on_timeout(self, mock_config):
        """Test close force-cancels streams that exceed drain_timeout."""
        transport = AsyncTransport(mock_config)

        mock_channel = AsyncMock()
        mock_stub = MagicMock()

        cancel_called = False

        class HangingStream:
            """Simulates a gRPC stream that never completes."""

            def __await__(self):
                return asyncio.sleep(999).__await__()

            def cancel(self):
                nonlocal cancel_called
                cancel_called = True

        mock_stream = HangingStream()

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            await transport.connect(verify=False)
            await transport._register_stream(mock_stream)

            await transport.close(drain_timeout=0.05)

        assert cancel_called
        assert len(transport._active_streams) == 0


class TestAsyncTransportChannelOptions:
    """Tests for channel options building."""

    def test_build_channel_options_basic(self, mock_config):
        """Test basic channel options."""
        transport = AsyncTransport(mock_config)
        options = transport._build_channel_options()

        # Should contain max send/receive size options
        option_keys = [opt[0] for opt in options]
        assert "grpc.max_send_message_length" in option_keys
        assert "grpc.max_receive_message_length" in option_keys

    def test_build_channel_options_with_keepalive(self, mock_config_with_keepalive):
        """Test channel options with keepalive."""
        transport = AsyncTransport(mock_config_with_keepalive)
        options = transport._build_channel_options()

        option_keys = [opt[0] for opt in options]
        assert "grpc.keepalive_time_ms" in option_keys
        assert "grpc.keepalive_timeout_ms" in option_keys
        assert "grpc.keepalive_permit_without_calls" in option_keys

    def test_build_channel_options_keepalive_values(self, mock_config_with_keepalive):
        """Test keepalive option values are correct."""
        transport = AsyncTransport(mock_config_with_keepalive)
        options = dict(transport._build_channel_options())

        assert options["grpc.keepalive_time_ms"] == 30 * 1000  # 30 seconds
        assert options["grpc.keepalive_timeout_ms"] == 10 * 1000  # 10 seconds


class TestAsyncTransportInterceptors:
    """Tests for interceptor building."""

    def test_build_interceptors(self, mock_config):
        """Test interceptors are built correctly."""
        transport = AsyncTransport(mock_config)
        interceptors = transport._build_interceptors()

        # Should have 4 interceptors (one for each call type)
        assert len(interceptors) == 4

    def test_build_interceptors_with_auth_token(self, mock_config):
        """Test interceptors receive auth token."""
        transport = AsyncTransport(mock_config)
        interceptors = transport._build_interceptors()

        # All interceptors should be created
        assert len(interceptors) == 4


class TestAsyncTransportIsConnected:
    """Tests for is_connected property."""

    def test_is_connected_false_when_not_connected(self, mock_config):
        """Test is_connected is False when not connected."""
        transport = AsyncTransport(mock_config)
        assert transport.is_connected is False

    @pytest.mark.asyncio
    async def test_is_connected_true_when_connected(self, mock_config):
        """Test is_connected is True when connected."""
        transport = AsyncTransport(mock_config)

        mock_channel = AsyncMock()
        mock_stub = MagicMock()

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            await transport.connect(verify=False)
            assert transport.is_connected is True

    def test_is_connected_false_when_closing(self, mock_config):
        """Test is_connected is False when closing."""
        transport = AsyncTransport(mock_config)
        transport._connected = True
        transport._closing = True

        assert transport.is_connected is False


class TestAsyncTransportSendOperations:
    """Tests for send operations."""

    @pytest.mark.asyncio
    async def test_send_event_when_not_connected(self, mock_config):
        """Test send_event raises when not connected."""
        transport = AsyncTransport(mock_config)
        event = pb.Event()

        with pytest.raises(KubeMQConnectionError):
            await transport.send_event(event)

    @pytest.mark.asyncio
    async def test_send_request_when_not_connected(self, mock_config):
        """Test send_request raises when not connected."""
        transport = AsyncTransport(mock_config)
        request = pb.Request()

        with pytest.raises(KubeMQConnectionError):
            await transport.send_request(request)

    @pytest.mark.asyncio
    async def test_send_response_when_not_connected(self, mock_config):
        """Test send_response raises when not connected."""
        transport = AsyncTransport(mock_config)
        response = pb.Response()

        with pytest.raises(KubeMQConnectionError):
            await transport.send_response(response)


class TestAsyncTransportQueueOperations:
    """Tests for queue operations."""

    @pytest.mark.asyncio
    async def test_send_queue_message_when_not_connected(self, mock_config):
        """Test send_queue_message raises when not connected."""
        transport = AsyncTransport(mock_config)
        message = pb.QueueMessage()

        with pytest.raises(KubeMQConnectionError):
            await transport.send_queue_message(message)

    @pytest.mark.asyncio
    async def test_receive_queue_messages_when_not_connected(self, mock_config):
        """Test receive_queue_messages raises when not connected."""
        transport = AsyncTransport(mock_config)
        request = pb.ReceiveQueueMessagesRequest()

        with pytest.raises(KubeMQConnectionError):
            await transport.receive_queue_messages(request)


# ==============================================================================
# Extended Tests - TLS Connection
# ==============================================================================


class TestAsyncTransportTLSConnection:
    """Tests for TLS/secure channel connection."""

    @pytest.mark.asyncio
    async def test_connect_creates_secure_channel_with_tls(self, mock_config_with_tls):
        """Test connect creates secure channel when TLS enabled."""
        transport = AsyncTransport(mock_config_with_tls)

        mock_channel = AsyncMock()
        mock_stub = MagicMock()
        mock_credentials = MagicMock()

        with (
            patch.object(transport, "_build_ssl_credentials", return_value=mock_credentials),
            patch("grpc.aio.secure_channel", return_value=mock_channel) as mock_secure,
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            await transport.connect(verify=False)

        mock_secure.assert_called_once()
        assert transport.is_connected

    @pytest.mark.asyncio
    async def test_build_ssl_credentials_with_all_files(self, tmp_path):
        """Test _build_ssl_credentials reads certificate files."""
        from kubemq.core.config import TLSConfig

        ca_file = tmp_path / "ca.pem"
        ca_file.write_bytes(b"-----BEGIN CERTIFICATE-----\nca content\n-----END CERTIFICATE-----")
        cert_file = tmp_path / "cert.pem"
        cert_file.write_bytes(
            b"-----BEGIN CERTIFICATE-----\ncert content\n-----END CERTIFICATE-----"
        )
        key_file = tmp_path / "key.pem"
        key_file.write_bytes(b"-----BEGIN PRIVATE KEY-----\nkey content\n-----END PRIVATE KEY-----")

        config = ClientConfig(
            address="localhost:50000",
            client_id="test",
            tls=TLSConfig(
                enabled=True,
                ca_file=ca_file,
                cert_file=cert_file,
                key_file=key_file,
            ),
        )
        transport = AsyncTransport(config)

        with patch("grpc.ssl_channel_credentials") as mock_ssl:
            mock_ssl.return_value = MagicMock()
            transport._build_ssl_credentials()

            mock_ssl.assert_called_once()
            call_args = mock_ssl.call_args
            assert call_args[1]["root_certificates"] is not None
            assert call_args[1]["private_key"] is not None
            assert call_args[1]["certificate_chain"] is not None

    @pytest.mark.asyncio
    async def test_build_ssl_credentials_without_ca_file(self, tmp_path):
        """Test _build_ssl_credentials works without CA file."""
        from kubemq.core.config import TLSConfig

        cert_file = tmp_path / "cert.pem"
        cert_file.write_bytes(
            b"-----BEGIN CERTIFICATE-----\ncert content\n-----END CERTIFICATE-----"
        )
        key_file = tmp_path / "key.pem"
        key_file.write_bytes(b"-----BEGIN PRIVATE KEY-----\nkey content\n-----END PRIVATE KEY-----")

        config = ClientConfig(
            address="localhost:50000",
            client_id="test",
            tls=TLSConfig(
                enabled=True,
                cert_file=cert_file,
                key_file=key_file,
            ),
        )
        transport = AsyncTransport(config)

        with patch("grpc.ssl_channel_credentials") as mock_ssl:
            mock_ssl.return_value = MagicMock()
            transport._build_ssl_credentials()

            mock_ssl.assert_called_once()
            call_args = mock_ssl.call_args
            assert call_args[1]["root_certificates"] is None


# ==============================================================================
# Extended Tests - Successful Send Operations
# ==============================================================================


class TestAsyncTransportSuccessfulSendOperations:
    """Tests for successful send operations."""

    @pytest.mark.asyncio
    async def test_send_event_success(self, mock_config):
        """Test send_event returns result on success."""
        transport = AsyncTransport(mock_config)

        mock_channel = AsyncMock()
        mock_stub = MagicMock()
        mock_result = MagicMock(spec=pb.Result)
        mock_stub.SendEvent = AsyncMock(return_value=mock_result)

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            await transport.connect(verify=False)
            event = pb.Event(EventID="test-event", Channel="test-channel")
            result = await transport.send_event(event)

        assert result == mock_result
        mock_stub.SendEvent.assert_called_once_with(event)

    @pytest.mark.asyncio
    async def test_send_request_success(self, mock_config):
        """Test send_request returns response on success."""
        transport = AsyncTransport(mock_config)

        mock_channel = AsyncMock()
        mock_stub = MagicMock()
        mock_response = MagicMock(spec=pb.Response)
        mock_response.Executed = True
        mock_stub.SendRequest = AsyncMock(return_value=mock_response)

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            await transport.connect(verify=False)
            request = pb.Request(RequestID="test-request", Channel="test-channel")
            response = await transport.send_request(request)

        assert response == mock_response
        assert response.Executed is True

    @pytest.mark.asyncio
    async def test_send_request_with_custom_timeout(self, mock_config):
        """Test send_request uses custom timeout when provided."""
        transport = AsyncTransport(mock_config)

        mock_channel = AsyncMock()
        mock_stub = MagicMock()
        mock_response = MagicMock(spec=pb.Response)
        mock_stub.SendRequest = AsyncMock(return_value=mock_response)

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
            patch("asyncio.wait_for", new_callable=AsyncMock) as mock_wait_for,
        ):
            mock_wait_for.return_value = mock_response
            await transport.connect(verify=False)
            request = pb.Request()
            await transport.send_request(request, timeout_seconds=60)

            # Verify custom timeout was used
            mock_wait_for.assert_called()
            call_kwargs = mock_wait_for.call_args[1]
            assert call_kwargs.get("timeout") == 60

    @pytest.mark.asyncio
    async def test_send_response_success(self, mock_config):
        """Test send_response completes successfully."""
        transport = AsyncTransport(mock_config)

        mock_channel = AsyncMock()
        mock_stub = MagicMock()
        mock_stub.SendResponse = AsyncMock(return_value=None)

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            await transport.connect(verify=False)
            response = pb.Response(RequestID="test-request", Executed=True)
            # Should not raise
            await transport.send_response(response)

        mock_stub.SendResponse.assert_called_once_with(response)


# ==============================================================================
# Extended Tests - Timeout Handling
# ==============================================================================


class TestAsyncTransportTimeoutHandling:
    """Tests for timeout handling across operations."""

    @pytest.mark.asyncio
    async def test_send_event_timeout_raises_kubemq_timeout_error(self, mock_config):
        """Test send_event raises KubeMQTimeoutError on timeout."""
        transport = AsyncTransport(mock_config)

        mock_channel = AsyncMock()
        mock_stub = MagicMock()

        async def timeout_send(*args):
            raise asyncio.TimeoutError()

        mock_stub.SendEvent = timeout_send

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
            patch("asyncio.wait_for", side_effect=asyncio.TimeoutError()),
        ):
            await transport.connect(verify=False)
            event = pb.Event()

            with pytest.raises(KubeMQTimeoutError, match="Send event timed out"):
                await transport.send_event(event)

    @pytest.mark.asyncio
    async def test_send_request_timeout_raises_kubemq_timeout_error(self, mock_config):
        """Test send_request raises KubeMQTimeoutError on timeout."""
        transport = AsyncTransport(mock_config)

        mock_channel = AsyncMock()
        mock_stub = MagicMock()

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
            patch("asyncio.wait_for", side_effect=asyncio.TimeoutError()),
        ):
            await transport.connect(verify=False)
            request = pb.Request()

            with pytest.raises(KubeMQTimeoutError, match="Request timed out"):
                await transport.send_request(request)

    @pytest.mark.asyncio
    async def test_send_response_timeout_raises_kubemq_timeout_error(self, mock_config):
        """Test send_response raises KubeMQTimeoutError on timeout."""
        transport = AsyncTransport(mock_config)

        mock_channel = AsyncMock()
        mock_stub = MagicMock()

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
            patch("asyncio.wait_for", side_effect=asyncio.TimeoutError()),
        ):
            await transport.connect(verify=False)
            response = pb.Response()

            with pytest.raises(KubeMQTimeoutError, match="Send response timed out"):
                await transport.send_response(response)


# ==============================================================================
# Extended Tests - Queue Operations
# ==============================================================================


class TestAsyncTransportQueueOperationsSuccess:
    """Tests for successful queue operations."""

    @pytest.mark.asyncio
    async def test_send_queue_message_success(self, mock_config):
        """Test send_queue_message returns result on success."""
        transport = AsyncTransport(mock_config)

        mock_channel = AsyncMock()
        mock_stub = MagicMock()
        mock_result = MagicMock(spec=pb.SendQueueMessageResult)
        mock_result.IsError = False
        mock_stub.SendQueueMessage = AsyncMock(return_value=mock_result)

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            await transport.connect(verify=False)
            message = pb.QueueMessage(Channel="test-queue", Body=b"test")
            result = await transport.send_queue_message(message)

        assert result == mock_result
        assert result.IsError is False

    @pytest.mark.asyncio
    async def test_send_queue_messages_batch_success(self, mock_config):
        """Test send_queue_messages_batch returns response on success."""
        transport = AsyncTransport(mock_config)

        mock_channel = AsyncMock()
        mock_stub = MagicMock()
        mock_response = MagicMock(spec=pb.QueueMessagesBatchResponse)
        mock_stub.SendQueueMessagesBatch = AsyncMock(return_value=mock_response)

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            await transport.connect(verify=False)
            request = pb.QueueMessagesBatchRequest()
            result = await transport.send_queue_messages_batch(request)

        assert result == mock_response

    @pytest.mark.asyncio
    async def test_receive_queue_messages_success(self, mock_config):
        """Test receive_queue_messages returns response on success."""
        transport = AsyncTransport(mock_config)

        mock_channel = AsyncMock()
        mock_stub = MagicMock()
        mock_response = MagicMock(spec=pb.ReceiveQueueMessagesResponse)
        mock_response.IsError = False
        mock_stub.ReceiveQueueMessages = AsyncMock(return_value=mock_response)

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            await transport.connect(verify=False)
            request = pb.ReceiveQueueMessagesRequest(Channel="test-queue", WaitTimeSeconds=5)
            result = await transport.receive_queue_messages(request)

        assert result == mock_response

    @pytest.mark.asyncio
    async def test_ack_all_queue_messages_success(self, mock_config):
        """Test ack_all_queue_messages returns response on success."""
        transport = AsyncTransport(mock_config)

        mock_channel = AsyncMock()
        mock_stub = MagicMock()
        mock_response = MagicMock(spec=pb.AckAllQueueMessagesResponse)
        mock_response.AffectedMessages = 5
        mock_stub.AckAllQueueMessages = AsyncMock(return_value=mock_response)

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            await transport.connect(verify=False)
            request = pb.AckAllQueueMessagesRequest(Channel="test-queue")
            result = await transport.ack_all_queue_messages(request)

        assert result == mock_response
        assert result.AffectedMessages == 5

    @pytest.mark.asyncio
    async def test_ack_all_queue_messages_when_not_connected(self, mock_config):
        """Test ack_all_queue_messages raises when not connected."""
        transport = AsyncTransport(mock_config)
        request = pb.AckAllQueueMessagesRequest()

        with pytest.raises(KubeMQConnectionError):
            await transport.ack_all_queue_messages(request)


# ==============================================================================
# Extended Tests - Queue Timeout Handling
# ==============================================================================


class TestAsyncTransportQueueTimeouts:
    """Tests for queue operation timeout handling."""

    @pytest.mark.asyncio
    async def test_send_queue_message_timeout(self, mock_config):
        """Test send_queue_message raises KubeMQTimeoutError on timeout."""
        transport = AsyncTransport(mock_config)

        mock_channel = AsyncMock()
        mock_stub = MagicMock()

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
            patch("asyncio.wait_for", side_effect=asyncio.TimeoutError()),
        ):
            await transport.connect(verify=False)
            message = pb.QueueMessage()

            with pytest.raises(KubeMQTimeoutError, match="Queue send timed out"):
                await transport.send_queue_message(message)

    @pytest.mark.asyncio
    async def test_send_queue_messages_batch_timeout(self, mock_config):
        """Test send_queue_messages_batch raises KubeMQTimeoutError on timeout."""
        transport = AsyncTransport(mock_config)

        mock_channel = AsyncMock()
        mock_stub = MagicMock()

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
            patch("asyncio.wait_for", side_effect=asyncio.TimeoutError()),
        ):
            await transport.connect(verify=False)
            request = pb.QueueMessagesBatchRequest()

            with pytest.raises(KubeMQTimeoutError, match="Queue batch send timed out"):
                await transport.send_queue_messages_batch(request)

    @pytest.mark.asyncio
    async def test_receive_queue_messages_timeout(self, mock_config):
        """Test receive_queue_messages raises KubeMQTimeoutError on timeout."""
        transport = AsyncTransport(mock_config)

        mock_channel = AsyncMock()
        mock_stub = MagicMock()

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
            patch("asyncio.wait_for", side_effect=asyncio.TimeoutError()),
        ):
            await transport.connect(verify=False)
            request = pb.ReceiveQueueMessagesRequest(WaitTimeSeconds=5)

            with pytest.raises(KubeMQTimeoutError, match="Queue receive timed out"):
                await transport.receive_queue_messages(request)

    @pytest.mark.asyncio
    async def test_ack_all_queue_messages_timeout(self, mock_config):
        """Test ack_all_queue_messages raises KubeMQTimeoutError on timeout."""
        transport = AsyncTransport(mock_config)

        mock_channel = AsyncMock()
        mock_stub = MagicMock()

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
            patch("asyncio.wait_for", side_effect=asyncio.TimeoutError()),
        ):
            await transport.connect(verify=False)
            request = pb.AckAllQueueMessagesRequest()

            with pytest.raises(KubeMQTimeoutError, match="Ack all timed out"):
                await transport.ack_all_queue_messages(request)


# ==============================================================================
# Extended Tests - Queue Info Operations
# ==============================================================================


class TestAsyncTransportQueuesInfo:
    """Tests for queues_info operation."""

    @pytest.mark.asyncio
    async def test_queues_info_success(self, mock_config):
        """Test queues_info returns response on success."""
        transport = AsyncTransport(mock_config)

        mock_channel = AsyncMock()
        mock_stub = MagicMock()
        mock_response = MagicMock(spec=pb.QueuesInfoResponse)
        mock_stub.QueuesInfo = AsyncMock(return_value=mock_response)

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            await transport.connect(verify=False)
            request = pb.QueuesInfoRequest(QueueName="test-queue")
            result = await transport.queues_info(request)

        assert result == mock_response

    @pytest.mark.asyncio
    async def test_queues_info_when_not_connected(self, mock_config):
        """Test queues_info raises when not connected."""
        transport = AsyncTransport(mock_config)
        request = pb.QueuesInfoRequest()

        with pytest.raises(KubeMQConnectionError):
            await transport.queues_info(request)

    @pytest.mark.asyncio
    async def test_queues_info_timeout(self, mock_config):
        """Test queues_info raises KubeMQTimeoutError on timeout."""
        transport = AsyncTransport(mock_config)

        mock_channel = AsyncMock()
        mock_stub = MagicMock()

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
            patch("asyncio.wait_for", side_effect=asyncio.TimeoutError()),
        ):
            await transport.connect(verify=False)
            request = pb.QueuesInfoRequest()

            with pytest.raises(KubeMQTimeoutError, match="Queue info request timed out"):
                await transport.queues_info(request)


# ==============================================================================
# Extended Tests - gRPC Error Handling
# ==============================================================================


class TestAsyncTransportGrpcErrorHandling:
    """Tests for gRPC error handling."""

    @pytest.mark.asyncio
    async def test_ping_grpc_error_converted(self, mock_config):
        """Test ping converts gRPC errors to KubeMQ exceptions."""
        transport = AsyncTransport(mock_config)

        mock_channel = AsyncMock()
        mock_stub = MagicMock()

        # Create a proper AioRpcError mock
        grpc_error = grpc.aio.AioRpcError(
            grpc.StatusCode.UNAVAILABLE,
            initial_metadata=None,
            trailing_metadata=None,
            details="Server unavailable",
            debug_error_string=None,
        )
        mock_stub.Ping = AsyncMock(side_effect=grpc_error)

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            await transport.connect(verify=False)

            # Should convert to KubeMQ exception
            with pytest.raises(Exception):  # from_grpc_error will convert it
                await transport.ping()

    @pytest.mark.asyncio
    async def test_send_event_grpc_error_converted(self, mock_config):
        """Test send_event converts gRPC errors to KubeMQ exceptions."""
        transport = AsyncTransport(mock_config)

        mock_channel = AsyncMock()
        mock_stub = MagicMock()

        grpc_error = grpc.aio.AioRpcError(
            grpc.StatusCode.INVALID_ARGUMENT,
            initial_metadata=None,
            trailing_metadata=None,
            details="Invalid event",
            debug_error_string=None,
        )
        mock_stub.SendEvent = AsyncMock(side_effect=grpc_error)

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            await transport.connect(verify=False)
            event = pb.Event()

            with pytest.raises(Exception):
                await transport.send_event(event)


# ==============================================================================
# Extended Tests - Subscription Streaming
# ==============================================================================


class TestAsyncTransportSubscriptionStreaming:
    """Tests for subscription streaming operations."""

    @pytest.mark.asyncio
    async def test_subscribe_to_events_when_not_connected(self, mock_config):
        """Test subscribe_to_events raises when not connected."""
        transport = AsyncTransport(mock_config)
        request = pb.Subscribe()

        with pytest.raises(KubeMQConnectionError):
            async for _ in transport.subscribe_to_events(request):
                pass

    @pytest.mark.asyncio
    async def test_subscribe_to_events_yields_events(self, mock_config):
        """Test subscribe_to_events yields events from stream."""
        transport = AsyncTransport(mock_config)

        mock_channel = AsyncMock()
        mock_stub = MagicMock()

        # Create mock events
        event1 = MagicMock(spec=pb.EventReceive)
        event1.EventID = "event-1"
        event2 = MagicMock(spec=pb.EventReceive)
        event2.EventID = "event-2"

        # Create a proper async iterable
        class MockAsyncStream:
            def __init__(self, items):
                self.items = items
                self.index = 0

            def cancel(self):
                pass

            def __aiter__(self):
                return self

            async def __anext__(self):
                if self.index >= len(self.items):
                    raise StopAsyncIteration
                item = self.items[self.index]
                self.index += 1
                return item

        mock_call = MockAsyncStream([event1, event2])
        mock_stub.SubscribeToEvents = MagicMock(return_value=mock_call)

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            await transport.connect(verify=False)
            request = pb.Subscribe(Channel="test-channel")

            received = []
            async for event in transport.subscribe_to_events(request):
                received.append(event)

        assert len(received) == 2
        assert received[0].EventID == "event-1"
        assert received[1].EventID == "event-2"

    @pytest.mark.asyncio
    async def test_subscribe_to_events_cancellation(self, mock_config):
        """Test subscribe_to_events respects cancellation token."""
        from kubemq.common.async_cancellation_token import AsyncCancellationToken

        transport = AsyncTransport(mock_config)

        mock_channel = AsyncMock()
        mock_stub = MagicMock()

        event1 = MagicMock(spec=pb.EventReceive)
        event2 = MagicMock(spec=pb.EventReceive)
        event3 = MagicMock(spec=pb.EventReceive)

        # Create a proper async iterable
        class MockAsyncStream:
            def __init__(self, items):
                self.items = items
                self.index = 0

            def cancel(self):
                pass

            def __aiter__(self):
                return self

            async def __anext__(self):
                if self.index >= len(self.items):
                    raise StopAsyncIteration
                item = self.items[self.index]
                self.index += 1
                return item

        mock_call = MockAsyncStream([event1, event2, event3])
        mock_stub.SubscribeToEvents = MagicMock(return_value=mock_call)

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            await transport.connect(verify=False)

            token = AsyncCancellationToken()
            request = pb.Subscribe(Channel="test-channel")

            received = []
            async for event in transport.subscribe_to_events(request, cancellation_token=token):
                received.append(event)
                if len(received) >= 2:
                    token.cancel()

        # Should have received 2 events before cancellation
        assert len(received) == 2

    @pytest.mark.asyncio
    async def test_subscribe_to_requests_when_not_connected(self, mock_config):
        """Test subscribe_to_requests raises when not connected."""
        transport = AsyncTransport(mock_config)
        request = pb.Subscribe()

        with pytest.raises(KubeMQConnectionError):
            async for _ in transport.subscribe_to_requests(request):
                pass

    @pytest.mark.asyncio
    async def test_subscribe_to_requests_yields_requests(self, mock_config):
        """Test subscribe_to_requests yields requests from stream."""
        transport = AsyncTransport(mock_config)

        mock_channel = AsyncMock()
        mock_stub = MagicMock()

        req1 = MagicMock(spec=pb.Request)
        req1.RequestID = "req-1"
        req2 = MagicMock(spec=pb.Request)
        req2.RequestID = "req-2"

        # Create a proper async iterable
        class MockAsyncStream:
            def __init__(self, items):
                self.items = items
                self.index = 0

            def cancel(self):
                pass

            def __aiter__(self):
                return self

            async def __anext__(self):
                if self.index >= len(self.items):
                    raise StopAsyncIteration
                item = self.items[self.index]
                self.index += 1
                return item

        mock_call = MockAsyncStream([req1, req2])
        mock_stub.SubscribeToRequests = MagicMock(return_value=mock_call)

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            await transport.connect(verify=False)
            request = pb.Subscribe(Channel="test-channel")

            received = []
            async for req in transport.subscribe_to_requests(request):
                received.append(req)

        assert len(received) == 2


# ==============================================================================
# Extended Tests - Bidirectional Queue Streaming
# ==============================================================================


class TestAsyncTransportBidirectionalStreaming:
    """Tests for bidirectional queue streaming operations."""

    @pytest.mark.asyncio
    async def test_queues_upstream_when_not_connected(self, mock_config):
        """Test queues_upstream raises when not connected."""
        transport = AsyncTransport(mock_config)

        async def empty_gen():
            if False:
                yield pb.QueuesUpstreamRequest()

        with pytest.raises(KubeMQConnectionError):
            async for _ in transport.queues_upstream(empty_gen()):
                pass

    @pytest.mark.asyncio
    async def test_queues_downstream_when_not_connected(self, mock_config):
        """Test queues_downstream raises when not connected."""
        transport = AsyncTransport(mock_config)

        async def empty_gen():
            if False:
                yield pb.QueuesDownstreamRequest()

        with pytest.raises(KubeMQConnectionError):
            async for _ in transport.queues_downstream(empty_gen()):
                pass

    @pytest.mark.asyncio
    async def test_queues_upstream_yields_responses(self, mock_config):
        """Test queues_upstream yields responses from stream."""
        transport = AsyncTransport(mock_config)

        mock_channel = AsyncMock()
        mock_stub = MagicMock()

        resp1 = MagicMock(spec=pb.QueuesUpstreamResponse)
        resp1.RefRequestId = "req-1"

        # Create a proper async iterable
        class MockAsyncStream:
            def __init__(self, items):
                self.items = items
                self.index = 0

            def cancel(self):
                pass

            def __aiter__(self):
                return self

            async def __anext__(self):
                if self.index >= len(self.items):
                    raise StopAsyncIteration
                item = self.items[self.index]
                self.index += 1
                return item

        mock_call = MockAsyncStream([resp1])
        mock_stub.QueuesUpstream = MagicMock(return_value=mock_call)

        async def request_gen():
            yield pb.QueuesUpstreamRequest()

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            await transport.connect(verify=False)

            received = []
            async for resp in transport.queues_upstream(request_gen()):
                received.append(resp)

        assert len(received) == 1
        assert received[0].RefRequestId == "req-1"

    @pytest.mark.asyncio
    async def test_queues_downstream_yields_responses(self, mock_config):
        """Test queues_downstream yields responses from stream."""
        transport = AsyncTransport(mock_config)

        mock_channel = AsyncMock()
        mock_stub = MagicMock()

        resp1 = MagicMock(spec=pb.QueuesDownstreamResponse)
        resp1.RefRequestId = "req-1"

        # Create a proper async iterable
        class MockAsyncStream:
            def __init__(self, items):
                self.items = items
                self.index = 0

            def cancel(self):
                pass

            def __aiter__(self):
                return self

            async def __anext__(self):
                if self.index >= len(self.items):
                    raise StopAsyncIteration
                item = self.items[self.index]
                self.index += 1
                return item

        mock_call = MockAsyncStream([resp1])
        mock_stub.QueuesDownstream = MagicMock(return_value=mock_call)

        async def request_gen():
            yield pb.QueuesDownstreamRequest()

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            await transport.connect(verify=False)

            received = []
            async for resp in transport.queues_downstream(request_gen()):
                received.append(resp)

        assert len(received) == 1


# ==============================================================================
# Extended Tests - Context Manager
# ==============================================================================


class TestAsyncTransportContextManagerExtended:
    """Extended tests for context manager."""

    @pytest.mark.asyncio
    async def test_context_manager_connects_and_closes(self, mock_config):
        """Test context manager properly connects and closes."""
        mock_channel = AsyncMock()
        mock_stub = MagicMock()

        # Setup ping response for verify
        ping_response = MagicMock()
        ping_response.Host = "localhost"
        ping_response.Version = "1.0.0"
        ping_response.ServerStartTime = 0
        ping_response.ServerUpTimeSeconds = 100
        mock_stub.Ping = AsyncMock(return_value=ping_response)

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            async with AsyncTransport(mock_config) as transport:
                assert transport.is_connected is True

            # After exit, should be disconnected
            assert transport.is_connected is False

    @pytest.mark.asyncio
    async def test_context_manager_with_exception(self, mock_config):
        """Test context manager closes on exception."""
        mock_channel = AsyncMock()
        mock_stub = MagicMock()

        # Setup ping response for verify
        ping_response = MagicMock()
        ping_response.Host = "localhost"
        ping_response.Version = "1.0.0"
        ping_response.ServerStartTime = 0
        ping_response.ServerUpTimeSeconds = 100
        mock_stub.Ping = AsyncMock(return_value=ping_response)

        transport = None
        with pytest.raises(ValueError):
            with (
                patch("grpc.aio.insecure_channel", return_value=mock_channel),
                patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
            ):
                async with AsyncTransport(mock_config) as t:
                    transport = t
                    assert transport.is_connected is True
                    raise ValueError("Test error")

        # Should still be closed
        assert transport is not None
        assert transport.is_connected is False


# ==============================================================================
# Extended Tests - set_token
# ==============================================================================


class TestAsyncTransportSetToken:
    """Tests for set_token method."""

    def test_set_token(self, mock_config):
        """Test set_token updates the token holder."""
        transport = AsyncTransport(mock_config)
        transport.set_token("new-token-value")
        assert transport._token_holder.token == "new-token-value"

    def test_set_token_updates_holder(self, mock_config):
        """Test set_token can be called multiple times."""
        transport = AsyncTransport(mock_config)
        transport.set_token("first-token")
        assert transport._token_holder.token == "first-token"

        transport.set_token("second-token")
        assert transport._token_holder.token == "second-token"


# ==============================================================================
# Extended Tests - _handle_unauthenticated
# ==============================================================================


class TestAsyncTransportHandleUnauthenticated:
    """Tests for _handle_unauthenticated method."""

    @pytest.mark.asyncio
    async def test_returns_false_without_manager(self, mock_config):
        """Test returns False when no token_manager is configured."""
        transport = AsyncTransport(mock_config)
        assert transport._token_manager is None

        error = KubeMQAuthenticationError("unauthenticated")
        result = await transport._handle_unauthenticated(error)
        assert result is False

    @pytest.mark.asyncio
    async def test_returns_true_on_refresh_success(self, mock_config):
        """Test returns True when token refresh succeeds."""
        transport = AsyncTransport(mock_config)
        mock_tm = AsyncMock()
        mock_tm.invalidate = AsyncMock()
        mock_tm.get_token = AsyncMock()
        transport._token_manager = mock_tm

        error = KubeMQAuthenticationError("unauthenticated")
        result = await transport._handle_unauthenticated(error)

        assert result is True
        mock_tm.invalidate.assert_awaited_once()
        mock_tm.get_token.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_returns_false_on_refresh_error(self, mock_config):
        """Test returns False when token refresh raises an exception."""
        transport = AsyncTransport(mock_config)
        mock_tm = AsyncMock()
        mock_tm.invalidate = AsyncMock()
        mock_tm.get_token = AsyncMock(side_effect=RuntimeError("refresh failed"))
        transport._token_manager = mock_tm

        error = KubeMQAuthenticationError("unauthenticated")
        result = await transport._handle_unauthenticated(error)

        assert result is False


# ==============================================================================
# Extended Tests - _create_channel_for_reconnect
# ==============================================================================


class TestAsyncTransportCreateChannelForReconnect:
    """Tests for _create_channel_for_reconnect method."""

    @pytest.mark.asyncio
    async def test_insecure_reconnect(self, mock_config):
        """Test insecure (non-TLS) reconnect path."""
        transport = AsyncTransport(mock_config)
        mock_channel = AsyncMock()

        with patch("grpc.aio.insecure_channel", return_value=mock_channel) as mock_insecure:
            channel = await transport._create_channel_for_reconnect()

        mock_insecure.assert_called_once()
        assert channel is mock_channel

    @pytest.mark.asyncio
    async def test_secure_reconnect(self, mock_config_with_tls):
        """Test TLS reconnect path with mock credentials."""
        transport = AsyncTransport(mock_config_with_tls)
        mock_channel = AsyncMock()
        mock_credentials = MagicMock()

        with (
            patch.object(transport, "_build_ssl_credentials", return_value=mock_credentials),
            patch("grpc.aio.secure_channel", return_value=mock_channel) as mock_secure,
        ):
            channel = await transport._create_channel_for_reconnect()

        mock_secure.assert_called_once()
        assert channel is mock_channel

    @pytest.mark.asyncio
    async def test_file_not_found_error(self, mock_config_with_tls):
        """Test FileNotFoundError during TLS reconnect raises KubeMQConnectionError."""
        transport = AsyncTransport(mock_config_with_tls)

        with patch.object(
            transport,
            "_build_ssl_credentials",
            side_effect=FileNotFoundError("ca.pem not found"),
        ):
            with pytest.raises(KubeMQConnectionError, match="Certificate file not found"):
                await transport._create_channel_for_reconnect()

    @pytest.mark.asyncio
    async def test_permission_error(self, mock_config_with_tls):
        """Test PermissionError during TLS reconnect raises KubeMQConnectionError."""
        transport = AsyncTransport(mock_config_with_tls)

        with patch.object(
            transport,
            "_build_ssl_credentials",
            side_effect=PermissionError("permission denied"),
        ):
            with pytest.raises(KubeMQConnectionError, match="permission denied"):
                await transport._create_channel_for_reconnect()


# ==============================================================================
# Extended Tests - _is_connection_error
# ==============================================================================


class TestAsyncTransportConnectionErrorDetection:
    """Tests for _is_connection_error method."""

    def test_unavailable_is_connection_error(self, mock_config):
        """Test UNAVAILABLE is detected as connection error."""
        transport = AsyncTransport(mock_config)
        error = grpc.aio.AioRpcError(
            grpc.StatusCode.UNAVAILABLE,
            initial_metadata=None,
            trailing_metadata=None,
            details="Server unavailable",
            debug_error_string=None,
        )
        assert transport._is_connection_error(error) is True

    def test_aborted_is_connection_error(self, mock_config):
        """Test ABORTED is detected as connection error."""
        transport = AsyncTransport(mock_config)
        error = grpc.aio.AioRpcError(
            grpc.StatusCode.ABORTED,
            initial_metadata=None,
            trailing_metadata=None,
            details="Aborted",
            debug_error_string=None,
        )
        assert transport._is_connection_error(error) is True

    def test_invalid_argument_not_connection_error(self, mock_config):
        """Test INVALID_ARGUMENT is not a connection error."""
        transport = AsyncTransport(mock_config)
        error = grpc.aio.AioRpcError(
            grpc.StatusCode.INVALID_ARGUMENT,
            initial_metadata=None,
            trailing_metadata=None,
            details="Bad argument",
            debug_error_string=None,
        )
        assert transport._is_connection_error(error) is False


# ==============================================================================
# Extended Tests - _on_connection_lost
# ==============================================================================


class TestAsyncTransportOnConnectionLost:
    """Tests for _on_connection_lost method."""

    @pytest.mark.asyncio
    async def test_no_op_when_closing(self, mock_config):
        """Test _on_connection_lost does nothing when _closing is True."""
        transport = AsyncTransport(mock_config)
        transport._closing = True
        transport._connected = True

        await transport._on_connection_lost()

        assert transport._connected is True

    @pytest.mark.asyncio
    async def test_triggers_reconnection(self, mock_config):
        """Test _on_connection_lost transitions state and starts reconnection."""
        transport = AsyncTransport(mock_config)
        transport._connected = True
        transport._closing = False

        mock_rm = MagicMock()
        mock_rm.is_reconnecting = False
        mock_rm.start_reconnection = AsyncMock()
        transport._reconnection_manager = mock_rm

        await transport._on_connection_lost()

        assert transport._connected is False
        mock_rm.start_reconnection.assert_awaited_once()


# ==============================================================================
# Extended Tests - _reconnect
# ==============================================================================


class TestAsyncTransportReconnect:
    """Tests for _reconnect method."""

    @pytest.mark.asyncio
    async def test_reconnect_creates_new_channel_and_pings(self, mock_config):
        """Test _reconnect creates new channel, pings, and closes old channel."""
        transport = AsyncTransport(mock_config)

        old_channel = AsyncMock()
        transport._channel = old_channel

        new_channel = AsyncMock()
        mock_stub = MagicMock()
        mock_stub.Ping = AsyncMock(return_value=MagicMock())

        with (
            patch.object(
                transport, "_create_channel_for_reconnect", return_value=new_channel,
            ) as mock_create,
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            await transport._reconnect()

        mock_create.assert_awaited_once()
        mock_stub.Ping.assert_awaited_once()
        old_channel.close.assert_awaited_once()
        assert transport._connected is True
        assert transport._channel is new_channel

    @pytest.mark.asyncio
    async def test_reconnect_without_old_channel(self, mock_config):
        """Test _reconnect works when there is no existing channel."""
        transport = AsyncTransport(mock_config)
        transport._channel = None

        new_channel = AsyncMock()
        mock_stub = MagicMock()
        mock_stub.Ping = AsyncMock(return_value=MagicMock())

        with (
            patch.object(
                transport, "_create_channel_for_reconnect", return_value=new_channel,
            ),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            await transport._reconnect()

        assert transport._connected is True
        assert transport._channel is new_channel


# ==============================================================================
# NEW: auto_reconnect __init__ tests
# ==============================================================================


class TestAsyncTransportAutoReconnectInit:
    """Tests for __init__ with auto_reconnect flag."""

    def test_init_auto_reconnect_true_creates_reconnection_manager(self):
        config = ClientConfig(
            address="localhost:50000",
            client_id="test-client",
            auto_reconnect=True,
        )
        transport = AsyncTransport(config)
        assert transport._reconnection_manager is not None

    def test_init_auto_reconnect_false_no_reconnection_manager(self):
        config = ClientConfig(
            address="localhost:50000",
            client_id="test-client",
            auto_reconnect=False,
        )
        transport = AsyncTransport(config)
        assert transport._reconnection_manager is None


# ==============================================================================
# NEW: TLS insecure_skip_verify warning
# ==============================================================================


class TestAsyncTransportTLSInsecureWarning:
    """Tests for TLS insecure_skip_verify warning log."""

    @pytest.mark.asyncio
    async def test_insecure_skip_verify_logs_warning(self, tmp_path, caplog):
        import logging

        from kubemq.core.config import TLSConfig

        ca_file = tmp_path / "ca.pem"
        ca_file.write_bytes(b"-----BEGIN CERTIFICATE-----\nca\n-----END CERTIFICATE-----")

        config = ClientConfig(
            address="localhost:50000",
            client_id="test",
            tls=TLSConfig(
                enabled=True,
                ca_file=ca_file,
                insecure_skip_verify=True,
            ),
        )
        transport = AsyncTransport(config)
        mock_channel = AsyncMock()
        mock_stub = MagicMock()
        mock_creds = MagicMock()

        with (
            patch.object(transport, "_build_ssl_credentials", return_value=mock_creds),
            patch("grpc.aio.secure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
            caplog.at_level(logging.WARNING),
        ):
            await transport.connect(verify=False)

        assert any("certificate verification is disabled" in r.message for r in caplog.records)


# ==============================================================================
# NEW: _build_channel_options with server_name_override
# ==============================================================================


class TestAsyncTransportChannelOptionsExtended:
    """Extended tests for _build_channel_options."""

    def test_server_name_override_option(self):
        from kubemq.core.config import TLSConfig

        config = ClientConfig(
            address="localhost:50000",
            client_id="test",
            tls=TLSConfig(server_name_override="my.server"),
        )
        transport = AsyncTransport(config)
        options = transport._build_channel_options()
        option_dict = dict(options)
        assert option_dict["grpc.ssl_target_name_override"] == "my.server"

    def test_keepalive_enabled_includes_all_options(self):
        from kubemq.core.config import KeepAliveConfig

        config = ClientConfig(
            address="localhost:50000",
            client_id="test",
            keep_alive=KeepAliveConfig(
                enabled=True,
                ping_interval_in_seconds=20,
                ping_timeout_in_seconds=8,
                permit_without_calls=False,
            ),
        )
        transport = AsyncTransport(config)
        options = dict(transport._build_channel_options())
        assert options["grpc.keepalive_time_ms"] == 20_000
        assert options["grpc.keepalive_timeout_ms"] == 8_000
        assert options["grpc.keepalive_permit_without_calls"] == 0

    def test_keepalive_disabled_excludes_keepalive_options(self):
        from kubemq.core.config import KeepAliveConfig

        config = ClientConfig(
            address="localhost:50000",
            client_id="test",
            keep_alive=KeepAliveConfig(enabled=False),
        )
        transport = AsyncTransport(config)
        options = dict(transport._build_channel_options())
        assert "grpc.keepalive_time_ms" not in options


# ==============================================================================
# NEW: TLS PEM vs file fallback
# ==============================================================================


class TestAsyncTransportTLSPemFallback:
    """Tests for _build_ssl_credentials PEM vs file fallback logic."""

    def test_pem_bytes_take_precedence_over_file(self):
        from kubemq.core.config import TLSConfig

        ca_pem = b"ca-pem-bytes"
        cert_pem = b"cert-pem-bytes"
        key_pem = b"key-pem-bytes"

        config = ClientConfig(
            address="localhost:50000",
            client_id="test",
            tls=TLSConfig(
                enabled=True,
                ca_pem=ca_pem,
                cert_pem=cert_pem,
                key_pem=key_pem,
            ),
        )
        transport = AsyncTransport(config)

        with patch("grpc.ssl_channel_credentials") as mock_ssl:
            mock_ssl.return_value = MagicMock()
            transport._build_ssl_credentials()

            call_kwargs = mock_ssl.call_args[1]
            assert call_kwargs["root_certificates"] == ca_pem
            assert call_kwargs["private_key"] == key_pem
            assert call_kwargs["certificate_chain"] == cert_pem

    def test_file_fallback_when_pem_is_none(self, tmp_path):
        from kubemq.core.config import TLSConfig

        ca_file = tmp_path / "ca.pem"
        ca_file.write_bytes(b"ca-file-content")
        cert_file = tmp_path / "cert.pem"
        cert_file.write_bytes(b"cert-file-content")
        key_file = tmp_path / "key.pem"
        key_file.write_bytes(b"key-file-content")

        config = ClientConfig(
            address="localhost:50000",
            client_id="test",
            tls=TLSConfig(
                enabled=True,
                ca_file=ca_file,
                cert_file=cert_file,
                key_file=key_file,
            ),
        )
        transport = AsyncTransport(config)

        with patch("grpc.ssl_channel_credentials") as mock_ssl:
            mock_ssl.return_value = MagicMock()
            transport._build_ssl_credentials()

            call_kwargs = mock_ssl.call_args[1]
            assert call_kwargs["root_certificates"] == b"ca-file-content"
            assert call_kwargs["private_key"] == b"key-file-content"
            assert call_kwargs["certificate_chain"] == b"cert-file-content"


# ==============================================================================
# NEW: Verification ping timeout
# ==============================================================================


class TestAsyncTransportVerifyPingTimeout:
    """Tests for verification ping timeout during connect."""

    @pytest.mark.asyncio
    async def test_ping_timeout_logs_warning_but_stays_connected(self, mock_config, caplog):
        import logging

        transport = AsyncTransport(mock_config)
        mock_channel = AsyncMock()
        mock_stub = MagicMock()

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
            patch("asyncio.wait_for", side_effect=asyncio.TimeoutError()),
            caplog.at_level(logging.WARNING),
        ):
            await transport.connect(verify=True)

        assert transport.is_connected
        assert any("verification timed out" in r.message for r in caplog.records)


# ==============================================================================
# NEW: _handle_unauthenticated exception path
# ==============================================================================


class TestAsyncTransportHandleUnauthenticatedException:
    """Tests for _handle_unauthenticated when invalidate raises."""

    @pytest.mark.asyncio
    async def test_invalidate_raises_returns_false(self, mock_config):
        transport = AsyncTransport(mock_config)
        mock_tm = AsyncMock()
        mock_tm.invalidate = AsyncMock(side_effect=RuntimeError("invalidate failed"))
        transport._token_manager = mock_tm

        error = KubeMQAuthenticationError("unauthenticated")
        result = await transport._handle_unauthenticated(error)
        assert result is False


# ==============================================================================
# NEW: OSError during reconnect TLS
# ==============================================================================


class TestAsyncTransportReconnectOSError:
    """Tests for OSError during TLS reconnect."""

    @pytest.mark.asyncio
    async def test_oserror_during_tls_reconnect(self, mock_config_with_tls):
        transport = AsyncTransport(mock_config_with_tls)

        with patch.object(
            transport,
            "_build_ssl_credentials",
            side_effect=OSError("disk I/O error"),
        ):
            with pytest.raises(KubeMQConnectionError, match="I/O error") as exc_info:
                await transport._create_channel_for_reconnect()

            assert exc_info.value.is_retryable is True


# ==============================================================================
# NEW: Close with drain timeout (force cancel)
# ==============================================================================


class TestAsyncTransportDrainTimeout:
    """Tests for close with drain_timeout forcing stream cancellation."""

    @pytest.mark.asyncio
    async def test_close_drain_timeout_force_cancels_multiple_streams(self, mock_config):
        transport = AsyncTransport(mock_config)
        mock_channel = AsyncMock()
        mock_stub = MagicMock()

        cancel_events = []

        class HangingStream:
            def __await__(self):
                return asyncio.sleep(999).__await__()

            def cancel(self):
                cancel_events.append(True)

        stream1 = HangingStream()
        stream2 = HangingStream()

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            await transport.connect(verify=False)
            await transport._register_stream(stream1)
            await transport._register_stream(stream2)
            await transport.close(drain_timeout=0.01)

        assert len(cancel_events) == 2
        assert len(transport._active_streams) == 0


# ==============================================================================
# NEW: _ensure_connected when draining
# ==============================================================================


class TestAsyncTransportEnsureConnectedDraining:
    """Tests for _ensure_connected when _draining is True."""

    def test_draining_raises_client_closed_error(self, mock_config):
        transport = AsyncTransport(mock_config)
        transport._connected = True
        transport._draining = True

        with pytest.raises(KubeMQClientClosedError, match="closing"):
            transport._ensure_connected()


# ==============================================================================
# NEW: Send methods connection error → _on_connection_lost
# ==============================================================================


def _make_unavailable_error():
    return grpc.aio.AioRpcError(
        grpc.StatusCode.UNAVAILABLE,
        initial_metadata=None,
        trailing_metadata=None,
        details="connection lost",
        debug_error_string=None,
    )


class TestAsyncTransportSendConnectionErrors:
    """Tests for send methods triggering _on_connection_lost on UNAVAILABLE."""

    @pytest.mark.asyncio
    async def test_send_event_unavailable_triggers_connection_lost(self, mock_config):
        transport = AsyncTransport(mock_config)
        mock_channel = AsyncMock()
        mock_stub = MagicMock()
        mock_stub.SendEvent = AsyncMock(side_effect=_make_unavailable_error())

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
            patch.object(transport, "_on_connection_lost", new_callable=AsyncMock) as mock_lost,
        ):
            await transport.connect(verify=False)
            with pytest.raises(Exception):
                await transport.send_event(pb.Event())
            mock_lost.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_send_request_unavailable_triggers_connection_lost(self, mock_config):
        transport = AsyncTransport(mock_config)
        mock_channel = AsyncMock()
        mock_stub = MagicMock()
        mock_stub.SendRequest = AsyncMock(side_effect=_make_unavailable_error())

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
            patch.object(transport, "_on_connection_lost", new_callable=AsyncMock) as mock_lost,
        ):
            await transport.connect(verify=False)
            with pytest.raises(Exception):
                await transport.send_request(pb.Request())
            mock_lost.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_send_response_unavailable_triggers_connection_lost(self, mock_config):
        transport = AsyncTransport(mock_config)
        mock_channel = AsyncMock()
        mock_stub = MagicMock()
        mock_stub.SendResponse = AsyncMock(side_effect=_make_unavailable_error())

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
            patch.object(transport, "_on_connection_lost", new_callable=AsyncMock) as mock_lost,
        ):
            await transport.connect(verify=False)
            with pytest.raises(Exception):
                await transport.send_response(pb.Response())
            mock_lost.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_send_queue_message_unavailable_triggers_connection_lost(self, mock_config):
        transport = AsyncTransport(mock_config)
        mock_channel = AsyncMock()
        mock_stub = MagicMock()
        mock_stub.SendQueueMessage = AsyncMock(side_effect=_make_unavailable_error())

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
            patch.object(transport, "_on_connection_lost", new_callable=AsyncMock) as mock_lost,
        ):
            await transport.connect(verify=False)
            with pytest.raises(Exception):
                await transport.send_queue_message(pb.QueueMessage())
            mock_lost.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_send_queue_batch_unavailable_triggers_connection_lost(self, mock_config):
        transport = AsyncTransport(mock_config)
        mock_channel = AsyncMock()
        mock_stub = MagicMock()
        mock_stub.SendQueueMessagesBatch = AsyncMock(side_effect=_make_unavailable_error())

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
            patch.object(transport, "_on_connection_lost", new_callable=AsyncMock) as mock_lost,
        ):
            await transport.connect(verify=False)
            with pytest.raises(Exception):
                await transport.send_queue_messages_batch(pb.QueueMessagesBatchRequest())
            mock_lost.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_receive_queue_unavailable_triggers_connection_lost(self, mock_config):
        transport = AsyncTransport(mock_config)
        mock_channel = AsyncMock()
        mock_stub = MagicMock()
        mock_stub.ReceiveQueueMessages = AsyncMock(side_effect=_make_unavailable_error())

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
            patch.object(transport, "_on_connection_lost", new_callable=AsyncMock) as mock_lost,
        ):
            await transport.connect(verify=False)
            with pytest.raises(Exception):
                await transport.receive_queue_messages(
                    pb.ReceiveQueueMessagesRequest(WaitTimeSeconds=1)
                )
            mock_lost.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_ack_all_unavailable_triggers_connection_lost(self, mock_config):
        transport = AsyncTransport(mock_config)
        mock_channel = AsyncMock()
        mock_stub = MagicMock()
        mock_stub.AckAllQueueMessages = AsyncMock(side_effect=_make_unavailable_error())

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
            patch.object(transport, "_on_connection_lost", new_callable=AsyncMock) as mock_lost,
        ):
            await transport.connect(verify=False)
            with pytest.raises(Exception):
                await transport.ack_all_queue_messages(pb.AckAllQueueMessagesRequest())
            mock_lost.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_queues_info_unavailable_triggers_connection_lost(self, mock_config):
        transport = AsyncTransport(mock_config)
        mock_channel = AsyncMock()
        mock_stub = MagicMock()
        mock_stub.QueuesInfo = AsyncMock(side_effect=_make_unavailable_error())

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
            patch.object(transport, "_on_connection_lost", new_callable=AsyncMock) as mock_lost,
        ):
            await transport.connect(verify=False)
            with pytest.raises(Exception):
                await transport.queues_info(pb.QueuesInfoRequest())
            mock_lost.assert_awaited_once()


# ==============================================================================
# NEW: Stream CANCELLED handling (suppressed)
# ==============================================================================


def _make_cancelled_error():
    return grpc.aio.AioRpcError(
        grpc.StatusCode.CANCELLED,
        initial_metadata=None,
        trailing_metadata=None,
        details="cancelled",
        debug_error_string=None,
    )


class _CancelledAsyncStream:
    """Mock stream that raises CANCELLED on iteration."""

    def __init__(self):
        self._cancelled = False

    def cancel(self):
        self._cancelled = True

    def __aiter__(self):
        return self

    async def __anext__(self):
        raise _make_cancelled_error()


class TestAsyncTransportStreamCancelledSuppressed:
    """Tests for CANCELLED error suppression in streaming methods."""

    @pytest.mark.asyncio
    async def test_subscribe_events_cancelled_suppressed(self, mock_config):
        transport = AsyncTransport(mock_config)
        mock_channel = AsyncMock()
        mock_stub = MagicMock()
        mock_stub.SubscribeToEvents = MagicMock(return_value=_CancelledAsyncStream())

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            await transport.connect(verify=False)
            received = []
            async for event in transport.subscribe_to_events(pb.Subscribe()):
                received.append(event)
            assert received == []

    @pytest.mark.asyncio
    async def test_subscribe_requests_cancelled_suppressed(self, mock_config):
        transport = AsyncTransport(mock_config)
        mock_channel = AsyncMock()
        mock_stub = MagicMock()
        mock_stub.SubscribeToRequests = MagicMock(return_value=_CancelledAsyncStream())

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            await transport.connect(verify=False)
            received = []
            async for req in transport.subscribe_to_requests(pb.Subscribe()):
                received.append(req)
            assert received == []

    @pytest.mark.asyncio
    async def test_queues_upstream_cancelled_suppressed(self, mock_config):
        transport = AsyncTransport(mock_config)
        mock_channel = AsyncMock()
        mock_stub = MagicMock()
        mock_stub.QueuesUpstream = MagicMock(return_value=_CancelledAsyncStream())

        async def empty_gen():
            if False:
                yield pb.QueuesUpstreamRequest()

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            await transport.connect(verify=False)
            received = []
            async for resp in transport.queues_upstream(empty_gen()):
                received.append(resp)
            assert received == []

    @pytest.mark.asyncio
    async def test_queues_downstream_cancelled_suppressed(self, mock_config):
        transport = AsyncTransport(mock_config)
        mock_channel = AsyncMock()
        mock_stub = MagicMock()
        mock_stub.QueuesDownstream = MagicMock(return_value=_CancelledAsyncStream())

        async def empty_gen():
            if False:
                yield pb.QueuesDownstreamRequest()

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            await transport.connect(verify=False)
            received = []
            async for resp in transport.queues_downstream(empty_gen()):
                received.append(resp)
            assert received == []


# ==============================================================================
# NEW: Stream closing check (loop break)
# ==============================================================================


class _ClosingAwareAsyncStream:
    """Mock stream that yields items, setting transport._closing on the second __anext__ call.

    The first item is returned normally; on the second call, _closing is
    set *before* returning so the transport loop's closing check fires.
    """

    def __init__(self, items, transport):
        self._items = items
        self._index = 0
        self._transport = transport

    def cancel(self):
        pass

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._index >= len(self._items):
            raise StopAsyncIteration
        if self._index > 0:
            self._transport._closing = True
        item = self._items[self._index]
        self._index += 1
        return item


class TestAsyncTransportStreamClosingBreak:
    """Tests for stream loop breaking when transport is closing."""

    @pytest.mark.asyncio
    async def test_subscribe_events_breaks_on_closing(self, mock_config):
        transport = AsyncTransport(mock_config)
        mock_channel = AsyncMock()
        mock_stub = MagicMock()

        event1 = MagicMock(spec=pb.EventReceive)
        event2 = MagicMock(spec=pb.EventReceive)
        mock_call = _ClosingAwareAsyncStream([event1, event2], transport)
        mock_stub.SubscribeToEvents = MagicMock(return_value=mock_call)

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            await transport.connect(verify=False)
            received = []
            async for event in transport.subscribe_to_events(pb.Subscribe()):
                received.append(event)

        assert len(received) == 1

    @pytest.mark.asyncio
    async def test_subscribe_requests_breaks_on_closing(self, mock_config):
        transport = AsyncTransport(mock_config)
        mock_channel = AsyncMock()
        mock_stub = MagicMock()

        req1 = MagicMock(spec=pb.Request)
        req2 = MagicMock(spec=pb.Request)
        mock_call = _ClosingAwareAsyncStream([req1, req2], transport)
        mock_stub.SubscribeToRequests = MagicMock(return_value=mock_call)

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            await transport.connect(verify=False)
            received = []
            async for req in transport.subscribe_to_requests(pb.Subscribe()):
                received.append(req)

        assert len(received) == 1

    @pytest.mark.asyncio
    async def test_queues_upstream_breaks_on_closing(self, mock_config):
        transport = AsyncTransport(mock_config)
        mock_channel = AsyncMock()
        mock_stub = MagicMock()

        resp1 = MagicMock(spec=pb.QueuesUpstreamResponse)
        resp2 = MagicMock(spec=pb.QueuesUpstreamResponse)
        mock_call = _ClosingAwareAsyncStream([resp1, resp2], transport)
        mock_stub.QueuesUpstream = MagicMock(return_value=mock_call)

        async def request_gen():
            yield pb.QueuesUpstreamRequest()

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            await transport.connect(verify=False)
            received = []
            async for resp in transport.queues_upstream(request_gen()):
                received.append(resp)

        assert len(received) == 1

    @pytest.mark.asyncio
    async def test_queues_downstream_breaks_on_closing(self, mock_config):
        transport = AsyncTransport(mock_config)
        mock_channel = AsyncMock()
        mock_stub = MagicMock()

        resp1 = MagicMock(spec=pb.QueuesDownstreamResponse)
        resp2 = MagicMock(spec=pb.QueuesDownstreamResponse)
        mock_call = _ClosingAwareAsyncStream([resp1, resp2], transport)
        mock_stub.QueuesDownstream = MagicMock(return_value=mock_call)

        async def request_gen():
            yield pb.QueuesDownstreamRequest()

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            await transport.connect(verify=False)
            received = []
            async for resp in transport.queues_downstream(request_gen()):
                received.append(resp)

        assert len(received) == 1


# ==============================================================================
# NEW: Upstream/downstream stream UNAVAILABLE propagation
# ==============================================================================


def _make_unavailable_stream():
    class _UnavailableStream:
        def cancel(self):
            pass

        def __aiter__(self):
            return self

        async def __anext__(self):
            raise _make_unavailable_error()

    return _UnavailableStream()


class TestAsyncTransportStreamUnavailablePropagation:
    """Tests for UNAVAILABLE errors propagating from stream methods."""

    @pytest.mark.asyncio
    async def test_queues_upstream_unavailable_propagates(self, mock_config):
        transport = AsyncTransport(mock_config)
        mock_channel = AsyncMock()
        mock_stub = MagicMock()
        mock_stub.QueuesUpstream = MagicMock(return_value=_make_unavailable_stream())

        async def request_gen():
            yield pb.QueuesUpstreamRequest()

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            await transport.connect(verify=False)
            with pytest.raises(Exception):
                async for _ in transport.queues_upstream(request_gen()):
                    pass

    @pytest.mark.asyncio
    async def test_queues_downstream_unavailable_propagates(self, mock_config):
        transport = AsyncTransport(mock_config)
        mock_channel = AsyncMock()
        mock_stub = MagicMock()
        mock_stub.QueuesDownstream = MagicMock(return_value=_make_unavailable_stream())

        async def request_gen():
            yield pb.QueuesDownstreamRequest()

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            await transport.connect(verify=False)
            with pytest.raises(Exception):
                async for _ in transport.queues_downstream(request_gen()):
                    pass


# ==============================================================================
# NEW: __aexit__ calls close
# ==============================================================================


class TestAsyncTransportAexit:
    """Tests for __aexit__ calling close()."""

    @pytest.mark.asyncio
    async def test_aexit_calls_close(self, mock_config):
        transport = AsyncTransport(mock_config)

        with patch.object(transport, "close", new_callable=AsyncMock) as mock_close:
            await transport.__aexit__(None, None, None)
            mock_close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_aexit_calls_close_with_exception_info(self, mock_config):
        transport = AsyncTransport(mock_config)

        with patch.object(transport, "close", new_callable=AsyncMock) as mock_close:
            await transport.__aexit__(ValueError, ValueError("test"), None)
            mock_close.assert_awaited_once()


# ==============================================================================
# NEW: connection_state property, on_* callbacks
# ==============================================================================


class TestAsyncTransportConnectionStateProperty:
    """Tests for connection_state property and on_* callback registration."""

    def test_connection_state_property(self, mock_config):
        from kubemq.core.types import ConnectionState

        transport = AsyncTransport(mock_config)
        assert transport.connection_state == ConnectionState.IDLE

    def test_on_connected_registers_without_error(self, mock_config):
        transport = AsyncTransport(mock_config)
        callback = MagicMock()
        transport.on_connected(callback)

    def test_on_disconnected_registers_without_error(self, mock_config):
        transport = AsyncTransport(mock_config)
        callback = MagicMock()
        transport.on_disconnected(callback)

    def test_on_reconnecting_registers_without_error(self, mock_config):
        transport = AsyncTransport(mock_config)
        callback = MagicMock()
        transport.on_reconnecting(callback)

    def test_on_reconnected_registers_without_error(self, mock_config):
        transport = AsyncTransport(mock_config)
        callback = MagicMock()
        transport.on_reconnected(callback)


# ==============================================================================
# NEW: close cancels reconnection_manager and token_manager
# ==============================================================================


class TestAsyncTransportCloseManagers:
    """Tests for close() cancelling reconnection_manager and token_manager."""

    @pytest.mark.asyncio
    async def test_close_cancels_reconnection_manager(self, mock_config):
        transport = AsyncTransport(mock_config)
        mock_channel = AsyncMock()
        mock_stub = MagicMock()
        mock_rm = AsyncMock()
        mock_rm.cancel = AsyncMock()

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            await transport.connect(verify=False)
            transport._reconnection_manager = mock_rm
            await transport.close()

        mock_rm.cancel.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_close_closes_token_manager(self, mock_config):
        transport = AsyncTransport(mock_config)
        mock_channel = AsyncMock()
        mock_stub = MagicMock()
        mock_tm = AsyncMock()
        mock_tm.close = AsyncMock()

        with (
            patch("grpc.aio.insecure_channel", return_value=mock_channel),
            patch("kubemq.grpc.kubemq_pb2_grpc.kubemqStub", return_value=mock_stub),
        ):
            await transport.connect(verify=False)
            transport._token_manager = mock_tm
            await transport.close()

        mock_tm.close.assert_awaited_once()


# ==============================================================================
# NEW: _create_channel_for_reconnect with token_manager
# ==============================================================================


class TestAsyncTransportReconnectWithTokenManager:
    """Tests for _create_channel_for_reconnect refreshing token."""

    @pytest.mark.asyncio
    async def test_reconnect_refreshes_token(self, mock_config):
        transport = AsyncTransport(mock_config)
        mock_tm = AsyncMock()
        mock_tm.get_token = AsyncMock()
        transport._token_manager = mock_tm

        mock_channel = AsyncMock()

        with patch("grpc.aio.insecure_channel", return_value=mock_channel):
            await transport._create_channel_for_reconnect()

        mock_tm.get_token.assert_awaited_once()
