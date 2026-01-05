"""Unit tests for CQ (Commands/Queries) sync client.

Tests for Client (sync) class from kubemq.cq.client.
"""

from __future__ import annotations

import warnings
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kubemq.common.cancellation_token import CancellationToken
from kubemq.core.config import ClientConfig
from kubemq.cq.client import Client
from kubemq.cq.command_message import CommandMessage
from kubemq.cq.command_response_message import CommandResponseMessage
from kubemq.cq.commands_subscription import CommandsSubscription
from kubemq.cq.queries_subscription import QueriesSubscription
from kubemq.cq.query_message import QueryMessage
from kubemq.cq.query_response_message import QueryResponseMessage
from kubemq.grpc import kubemq_pb2 as pb


@pytest.fixture
def mock_config():
    """Create a mock client config."""
    return ClientConfig(
        address="localhost:50000",
        client_id="test-client",
    )


# ==============================================================================
# Initialization Tests
# ==============================================================================


class TestSyncCQClientInit:
    """Tests for CQ sync client initialization."""

    def test_init_with_address(self):
        """Test initialization with address."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            assert client._config.address == "localhost:50000"

    def test_init_with_config(self, mock_config):
        """Test initialization with config object."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(config=mock_config)

            assert client._config == mock_config

    def test_init_generates_client_id(self):
        """Test that client_id is generated if not provided."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            assert client._config.client_id is not None
            assert len(client._config.client_id) > 0

    def test_init_with_auth_token(self):
        """Test initialization with auth token."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000", auth_token="my-token")

            assert client._config.auth_token == "my-token"

    def test_init_creates_legacy_connection_attribute(self):
        """Test that legacy connection attribute is created."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            assert hasattr(client, "connection")
            assert client.connection is not None


# ==============================================================================
# Command Tests
# ==============================================================================


class TestSyncCQClientCommands:
    """Tests for command sending methods."""

    def test_send_command_request_returns_response(self):
        """Test send_command_request returns CommandResponseMessage."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            # Create mock response
            mock_pb_response = pb.Response()
            mock_pb_response.Executed = True
            mock_pb_response.RequestID = "req-123"

            mock_stub = MagicMock()
            mock_stub.SendRequest.return_value = mock_pb_response
            mock_transport.kubemq_client.return_value = mock_stub

            message = CommandMessage(
                channel="test-commands",
                body=b"do something",
                timeout_in_seconds=30,
            )
            response = client.send_command_request(message)

            assert response is not None
            assert response.is_executed is True

    def test_send_command_request_async_emits_deprecation_warning(self):
        """Test send_command_request_async emits deprecation warning."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            mock_pb_response = pb.Response()
            mock_pb_response.Executed = True

            mock_stub = MagicMock()
            mock_stub.SendRequest.return_value = mock_pb_response
            mock_transport.kubemq_client.return_value = mock_stub

            message = CommandMessage(
                channel="test-commands",
                body=b"do something",
                timeout_in_seconds=30,
            )

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                import asyncio

                asyncio.run(client.send_command_request_async(message))

                deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
                assert len(deprecation_warnings) > 0


# ==============================================================================
# Query Tests
# ==============================================================================


class TestSyncCQClientQueries:
    """Tests for query sending methods."""

    def test_send_query_request_returns_response(self):
        """Test send_query_request returns QueryResponseMessage."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            mock_pb_response = pb.Response()
            mock_pb_response.Executed = True
            mock_pb_response.RequestID = "req-123"
            mock_pb_response.Body = b"query result"

            mock_stub = MagicMock()
            mock_stub.SendRequest.return_value = mock_pb_response
            mock_transport.kubemq_client.return_value = mock_stub

            message = QueryMessage(
                channel="test-queries",
                body=b"get something",
                timeout_in_seconds=30,
            )
            response = client.send_query_request(message)

            assert response is not None
            assert response.is_executed is True

    def test_send_query_request_async_emits_deprecation_warning(self):
        """Test send_query_request_async emits deprecation warning."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            mock_pb_response = pb.Response()
            mock_pb_response.Executed = True

            mock_stub = MagicMock()
            mock_stub.SendRequest.return_value = mock_pb_response
            mock_transport.kubemq_client.return_value = mock_stub

            message = QueryMessage(
                channel="test-queries",
                body=b"get something",
                timeout_in_seconds=30,
            )

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                import asyncio

                asyncio.run(client.send_query_request_async(message))

                deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
                assert len(deprecation_warnings) > 0


# ==============================================================================
# Response Tests
# ==============================================================================


class TestSyncCQClientResponses:
    """Tests for response sending methods."""

    def test_send_response_message_command(self):
        """Test send_response_message for command response."""
        from kubemq.cq.command_message_received import CommandMessageReceived

        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            mock_stub = MagicMock()
            mock_transport.kubemq_client.return_value = mock_stub

            # Create a mock command received to pass to response
            command_received = CommandMessageReceived(
                id="cmd-123",
                from_client_id="sender-client",
                timestamp="2024-01-01T00:00:00Z",
                channel="test-commands",
                reply_channel="reply-channel",
            )

            response = CommandResponseMessage(
                command_received=command_received,
                is_executed=True,
            )
            client.send_response_message(response)

            mock_stub.SendResponse.assert_called_once()

    def test_send_response_message_query(self):
        """Test send_response_message for query response."""
        from kubemq.cq.query_message_received import QueryMessageReceived

        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            mock_stub = MagicMock()
            mock_transport.kubemq_client.return_value = mock_stub

            # Create a mock query received to pass to response
            query_received = QueryMessageReceived(
                id="qry-123",
                from_client_id="sender-client",
                timestamp="2024-01-01T00:00:00Z",
                channel="test-queries",
                reply_channel="reply-channel",
            )

            response = QueryResponseMessage(
                query_received=query_received,
                is_executed=True,
                body=b"result data",
            )
            client.send_response_message(response)

            mock_stub.SendResponse.assert_called_once()

    def test_send_response_message_async_emits_deprecation_warning(self):
        """Test send_response_message_async emits deprecation warning."""
        from kubemq.cq.command_message_received import CommandMessageReceived

        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            mock_stub = MagicMock()
            mock_transport.kubemq_client.return_value = mock_stub

            command_received = CommandMessageReceived(
                id="cmd-123",
                from_client_id="sender-client",
                timestamp="2024-01-01T00:00:00Z",
                channel="test-commands",
                reply_channel="reply-channel",
            )

            response = CommandResponseMessage(
                command_received=command_received,
                is_executed=True,
            )

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                import asyncio

                asyncio.run(client.send_response_message_async(response))

                deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
                assert len(deprecation_warnings) > 0


# ==============================================================================
# Channel Management Tests
# ==============================================================================


class TestSyncCQClientChannelManagement:
    """Tests for channel management methods."""

    def test_create_commands_channel(self):
        """Test create_commands_channel calls request helper."""
        with (
            patch("kubemq.transport.transport.Transport") as mock_transport_class,
            patch("kubemq.cq.client.create_channel_request") as mock_create,
        ):
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            mock_create.return_value = True

            client = Client(address="localhost:50000")
            result = client.create_commands_channel("test-commands")

            assert result is True
            mock_create.assert_called_once()
            call_args = mock_create.call_args[0]
            assert call_args[2] == "test-commands"
            assert call_args[3] == "commands"

    def test_create_queries_channel(self):
        """Test create_queries_channel calls request helper."""
        with (
            patch("kubemq.transport.transport.Transport") as mock_transport_class,
            patch("kubemq.cq.client.create_channel_request") as mock_create,
        ):
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            mock_create.return_value = True

            client = Client(address="localhost:50000")
            result = client.create_queries_channel("test-queries")

            assert result is True
            call_args = mock_create.call_args[0]
            assert call_args[2] == "test-queries"
            assert call_args[3] == "queries"

    def test_delete_commands_channel(self):
        """Test delete_commands_channel calls request helper."""
        with (
            patch("kubemq.transport.transport.Transport") as mock_transport_class,
            patch("kubemq.cq.client.delete_channel_request") as mock_delete,
        ):
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            mock_delete.return_value = True

            client = Client(address="localhost:50000")
            result = client.delete_commands_channel("test-commands")

            assert result is True
            call_args = mock_delete.call_args[0]
            assert call_args[2] == "test-commands"
            assert call_args[3] == "commands"

    def test_delete_queries_channel(self):
        """Test delete_queries_channel calls request helper."""
        with (
            patch("kubemq.transport.transport.Transport") as mock_transport_class,
            patch("kubemq.cq.client.delete_channel_request") as mock_delete,
        ):
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            mock_delete.return_value = True

            client = Client(address="localhost:50000")
            result = client.delete_queries_channel("test-queries")

            assert result is True
            call_args = mock_delete.call_args[0]
            assert call_args[2] == "test-queries"
            assert call_args[3] == "queries"

    def test_list_commands_channels(self):
        """Test list_commands_channels calls request helper."""
        with (
            patch("kubemq.transport.transport.Transport") as mock_transport_class,
            patch("kubemq.cq.client.list_cq_channels") as mock_list,
        ):
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            mock_list.return_value = []

            client = Client(address="localhost:50000")
            result = client.list_commands_channels()

            assert result == []
            call_args = mock_list.call_args[0]
            assert call_args[2] == "commands"

    def test_list_queries_channels(self):
        """Test list_queries_channels calls request helper."""
        with (
            patch("kubemq.transport.transport.Transport") as mock_transport_class,
            patch("kubemq.cq.client.list_cq_channels") as mock_list,
        ):
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            mock_list.return_value = []

            client = Client(address="localhost:50000")
            result = client.list_queries_channels()

            assert result == []
            call_args = mock_list.call_args[0]
            assert call_args[2] == "queries"


# ==============================================================================
# Subscription Tests
# ==============================================================================


class TestSyncCQClientSubscriptions:
    """Tests for subscription methods."""

    def test_subscribe_to_commands_starts_thread(self):
        """Test subscribe_to_commands starts subscription thread."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            subscription = CommandsSubscription(
                channel="test-commands",
                on_receive_command_callback=lambda msg: None,
            )

            with patch("kubemq.cq.client.threading.Thread") as mock_thread:
                mock_thread_instance = MagicMock()
                mock_thread.return_value = mock_thread_instance

                client.subscribe_to_commands(subscription)

                mock_thread.assert_called_once()
                mock_thread_instance.start.assert_called_once()
                # Verify daemon thread
                call_kwargs = mock_thread.call_args[1]
                assert call_kwargs["daemon"] is True

    def test_subscribe_to_commands_with_cancellation_token(self):
        """Test subscribe_to_commands uses provided cancellation token."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            cancel = CancellationToken()
            subscription = CommandsSubscription(
                channel="test-commands",
                on_receive_command_callback=lambda msg: None,
            )

            with patch("kubemq.cq.client.threading.Thread") as mock_thread:
                mock_thread_instance = MagicMock()
                mock_thread.return_value = mock_thread_instance

                client.subscribe_to_commands(subscription, cancel=cancel)

                mock_thread_instance.start.assert_called_once()

    def test_subscribe_to_queries_starts_thread(self):
        """Test subscribe_to_queries starts subscription thread."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            subscription = QueriesSubscription(
                channel="test-queries",
                on_receive_query_callback=lambda msg: None,
            )

            with patch("kubemq.cq.client.threading.Thread") as mock_thread:
                mock_thread_instance = MagicMock()
                mock_thread.return_value = mock_thread_instance

                client.subscribe_to_queries(subscription)

                mock_thread.assert_called_once()
                mock_thread_instance.start.assert_called_once()

    def test_subscribe_creates_cancellation_token_if_none(self):
        """Test _subscribe creates cancellation token if not provided."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            subscription = CommandsSubscription(
                channel="test-commands",
                on_receive_command_callback=lambda msg: None,
            )

            with patch("kubemq.cq.client.threading.Thread") as mock_thread:
                mock_thread_instance = MagicMock()
                mock_thread.return_value = mock_thread_instance

                # Cancel=None should create internal token
                client._subscribe(subscription, cancel=None)

                mock_thread.assert_called_once()


# ==============================================================================
# Context Manager Tests
# ==============================================================================


class TestSyncCQClientContextManager:
    """Tests for context manager support."""

    def test_context_manager_enters_and_exits(self):
        """Test sync context manager properly enters and exits."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            with Client(address="localhost:50000") as client:
                assert client is not None

            mock_transport.close.assert_called()

    @pytest.mark.asyncio
    async def test_async_context_manager_enters_and_exits(self):
        """Test async context manager on sync client."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.close_async = AsyncMock()
            mock_transport_class.return_value = mock_transport

            async with Client(address="localhost:50000") as client:
                assert client is not None


# ==============================================================================
# Async Methods Tests
# ==============================================================================


class TestSyncCQClientAsyncMethods:
    """Tests for async wrapper methods."""

    @pytest.mark.asyncio
    async def test_ping_async_emits_deprecation_warning(self):
        """Test ping_async emits deprecation warning."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.ping.return_value = MagicMock(
                host="localhost",
                version="1.0",
                server_start_time=0,
                server_up_time_seconds=100,
            )
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                await client.ping_async()

                deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
                assert len(deprecation_warnings) > 0

    @pytest.mark.asyncio
    async def test_create_commands_channel_async(self):
        """Test create_commands_channel_async calls sync method."""
        with (
            patch("kubemq.transport.transport.Transport") as mock_transport_class,
            patch("kubemq.cq.client.create_channel_request") as mock_create,
        ):
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            mock_create.return_value = True

            client = Client(address="localhost:50000")
            result = await client.create_commands_channel_async("test-commands")

            assert result is True
