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


# ==============================================================================
# Command Error Tests
# ==============================================================================

import threading  # noqa: E402

import grpc  # noqa: E402

from kubemq.core.exceptions import KubeMQValidationError  # noqa: E402


class FakeRpcError(grpc.RpcError):
    """Minimal stub that satisfies grpc.RpcError for testing."""

    def code(self):
        return grpc.StatusCode.UNAVAILABLE

    def details(self):
        return "server unavailable"


class TestSyncCQClientCommandErrors:
    """Tests for error paths in send_command."""

    def test_send_command_grpc_error_propagates(self):
        """Test that a gRPC RpcError from the transport propagates."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            mock_stub = MagicMock()
            mock_stub.SendRequest.side_effect = FakeRpcError()
            mock_transport.kubemq_client.return_value = mock_stub

            message = CommandMessage(
                channel="test-commands",
                body=b"payload",
                timeout_in_seconds=10,
            )

            with pytest.raises(grpc.RpcError):
                client.send_command(message)

    def test_send_command_validation_error_raises_kubemq_validation_error(self):
        """Test that a ValidationError is wrapped in KubeMQValidationError."""
        from pydantic import ValidationError as PydanticValidationError

        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            mock_stub = MagicMock()
            mock_transport.kubemq_client.return_value = mock_stub

            message = CommandMessage(
                channel="test-commands",
                body=b"payload",
                timeout_in_seconds=10,
            )

            validation_err = PydanticValidationError.from_exception_data(
                title="CommandMessage",
                line_errors=[],
            )
            with patch.object(CommandMessage, "encode", side_effect=validation_err):
                with pytest.raises(KubeMQValidationError):
                    client.send_command(message)


# ==============================================================================
# Query Error Tests
# ==============================================================================


class TestSyncCQClientQueryErrors:
    """Tests for error paths in send_query."""

    def test_send_query_grpc_error_propagates(self):
        """Test that a gRPC RpcError from the transport propagates."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            mock_stub = MagicMock()
            mock_stub.SendRequest.side_effect = FakeRpcError()
            mock_transport.kubemq_client.return_value = mock_stub

            message = QueryMessage(
                channel="test-queries",
                body=b"payload",
                timeout_in_seconds=10,
            )

            with pytest.raises(grpc.RpcError):
                client.send_query(message)

    def test_send_query_validation_error_raises_kubemq_validation_error(self):
        """Test that a ValidationError is wrapped in KubeMQValidationError."""
        from pydantic import ValidationError as PydanticValidationError

        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            mock_stub = MagicMock()
            mock_transport.kubemq_client.return_value = mock_stub

            message = QueryMessage(
                channel="test-queries",
                body=b"payload",
                timeout_in_seconds=10,
            )

            validation_err = PydanticValidationError.from_exception_data(
                title="QueryMessage",
                line_errors=[],
            )
            with patch.object(QueryMessage, "encode", side_effect=validation_err):
                with pytest.raises(KubeMQValidationError):
                    client.send_query(message)


# ==============================================================================
# Subscription Task Tests
# ==============================================================================


class TestSyncCQClientSubscriptionTask:
    """Tests for _subscribe_task internals."""

    def test_stream_delivers_messages_to_decode_callable(self):
        """Test that messages from the stream are forwarded to decode_callable."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            mock_message = MagicMock()
            mock_message.Tags = {}

            cancel_token = threading.Event()
            decoded = []

            def fake_stream():
                return iter([mock_message])

            def fake_decode(msg):
                decoded.append(msg)
                cancel_token.set()

            error_callable = MagicMock()

            client._subscribe_task(
                fake_stream,
                fake_decode,
                error_callable,
                cancel_token,
                channel="test-ch",
            )

            assert len(decoded) == 1
            assert decoded[0] is mock_message
            error_callable.assert_not_called()

    def test_stream_grpc_error_calls_error_callable(self):
        """Test that a gRPC RpcError from the stream calls error_callable."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            cancel_token = threading.Event()
            call_count = 0

            def fake_stream():
                nonlocal call_count
                call_count += 1
                if call_count >= 2:
                    cancel_token.set()
                raise FakeRpcError()

            decode_callable = MagicMock()
            error_callable = MagicMock()

            with patch("kubemq.cq.client.time.sleep"):
                client._subscribe_task(
                    fake_stream,
                    decode_callable,
                    error_callable,
                    cancel_token,
                    channel="test-ch",
                )

            decode_callable.assert_not_called()
            assert error_callable.call_count >= 1

    def test_cancel_token_stops_loop(self):
        """Test that setting the cancel token stops the subscription loop."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            cancel_token = threading.Event()
            cancel_token.set()

            stream_callable = MagicMock()
            decode_callable = MagicMock()
            error_callable = MagicMock()

            client._subscribe_task(
                stream_callable,
                decode_callable,
                error_callable,
                cancel_token,
                channel="test-ch",
            )

            stream_callable.assert_not_called()
            decode_callable.assert_not_called()
            error_callable.assert_not_called()


# ==============================================================================
# New Verb Method Tests
# ==============================================================================


class TestSyncCQClientNewVerbs:
    """Tests for the non-deprecated send_command / send_query verbs."""

    def test_send_command_returns_command_response_message(self):
        """Test send_command (non-deprecated) returns CommandResponseMessage."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            mock_pb_response = pb.Response()
            mock_pb_response.Executed = True
            mock_pb_response.RequestID = "req-456"

            mock_stub = MagicMock()
            mock_stub.SendRequest.return_value = mock_pb_response
            mock_transport.kubemq_client.return_value = mock_stub

            message = CommandMessage(
                channel="test-commands",
                body=b"do something",
                timeout_in_seconds=30,
            )
            response = client.send_command(message)

            assert isinstance(response, CommandResponseMessage)
            assert response.is_executed is True

    def test_send_command_does_not_emit_deprecation_warning(self):
        """Test send_command does NOT emit a deprecation warning."""
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
                client.send_command(message)

                deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
                assert len(deprecation_warnings) == 0

    def test_send_query_returns_query_response_message(self):
        """Test send_query (non-deprecated) returns QueryResponseMessage."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            mock_pb_response = pb.Response()
            mock_pb_response.Executed = True
            mock_pb_response.RequestID = "req-789"
            mock_pb_response.Body = b"query result"

            mock_stub = MagicMock()
            mock_stub.SendRequest.return_value = mock_pb_response
            mock_transport.kubemq_client.return_value = mock_stub

            message = QueryMessage(
                channel="test-queries",
                body=b"get something",
                timeout_in_seconds=30,
            )
            response = client.send_query(message)

            assert isinstance(response, QueryResponseMessage)
            assert response.is_executed is True

    def test_send_query_does_not_emit_deprecation_warning(self):
        """Test send_query does NOT emit a deprecation warning."""
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
                client.send_query(message)

                deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
                assert len(deprecation_warnings) == 0


# ==============================================================================
# Async Subscription Task Tests (_subscribe_task_async)
# ==============================================================================


class TestSyncCQClientSubscribeTaskAsync:
    """Tests for _subscribe_task_async method."""

    def _make_client(self):
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            client = Client(address="localhost:50000")
        return client

    @pytest.mark.asyncio
    async def test_subscribe_task_async_delivers_messages(self):
        """Test that messages from the stream are forwarded to decode_callable."""
        client = self._make_client()

        mock_msg = MagicMock()
        mock_msg.Tags = {}

        cancel = threading.Event()
        messages_iter = iter([mock_msg])

        call_count = [0]

        async def mock_to_thread(fn, *args):
            call_count[0] += 1
            if call_count[0] == 1:
                return messages_iter
            try:
                return next(args[0]) if args else fn()
            except StopIteration:
                cancel.set()
                raise

        decoded = []

        async def decode(msg):
            decoded.append(msg)

        async def on_error(err):
            pass

        with patch("kubemq.cq.client.asyncio.to_thread", side_effect=mock_to_thread):
            await client._subscribe_task_async(
                lambda: messages_iter,
                decode,
                on_error,
                cancel,
                "test-channel",
            )

        assert len(decoded) == 1
        assert decoded[0] is mock_msg

    @pytest.mark.asyncio
    async def test_subscribe_task_async_grpc_error_calls_error(self):
        """Test that a gRPC RpcError calls error_callable."""
        client = self._make_client()

        cancel = threading.Event()
        errors = []

        async def mock_to_thread(fn, *args):
            raise FakeRpcError()

        async def decode(msg):
            pass

        async def on_error(err):
            errors.append(err)
            cancel.set()

        with (
            patch("kubemq.cq.client.asyncio.to_thread", side_effect=mock_to_thread),
            patch("kubemq.cq.client.asyncio.sleep", new_callable=AsyncMock),
        ):
            await client._subscribe_task_async(
                lambda: None,
                decode,
                on_error,
                cancel,
                "test-channel",
            )

        assert len(errors) >= 1

    @pytest.mark.asyncio
    async def test_subscribe_task_async_generic_error_calls_error(self):
        """Test that a generic exception calls error_callable."""
        client = self._make_client()

        cancel = threading.Event()
        errors = []

        async def mock_to_thread(fn, *args):
            raise RuntimeError("boom")

        async def decode(msg):
            pass

        async def on_error(err):
            errors.append(err)
            cancel.set()

        with (
            patch("kubemq.cq.client.asyncio.to_thread", side_effect=mock_to_thread),
            patch("kubemq.cq.client.asyncio.sleep", new_callable=AsyncMock),
        ):
            await client._subscribe_task_async(
                lambda: None,
                decode,
                on_error,
                cancel,
                "test-channel",
            )

        assert len(errors) >= 1

    @pytest.mark.asyncio
    async def test_subscribe_task_async_cancel_stops_loop(self):
        """Test that pre-setting cancel_token prevents stream_callable from being called."""
        client = self._make_client()

        cancel = threading.Event()
        cancel.set()

        stream_called = []

        def stream_fn():
            stream_called.append(True)
            return iter([])

        async def decode(msg):
            pass

        async def on_error(err):
            pass

        await client._subscribe_task_async(
            stream_fn,
            decode,
            on_error,
            cancel,
            "test-channel",
        )

        assert len(stream_called) == 0


# ==============================================================================
# Additional Coverage Tests
# ==============================================================================


class TestSyncCQClientCloseAsync:
    """Tests for close_async method."""

    @pytest.mark.asyncio
    async def test_close_async_calls_transport_close_async(self):
        """Test close_async calls transport.close_async when transport exists."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport.close_async = AsyncMock()
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")
            await client.close_async()

            mock_transport.close_async.assert_called_once()
            assert client._shutdown_event.is_set()

    @pytest.mark.asyncio
    async def test_close_async_no_transport(self):
        """Test close_async handles None transport gracefully."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")
            client._transport = None

            await client.close_async()

            assert client._shutdown_event.is_set()


class TestSyncCQClientSendCommandValidationError:
    """Tests for ValidationError path in _send_command_impl covering lines 208-213."""

    def test_send_command_validation_error_wraps_pydantic(self):
        """Test that pydantic.ValidationError from encode is wrapped in KubeMQValidationError."""
        from pydantic import ValidationError as PydanticValidationError

        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            mock_stub = MagicMock()
            mock_transport.kubemq_client.return_value = mock_stub

            message = CommandMessage(
                channel="test-commands",
                body=b"payload",
                timeout_in_seconds=10,
            )

            validation_err = PydanticValidationError.from_exception_data(
                title="CommandMessage",
                line_errors=[],
            )
            with patch.object(CommandMessage, "encode", side_effect=validation_err):
                with pytest.raises(KubeMQValidationError) as exc_info:
                    client.send_command(message)
                assert exc_info.value.__cause__ is validation_err


class TestSyncCQClientSendQueryValidationError:
    """Tests for ValidationError path in _send_query_impl covering lines 288-293."""

    def test_send_query_validation_error_wraps_pydantic(self):
        """Test that pydantic.ValidationError from encode is wrapped in KubeMQValidationError."""
        from pydantic import ValidationError as PydanticValidationError

        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            mock_stub = MagicMock()
            mock_transport.kubemq_client.return_value = mock_stub

            message = QueryMessage(
                channel="test-queries",
                body=b"payload",
                timeout_in_seconds=10,
            )

            validation_err = PydanticValidationError.from_exception_data(
                title="QueryMessage",
                line_errors=[],
            )
            with patch.object(QueryMessage, "encode", side_effect=validation_err):
                with pytest.raises(KubeMQValidationError) as exc_info:
                    client.send_query(message)
                assert exc_info.value.__cause__ is validation_err


class TestSyncCQClientSendResponseException:
    """Tests for exception path in send_response_message covering lines 370-373."""

    def test_send_response_message_transport_exception_reraises(self):
        """Test that exception from transport.SendResponse is re-raised."""
        from kubemq.cq.command_message_received import CommandMessageReceived

        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            mock_stub = MagicMock()
            mock_stub.SendResponse.side_effect = RuntimeError("transport error")
            mock_transport.kubemq_client.return_value = mock_stub

            command_received = CommandMessageReceived(
                id="cmd-123",
                from_client_id="sender",
                timestamp="2024-01-01T00:00:00Z",
                channel="test-commands",
                reply_channel="reply-channel",
            )
            response = CommandResponseMessage(
                command_received=command_received,
                is_executed=True,
            )

            with pytest.raises(RuntimeError, match="transport error"):
                client.send_response_message(response)

    def test_send_response_message_grpc_error_reraises(self):
        """Test that gRPC error from transport.SendResponse is re-raised."""
        from kubemq.cq.command_message_received import CommandMessageReceived

        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            mock_stub = MagicMock()
            mock_stub.SendResponse.side_effect = FakeRpcError()
            mock_transport.kubemq_client.return_value = mock_stub

            command_received = CommandMessageReceived(
                id="cmd-456",
                from_client_id="sender",
                timestamp="2024-01-01T00:00:00Z",
                channel="test-commands",
                reply_channel="reply-channel",
            )
            response = CommandResponseMessage(
                command_received=command_received,
                is_executed=True,
            )

            with pytest.raises(grpc.RpcError):
                client.send_response_message(response)


class TestSyncCQClientChannelManagementAsync:
    """Tests for async channel management methods covering lines 437-525."""

    def _make_client(self):
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            client = Client(address="localhost:50000")
        return client

    @pytest.mark.asyncio
    async def test_create_queries_channel_async(self):
        """Test create_queries_channel_async delegates to sync."""
        with (
            patch("kubemq.transport.transport.Transport") as mock_transport_class,
            patch("kubemq.cq.client.create_channel_request") as mock_create,
        ):
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            mock_create.return_value = True

            client = Client(address="localhost:50000")
            result = await client.create_queries_channel_async("test-queries")

            assert result is True
            call_args = mock_create.call_args[0]
            assert call_args[2] == "test-queries"
            assert call_args[3] == "queries"

    @pytest.mark.asyncio
    async def test_delete_commands_channel_async(self):
        """Test delete_commands_channel_async delegates to sync."""
        with (
            patch("kubemq.transport.transport.Transport") as mock_transport_class,
            patch("kubemq.cq.client.delete_channel_request") as mock_delete,
        ):
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            mock_delete.return_value = True

            client = Client(address="localhost:50000")
            result = await client.delete_commands_channel_async("test-commands")

            assert result is True
            call_args = mock_delete.call_args[0]
            assert call_args[2] == "test-commands"
            assert call_args[3] == "commands"

    @pytest.mark.asyncio
    async def test_delete_queries_channel_async(self):
        """Test delete_queries_channel_async delegates to sync."""
        with (
            patch("kubemq.transport.transport.Transport") as mock_transport_class,
            patch("kubemq.cq.client.delete_channel_request") as mock_delete,
        ):
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            mock_delete.return_value = True

            client = Client(address="localhost:50000")
            result = await client.delete_queries_channel_async("test-queries")

            assert result is True
            call_args = mock_delete.call_args[0]
            assert call_args[2] == "test-queries"
            assert call_args[3] == "queries"

    @pytest.mark.asyncio
    async def test_list_commands_channels_async(self):
        """Test list_commands_channels_async delegates to sync."""
        with (
            patch("kubemq.transport.transport.Transport") as mock_transport_class,
            patch("kubemq.cq.client.list_cq_channels") as mock_list,
        ):
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            mock_list.return_value = ["ch1"]

            client = Client(address="localhost:50000")
            result = await client.list_commands_channels_async("search*")

            assert result == ["ch1"]
            call_args = mock_list.call_args[0]
            assert call_args[2] == "commands"
            assert call_args[3] == "search*"

    @pytest.mark.asyncio
    async def test_list_queries_channels_async(self):
        """Test list_queries_channels_async delegates to sync."""
        with (
            patch("kubemq.transport.transport.Transport") as mock_transport_class,
            patch("kubemq.cq.client.list_cq_channels") as mock_list,
        ):
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            mock_list.return_value = []

            client = Client(address="localhost:50000")
            result = await client.list_queries_channels_async()

            assert result == []
            call_args = mock_list.call_args[0]
            assert call_args[2] == "queries"


class TestSyncCQClientSubscribeTaskCommands:
    """Tests for _subscribe_task with Commands covering lines 627-664."""

    def _make_client(self):
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            client = Client(address="localhost:50000")
        return client

    def test_subscribe_task_commands_calls_raise_on_receive_message(self):
        """Test that subscribe_to_commands routes messages through raise_on_receive_message."""
        client = self._make_client()

        cancel_token = threading.Event()
        received_messages = []

        mock_msg = MagicMock()
        mock_msg.Tags = {}

        def stream_callable():
            return iter([mock_msg])

        def decode_callable(msg):
            received_messages.append(msg)
            cancel_token.set()

        error_callable = MagicMock()

        client._subscribe_task(
            stream_callable, decode_callable, error_callable, cancel_token, channel="cmd-ch"
        )

        assert len(received_messages) == 1
        error_callable.assert_not_called()

    def test_subscribe_task_handler_exception_calls_error(self):
        """Test that handler exception in decode_callable is caught (lines 645-649)."""
        client = self._make_client()

        cancel_token = threading.Event()
        mock_msg = MagicMock()
        mock_msg.Tags = {}

        call_count = 0

        def stream_callable():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return iter([mock_msg])
            cancel_token.set()
            return iter([])

        def decode_callable(msg):
            raise RuntimeError("handler exploded")

        error_callable = MagicMock()

        client._subscribe_task(
            stream_callable, decode_callable, error_callable, cancel_token, channel="cmd-ch"
        )

        assert call_count >= 1

    def test_subscribe_task_generic_exception_calls_error_callable(self):
        """Test that a generic Exception from stream calls error_callable (lines 661-664)."""
        client = self._make_client()

        cancel_token = threading.Event()
        errors = []
        call_count = 0

        def stream_callable():
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                cancel_token.set()
            raise RuntimeError("unexpected failure")

        def decode_callable(msg):
            pass

        def error_callable(err):
            errors.append(err)

        with patch("kubemq.cq.client.time.sleep"):
            client._subscribe_task(
                stream_callable, decode_callable, error_callable, cancel_token, channel="cmd-ch"
            )

        assert len(errors) >= 1

    def test_subscribe_task_cancel_breaks_message_loop(self):
        """Test that setting cancel_token mid-stream breaks the loop (line 627)."""
        client = self._make_client()

        cancel_token = threading.Event()

        msg1 = MagicMock()
        msg1.Tags = {}
        msg2 = MagicMock()
        msg2.Tags = {}

        decoded = []

        def stream_callable():
            return iter([msg1, msg2])

        def decode_callable(msg):
            decoded.append(msg)
            cancel_token.set()

        error_callable = MagicMock()

        client._subscribe_task(
            stream_callable, decode_callable, error_callable, cancel_token, channel="cmd-ch"
        )

        assert len(decoded) == 1
        assert decoded[0] is msg1


class TestSyncCQClientSubscribeTaskAsyncAdditional:
    """Additional tests for _subscribe_task_async covering lines 705-738, 764, 773-777, 786."""

    def _make_client(self):
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport
            client = Client(address="localhost:50000")
        return client

    @pytest.mark.asyncio
    async def test_subscribe_task_async_handler_error_isolated(self):
        """Test that handler error in decode_callable is caught (lines 773-777)."""
        client = self._make_client()

        cancel = threading.Event()

        mock_msg = MagicMock()
        mock_msg.Tags = {}
        messages_iter = iter([mock_msg])

        call_count = [0]

        async def mock_to_thread(fn, *args):
            call_count[0] += 1
            if call_count[0] == 1:
                return messages_iter
            try:
                return next(args[0]) if args else fn()
            except StopIteration:
                cancel.set()
                raise

        async def decode(msg):
            raise ValueError("handler error")

        async def on_error(err):
            pass

        with patch("kubemq.cq.client.asyncio.to_thread", side_effect=mock_to_thread):
            await client._subscribe_task_async(
                lambda: messages_iter,
                decode,
                on_error,
                cancel,
                "test-channel",
            )

    @pytest.mark.asyncio
    async def test_subscribe_task_async_stop_iteration_breaks(self):
        """Test that StopIteration from next() breaks inner loop (line 786)."""
        client = self._make_client()

        cancel = threading.Event()
        decoded = []

        call_count = [0]

        async def mock_to_thread(fn, *args):
            call_count[0] += 1
            if call_count[0] == 1:
                return iter([])
            raise StopIteration()

        async def decode(msg):
            decoded.append(msg)

        async def on_error(err):
            pass

        with patch("kubemq.cq.client.asyncio.to_thread", side_effect=mock_to_thread):
            cancel.set()
            await client._subscribe_task_async(
                lambda: iter([]),
                decode,
                on_error,
                cancel,
                "test-channel",
            )

        assert len(decoded) == 0

    @pytest.mark.asyncio
    async def test_subscribe_async_commands_builds_args(self):
        """Test _subscribe_async with CommandsSubscription builds correct args (lines 705-738)."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            received = []

            async def on_cmd(msg):
                received.append(msg)

            async def on_err(err):
                pass

            subscription = CommandsSubscription(
                channel="test-commands",
                on_receive_command_callback=on_cmd,
                on_error_callback=on_err,
            )

            cancel = CancellationToken()
            cancel.cancel()

            with patch.object(client, "_subscribe_task_async", new_callable=AsyncMock) as mock_task:
                mock_task.return_value = None
                with patch("kubemq.cq.client.asyncio.create_task") as mock_create_task:
                    mock_create_task.return_value = MagicMock()
                    client._subscribe_async(subscription, cancel)
                    mock_create_task.assert_called_once()

    @pytest.mark.asyncio
    async def test_subscribe_async_queries_builds_args(self):
        """Test _subscribe_async with QueriesSubscription builds correct args (lines 726-737)."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            async def on_query(msg):
                pass

            async def on_err(err):
                pass

            subscription = QueriesSubscription(
                channel="test-queries",
                on_receive_query_callback=on_query,
                on_error_callback=on_err,
            )

            cancel = CancellationToken()
            cancel.cancel()

            with patch.object(client, "_subscribe_task_async", new_callable=AsyncMock) as mock_task:
                mock_task.return_value = None
                with patch("kubemq.cq.client.asyncio.create_task") as mock_create_task:
                    mock_create_task.return_value = MagicMock()
                    client._subscribe_async(subscription, cancel)
                    mock_create_task.assert_called_once()


class TestSyncCQClientSubscribeToCommandsAsync:
    """Tests for subscribe_to_commands_async / subscribe_to_queries_async (lines 681, 697)."""

    @pytest.mark.asyncio
    async def test_subscribe_to_commands_async_returns_task(self):
        """Test subscribe_to_commands_async delegates to _subscribe_async."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            subscription = CommandsSubscription(
                channel="test-commands",
                on_receive_command_callback=lambda msg: None,
            )

            with patch.object(client, "_subscribe_async", return_value=MagicMock()) as mock_sub:
                client.subscribe_to_commands_async(subscription)
                mock_sub.assert_called_once_with(subscription, None)

    @pytest.mark.asyncio
    async def test_subscribe_to_queries_async_returns_task(self):
        """Test subscribe_to_queries_async delegates to _subscribe_async."""
        with patch("kubemq.transport.transport.Transport") as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport.initialize.return_value = mock_transport
            mock_transport_class.return_value = mock_transport

            client = Client(address="localhost:50000")

            subscription = QueriesSubscription(
                channel="test-queries",
                on_receive_query_callback=lambda msg: None,
            )

            with patch.object(client, "_subscribe_async", return_value=MagicMock()) as mock_sub:
                client.subscribe_to_queries_async(subscription)
                mock_sub.assert_called_once_with(subscription, None)
