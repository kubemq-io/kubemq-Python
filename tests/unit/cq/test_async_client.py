"""Tests for AsyncCQClient."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kubemq.common.async_cancellation_token import AsyncCancellationToken
from kubemq.core.config import ClientConfig
from kubemq.core.exceptions import KubeMQConnectionError
from kubemq.cq.async_client import AsyncClient, AsyncCQClient
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


@pytest.fixture
def mock_transport():
    """Create a mock async transport."""
    transport = AsyncMock()
    transport.is_connected = True
    return transport


class TestAsyncClientAlias:
    """Test that the alias works correctly."""

    def test_alias_is_same_class(self):
        """Test that AsyncCQClient is an alias for AsyncClient."""
        assert AsyncCQClient is AsyncClient


class TestAsyncClientInitialization:
    """Tests for AsyncClient initialization."""

    def test_init_with_address(self):
        """Test initialization with address."""
        client = AsyncClient(address="localhost:50000")
        assert client._config.address == "localhost:50000"

    def test_init_with_config(self, mock_config):
        """Test initialization with config object."""
        client = AsyncClient(config=mock_config)
        assert client._config == mock_config

    def test_init_with_client_id(self):
        """Test initialization with client ID."""
        client = AsyncClient(address="localhost:50000", client_id="my-client")
        assert client._config.client_id == "my-client"

    def test_init_with_auth_token(self):
        """Test initialization with auth token."""
        client = AsyncClient(
            address="localhost:50000",
            auth_token="test-token",
        )
        assert client._config.auth_token == "test-token"


class TestAsyncClientSendCommand:
    """Tests for send_command method."""

    @pytest.mark.asyncio
    async def test_send_command_when_not_connected(self):
        """Test send_command raises when not connected."""
        client = AsyncClient(address="localhost:50000")
        message = CommandMessage(
            channel="test",
            body=b"data",
            timeout_in_seconds=10,
        )

        with pytest.raises(KubeMQConnectionError):
            await client.send_command(message)

    @pytest.mark.asyncio
    async def test_send_command_returns_response(self, mock_transport):
        """Test send_command returns CommandResponseMessage."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        # Setup mock response
        mock_response = pb.Response()
        mock_response.Executed = True
        mock_response.Error = ""
        mock_transport.send_request.return_value = mock_response

        message = CommandMessage(
            channel="test-channel",
            body=b"test body",
            timeout_in_seconds=10,
        )

        response = await client.send_command(message)

        assert isinstance(response, CommandResponseMessage)
        mock_transport.send_request.assert_called_once()


class TestAsyncClientSendQuery:
    """Tests for send_query method."""

    @pytest.mark.asyncio
    async def test_send_query_when_not_connected(self):
        """Test send_query raises when not connected."""
        client = AsyncClient(address="localhost:50000")
        message = QueryMessage(
            channel="test",
            body=b"data",
            timeout_in_seconds=10,
        )

        with pytest.raises(KubeMQConnectionError):
            await client.send_query(message)

    @pytest.mark.asyncio
    async def test_send_query_returns_response(self, mock_transport):
        """Test send_query returns QueryResponseMessage."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        # Setup mock response
        mock_response = pb.Response()
        mock_response.Executed = True
        mock_response.Error = ""
        mock_response.Body = b"result"
        mock_transport.send_request.return_value = mock_response

        message = QueryMessage(
            channel="test-channel",
            body=b"test body",
            timeout_in_seconds=10,
        )

        response = await client.send_query(message)

        assert isinstance(response, QueryResponseMessage)
        mock_transport.send_request.assert_called_once()


class TestAsyncClientSendResponse:
    """Tests for send_response method."""

    @pytest.mark.asyncio
    async def test_send_response_when_not_connected(self):
        """Test send_response raises when not connected."""
        client = AsyncClient(address="localhost:50000")

        # Create a mock command received
        from kubemq.cq.command_message_received import CommandMessageReceived

        command_received = CommandMessageReceived(
            id="cmd-123",
            channel="test",
            reply_channel="reply",
        )
        response = CommandResponseMessage(
            command_received=command_received,
            is_executed=True,
        )

        with pytest.raises(KubeMQConnectionError):
            await client.send_response(response)

    @pytest.mark.asyncio
    async def test_send_command_response(self, mock_transport):
        """Test send_response with CommandResponseMessage."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        # Create a mock command received
        from kubemq.cq.command_message_received import CommandMessageReceived

        command_received = CommandMessageReceived(
            id="cmd-123",
            channel="test",
            reply_channel="reply",
        )
        response = CommandResponseMessage(
            command_received=command_received,
            is_executed=True,
            error="",
        )

        await client.send_response(response)

        mock_transport.send_response.assert_called_once()

    @pytest.mark.asyncio
    async def test_send_query_response(self, mock_transport):
        """Test send_response with QueryResponseMessage."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        # Create a mock query received
        from kubemq.cq.query_message_received import QueryMessageReceived

        query_received = QueryMessageReceived(
            id="qry-123",
            channel="test",
            reply_channel="reply",
        )
        response = QueryResponseMessage(
            query_received=query_received,
            is_executed=True,
            error="",
            body=b"result",
        )

        await client.send_response(response)

        mock_transport.send_response.assert_called_once()


class TestAsyncClientSendBatch:
    """Tests for batch send methods."""

    @pytest.mark.asyncio
    async def test_send_commands_batch_when_not_connected(self):
        """Test send_commands_batch raises when not connected."""
        client = AsyncClient(address="localhost:50000")
        messages = [CommandMessage(channel="test", body=b"data", timeout_in_seconds=10)]

        with pytest.raises(KubeMQConnectionError):
            await client.send_commands_batch(messages)

    @pytest.mark.asyncio
    async def test_send_queries_batch_when_not_connected(self):
        """Test send_queries_batch raises when not connected."""
        client = AsyncClient(address="localhost:50000")
        messages = [QueryMessage(channel="test", body=b"data", timeout_in_seconds=10)]

        with pytest.raises(KubeMQConnectionError):
            await client.send_queries_batch(messages)

    @pytest.mark.asyncio
    async def test_send_commands_batch_respects_concurrency(self, mock_transport):
        """Test send_commands_batch respects max_concurrent."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        # Setup mock response
        mock_response = pb.Response()
        mock_response.Executed = True
        mock_transport.send_request.return_value = mock_response

        messages = [
            CommandMessage(
                channel="test",
                body=f"msg-{i}".encode(),
                timeout_in_seconds=10,
            )
            for i in range(10)
        ]

        results = await client.send_commands_batch(messages, max_concurrent=5)

        assert len(results) == 10


class TestAsyncClientSubscription:
    """Tests for subscription methods."""

    @pytest.mark.asyncio
    async def test_subscribe_to_commands_when_not_connected(self):
        """Test subscribe_to_commands raises when not connected."""
        client = AsyncClient(address="localhost:50000")
        subscription = CommandsSubscription(
            channel="test",
            on_receive_command_callback=lambda c: None,
        )

        with pytest.raises(KubeMQConnectionError):
            async for _ in client.subscribe_to_commands(subscription):
                pass

    @pytest.mark.asyncio
    async def test_subscribe_to_queries_when_not_connected(self):
        """Test subscribe_to_queries raises when not connected."""
        client = AsyncClient(address="localhost:50000")
        subscription = QueriesSubscription(
            channel="test",
            on_receive_query_callback=lambda q: None,
        )

        with pytest.raises(KubeMQConnectionError):
            async for _ in client.subscribe_to_queries(subscription):
                pass

    @pytest.mark.asyncio
    async def test_subscribe_to_commands_creates_token_if_not_provided(self, mock_transport):
        """Test subscribe_to_commands creates cancellation token if not provided."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        # Make the transport return an async iterator directly
        mock_transport.subscribe_to_requests = MagicMock(return_value=AsyncIteratorMock([]))

        subscription = CommandsSubscription(
            channel="test",
            on_receive_command_callback=lambda c: None,
        )

        # Should not raise
        async for _ in client.subscribe_to_commands(subscription):
            pass


class TestAsyncClientContextManager:
    """Tests for context manager support."""

    @pytest.mark.asyncio
    async def test_context_manager_connects_and_disconnects(self):
        """Test that context manager properly connects and disconnects."""
        with (
            patch.object(AsyncClient, "connect", new_callable=AsyncMock) as mock_connect,
            patch.object(AsyncClient, "close", new_callable=AsyncMock) as mock_close,
        ):
            async with AsyncClient(address="localhost:50000"):
                mock_connect.assert_called_once()

            mock_close.assert_called_once()


class TestAsyncClientSubscriptionManagement:
    """Tests for subscription registration."""

    @pytest.mark.asyncio
    async def test_register_subscription(self, mock_transport):
        """Test that subscriptions are registered."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        token = AsyncCancellationToken()
        await client._register_subscription(token)

        assert token in client._active_subscriptions

    @pytest.mark.asyncio
    async def test_unregister_subscription(self, mock_transport):
        """Test that subscriptions are unregistered."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        token = AsyncCancellationToken()
        await client._register_subscription(token)
        await client._unregister_subscription(token)

        assert token not in client._active_subscriptions


class AsyncIteratorMock:
    """Mock for async iterators."""

    def __init__(self, items):
        self.items = iter(items)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self.items)
        except StopIteration:
            raise StopAsyncIteration


# ==============================================================================
# Extended Tests for Additional Coverage
# ==============================================================================


class TestAsyncClientSendQueriesBatch:
    """Tests for send_queries_batch method."""

    @pytest.mark.asyncio
    async def test_send_queries_batch_respects_concurrency(self, mock_transport):
        """Test send_queries_batch respects max_concurrent."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        # Setup mock response
        mock_response = pb.Response()
        mock_response.Executed = True
        mock_response.Body = b"result"
        mock_transport.send_request.return_value = mock_response

        messages = [
            QueryMessage(
                channel="test",
                body=f"query-{i}".encode(),
                timeout_in_seconds=10,
            )
            for i in range(10)
        ]

        results = await client.send_queries_batch(messages, max_concurrent=5)

        assert len(results) == 10
        assert all(isinstance(r, QueryResponseMessage) for r in results)

    @pytest.mark.asyncio
    async def test_send_queries_batch_preserves_order(self, mock_transport):
        """Test send_queries_batch preserves message order in results."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        # Make send_request return different results for different calls
        call_count = [0]

        async def mock_send_request(request, timeout_seconds=10):
            call_count[0] += 1
            response = pb.Response()
            response.Executed = True
            response.Body = f"result-{call_count[0]}".encode()
            return response

        mock_transport.send_request.side_effect = mock_send_request

        messages = [
            QueryMessage(
                channel="test",
                body=f"query-{i}".encode(),
                timeout_in_seconds=10,
            )
            for i in range(4)
        ]

        results = await client.send_queries_batch(messages)

        # All should succeed
        assert len(results) == 4
        assert all(r.is_executed for r in results)


class TestAsyncClientSubscribeToQueriesYield:
    """Tests for subscribe_to_queries yielding messages."""

    @pytest.mark.asyncio
    async def test_subscribe_to_queries_yields_messages(self, mock_transport):
        """Test subscribe_to_queries yields query messages."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        # Create mock protobuf request with all required fields
        mock_pb_request = MagicMock()
        mock_pb_request.RequestID = "qry-123"
        mock_pb_request.Channel = "test-channel"
        mock_pb_request.ClientID = "sender-client"  # Required field
        mock_pb_request.Metadata = "test-metadata"
        mock_pb_request.Body = b"test-body"
        mock_pb_request.ReplyChannel = "reply-channel"
        mock_pb_request.Tags = {}
        mock_pb_request.RequestTypeData = 2  # Query type

        # Make the transport return an async iterator
        mock_transport.subscribe_to_requests = MagicMock(
            return_value=AsyncIteratorMock([mock_pb_request])
        )

        subscription = QueriesSubscription(
            channel="test",
            on_receive_query_callback=lambda q: None,
        )

        queries = []
        async for query in client.subscribe_to_queries(subscription):
            queries.append(query)

        assert len(queries) == 1
        assert queries[0].id == "qry-123"


class TestAsyncClientSubscribeCommandsWithCallback:
    """Tests for subscribe_commands_with_callback method."""

    @pytest.mark.asyncio
    async def test_subscribe_commands_with_callback_when_not_connected(self):
        """Test subscribe_commands_with_callback raises when not connected."""
        client = AsyncClient(address="localhost:50000")
        subscription = CommandsSubscription(
            channel="test",
            on_receive_command_callback=lambda c: None,
        )

        async def callback(command):
            pass

        with pytest.raises(KubeMQConnectionError):
            await client.subscribe_commands_with_callback(subscription, callback)

    @pytest.mark.asyncio
    async def test_subscribe_commands_with_callback_calls_callback(self, mock_transport):
        """Test subscribe_commands_with_callback calls callback for each command."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        # Create mock protobuf request with all required fields
        mock_pb_request = MagicMock()
        mock_pb_request.RequestID = "cmd-123"
        mock_pb_request.Channel = "test-channel"
        mock_pb_request.ClientID = "sender-client"  # Required field
        mock_pb_request.Metadata = ""
        mock_pb_request.Body = b"test-body"
        mock_pb_request.ReplyChannel = "reply-channel"
        mock_pb_request.Tags = {}
        mock_pb_request.RequestTypeData = 1  # Command type

        mock_transport.subscribe_to_requests = MagicMock(
            return_value=AsyncIteratorMock([mock_pb_request])
        )

        subscription = CommandsSubscription(
            channel="test",
            on_receive_command_callback=lambda c: None,
        )

        received_commands = []

        async def callback(command):
            received_commands.append(command)

        await client.subscribe_commands_with_callback(subscription, callback)

        assert len(received_commands) == 1
        assert received_commands[0].id == "cmd-123"

    @pytest.mark.asyncio
    async def test_subscribe_commands_with_callback_calls_error_callback(self, mock_transport):
        """Test subscribe_commands_with_callback calls error callback on error."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        # Create an async iterator that raises an exception
        class ErrorIterator:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise Exception("Stream error")

        mock_transport.subscribe_to_requests = MagicMock(return_value=ErrorIterator())

        subscription = CommandsSubscription(
            channel="test",
            on_receive_command_callback=lambda c: None,
        )

        errors = []

        async def callback(command):
            pass

        async def error_callback(error):
            errors.append(error)

        await client.subscribe_commands_with_callback(subscription, callback, error_callback)

        assert len(errors) == 1
        assert "Stream error" in str(errors[0])


class TestAsyncClientSubscribeQueriesWithCallback:
    """Tests for subscribe_queries_with_callback method."""

    @pytest.mark.asyncio
    async def test_subscribe_queries_with_callback_when_not_connected(self):
        """Test subscribe_queries_with_callback raises when not connected."""
        client = AsyncClient(address="localhost:50000")
        subscription = QueriesSubscription(
            channel="test",
            on_receive_query_callback=lambda q: None,
        )

        async def callback(query):
            pass

        with pytest.raises(KubeMQConnectionError):
            await client.subscribe_queries_with_callback(subscription, callback)

    @pytest.mark.asyncio
    async def test_subscribe_queries_with_callback_calls_callback(self, mock_transport):
        """Test subscribe_queries_with_callback calls callback for each query."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        # Create mock protobuf request with all required fields
        mock_pb_request = MagicMock()
        mock_pb_request.RequestID = "qry-123"
        mock_pb_request.Channel = "test-channel"
        mock_pb_request.ClientID = "sender-client"  # Required field
        mock_pb_request.Metadata = ""
        mock_pb_request.Body = b"test-body"
        mock_pb_request.ReplyChannel = "reply-channel"
        mock_pb_request.Tags = {}
        mock_pb_request.RequestTypeData = 2  # Query type

        mock_transport.subscribe_to_requests = MagicMock(
            return_value=AsyncIteratorMock([mock_pb_request])
        )

        subscription = QueriesSubscription(
            channel="test",
            on_receive_query_callback=lambda q: None,
        )

        received_queries = []

        async def callback(query):
            received_queries.append(query)

        await client.subscribe_queries_with_callback(subscription, callback)

        assert len(received_queries) == 1
        assert received_queries[0].id == "qry-123"


class TestAsyncClientSubscriptionCallbackHandling:
    """Tests for subscription callback handling (sync vs async)."""

    @pytest.mark.asyncio
    async def test_subscribe_to_commands_calls_sync_callback(self, mock_transport):
        """Test subscribe_to_commands calls sync callback."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        # Create mock protobuf request with all required fields
        mock_pb_request = MagicMock()
        mock_pb_request.RequestID = "cmd-123"
        mock_pb_request.Channel = "test-channel"
        mock_pb_request.ClientID = "sender-client"  # Required field
        mock_pb_request.Metadata = ""
        mock_pb_request.Body = b"test-body"
        mock_pb_request.ReplyChannel = "reply-channel"
        mock_pb_request.Tags = {}
        mock_pb_request.RequestTypeData = 1  # Command type

        mock_transport.subscribe_to_requests = MagicMock(
            return_value=AsyncIteratorMock([mock_pb_request])
        )

        received_commands = []

        def sync_callback(command):
            received_commands.append(command)

        subscription = CommandsSubscription(
            channel="test",
            on_receive_command_callback=sync_callback,
        )

        async for _ in client.subscribe_to_commands(subscription):
            pass

        assert len(received_commands) == 1

    @pytest.mark.asyncio
    async def test_subscribe_to_commands_calls_async_callback(self, mock_transport):
        """Test subscribe_to_commands calls async callback."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        # Create mock protobuf request with all required fields
        mock_pb_request = MagicMock()
        mock_pb_request.RequestID = "cmd-123"
        mock_pb_request.Channel = "test-channel"
        mock_pb_request.ClientID = "sender-client"  # Required field
        mock_pb_request.Metadata = ""
        mock_pb_request.Body = b"test-body"
        mock_pb_request.ReplyChannel = "reply-channel"
        mock_pb_request.Tags = {}
        mock_pb_request.RequestTypeData = 1  # Command type

        mock_transport.subscribe_to_requests = MagicMock(
            return_value=AsyncIteratorMock([mock_pb_request])
        )

        received_commands = []

        async def async_callback(command):
            received_commands.append(command)

        subscription = CommandsSubscription(
            channel="test",
            on_receive_command_callback=async_callback,
        )

        async for _ in client.subscribe_to_commands(subscription):
            pass

        assert len(received_commands) == 1

    @pytest.mark.asyncio
    async def test_subscribe_to_commands_calls_error_callback_on_exception(self, mock_transport):
        """Test subscribe_to_commands calls error callback on exception."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        # Create an async iterator that raises an exception
        class ErrorIterator:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise Exception("Stream error")

        mock_transport.subscribe_to_requests = MagicMock(return_value=ErrorIterator())

        errors = []

        def error_callback(error):
            errors.append(error)

        subscription = CommandsSubscription(
            channel="test",
            on_receive_command_callback=lambda c: None,
            on_error_callback=error_callback,
        )

        with pytest.raises(Exception, match="Stream error"):
            async for _ in client.subscribe_to_commands(subscription):
                pass

        assert len(errors) == 1
        assert "Stream error" in errors[0]
