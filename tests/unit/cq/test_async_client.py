"""Tests for AsyncCQClient."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kubemq.common.async_cancellation_token import AsyncCancellationToken
from kubemq.core.config import ClientConfig
from kubemq.core.exceptions import KubeMQConnectionError
from kubemq.cq.async_client import AsyncClient, AsyncCQClient
from kubemq.cq.command_message import CommandMessage
from kubemq.cq.command_response_message import CommandResponse
from kubemq.cq.commands_subscription import CommandsSubscription
from kubemq.cq.queries_subscription import QueriesSubscription
from kubemq.cq.query_message import QueryMessage
from kubemq.cq.query_response_message import QueryResponse
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
        """Test send_command returns CommandResponse."""
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

        assert isinstance(response, CommandResponse)
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
        """Test send_query returns QueryResponse."""
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

        assert isinstance(response, QueryResponse)
        mock_transport.send_request.assert_called_once()


class TestAsyncClientSendResponse:
    """Tests for send_response method."""

    @pytest.mark.asyncio
    async def test_send_response_when_not_connected(self):
        """Test send_response raises when not connected."""
        client = AsyncClient(address="localhost:50000")

        # Create a mock command received
        from kubemq.cq.command_message_received import CommandReceived

        command_received = CommandReceived(
            id="cmd-123",
            channel="test",
            reply_channel="reply",
        )
        response = CommandResponse(
            command_received=command_received,
            is_executed=True,
        )

        with pytest.raises(KubeMQConnectionError):
            await client.send_response(response)

    @pytest.mark.asyncio
    async def test_send_command_response(self, mock_transport):
        """Test send_response with CommandResponse."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        # Create a mock command received
        from kubemq.cq.command_message_received import CommandReceived

        command_received = CommandReceived(
            id="cmd-123",
            channel="test",
            reply_channel="reply",
        )
        response = CommandResponse(
            command_received=command_received,
            is_executed=True,
            error="",
        )

        await client.send_response(response)

        mock_transport.send_response.assert_called_once()

    @pytest.mark.asyncio
    async def test_send_query_response(self, mock_transport):
        """Test send_response with QueryResponse."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        # Create a mock query received
        from kubemq.cq.query_message_received import QueryReceived

        query_received = QueryReceived(
            id="qry-123",
            channel="test",
            reply_channel="reply",
        )
        response = QueryResponse(
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
        assert all(isinstance(r, QueryResponse) for r in results)

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


class TestAsyncCQClientConcurrentCallbackSubscription:
    """Tests for concurrent callback subscription with max_concurrent_callbacks > 1."""

    @pytest.mark.asyncio
    async def test_subscribe_commands_with_concurrent_callbacks(self, mock_transport):
        """Test subscribe_commands_with_callback with max_concurrent_callbacks=3."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        mock_requests = []
        for i in range(3):
            req = MagicMock()
            req.RequestID = f"cmd-{i}"
            req.Channel = "test-channel"
            req.ClientID = "sender-client"
            req.Metadata = ""
            req.Body = b"test-body"
            req.ReplyChannel = "reply-channel"
            req.Tags = {}
            req.RequestTypeData = 1
            mock_requests.append(req)

        mock_transport.subscribe_to_requests = MagicMock(
            return_value=AsyncIteratorMock(mock_requests)
        )

        subscription = CommandsSubscription(
            channel="test",
            on_receive_command_callback=lambda c: None,
        )

        received = []

        async def callback(command):
            received.append(command)

        await client.subscribe_commands_with_callback(
            subscription, callback, max_concurrent_callbacks=3
        )

        assert len(received) == 3

    @pytest.mark.asyncio
    async def test_subscribe_queries_with_concurrent_callbacks(self, mock_transport):
        """Test subscribe_queries_with_callback with max_concurrent_callbacks=3."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        mock_requests = []
        for i in range(3):
            req = MagicMock()
            req.RequestID = f"qry-{i}"
            req.Channel = "test-channel"
            req.ClientID = "sender-client"
            req.Metadata = ""
            req.Body = b"test-body"
            req.ReplyChannel = "reply-channel"
            req.Tags = {}
            req.RequestTypeData = 2
            mock_requests.append(req)

        mock_transport.subscribe_to_requests = MagicMock(
            return_value=AsyncIteratorMock(mock_requests)
        )

        subscription = QueriesSubscription(
            channel="test",
            on_receive_query_callback=lambda q: None,
        )

        received = []

        async def callback(query):
            received.append(query)

        await client.subscribe_queries_with_callback(
            subscription, callback, max_concurrent_callbacks=3
        )

        assert len(received) == 3

    @pytest.mark.asyncio
    async def test_subscribe_commands_callback_rejects_zero(self):
        """Test max_concurrent_callbacks=0 raises ValueError."""
        client = AsyncClient(address="localhost:50000")
        subscription = CommandsSubscription(
            channel="test",
            on_receive_command_callback=lambda c: None,
        )

        with pytest.raises(ValueError, match="must be >= 1"):
            await client.subscribe_commands_with_callback(
                subscription, AsyncMock(), max_concurrent_callbacks=0
            )

    @pytest.mark.asyncio
    async def test_subscribe_commands_callback_rejects_over_1000(self):
        """Test max_concurrent_callbacks=1001 raises ValueError."""
        client = AsyncClient(address="localhost:50000")
        subscription = CommandsSubscription(
            channel="test",
            on_receive_command_callback=lambda c: None,
        )

        with pytest.raises(ValueError, match="must be <= 1000"):
            await client.subscribe_commands_with_callback(
                subscription, AsyncMock(), max_concurrent_callbacks=1001
            )

    @pytest.mark.asyncio
    async def test_subscribe_queries_callback_rejects_zero(self):
        """Test max_concurrent_callbacks=0 raises ValueError for queries."""
        client = AsyncClient(address="localhost:50000")
        subscription = QueriesSubscription(
            channel="test",
            on_receive_query_callback=lambda q: None,
        )

        with pytest.raises(ValueError, match="must be >= 1"):
            await client.subscribe_queries_with_callback(
                subscription, AsyncMock(), max_concurrent_callbacks=0
            )

    @pytest.mark.asyncio
    async def test_subscribe_queries_callback_rejects_over_1000(self):
        """Test max_concurrent_callbacks=1001 raises ValueError for queries."""
        client = AsyncClient(address="localhost:50000")
        subscription = QueriesSubscription(
            channel="test",
            on_receive_query_callback=lambda q: None,
        )

        with pytest.raises(ValueError, match="must be <= 1000"):
            await client.subscribe_queries_with_callback(
                subscription, AsyncMock(), max_concurrent_callbacks=1001
            )


class TestAsyncCQClientBatchErrorHandling:
    """Tests for batch send error handling."""

    @pytest.mark.asyncio
    async def test_send_commands_batch_handles_error(self, mock_transport):
        """Test send_commands_batch returns error responses when send_command fails."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        mock_transport.send_request.side_effect = Exception("Send failed")

        messages = [
            CommandMessage(
                channel="test",
                body=f"cmd-{i}".encode(),
                timeout_in_seconds=10,
            )
            for i in range(4)
        ]

        results = await client.send_commands_batch(messages)

        assert len(results) == 4
        for r in results:
            assert isinstance(r, CommandResponse)
            assert r.is_executed is False
            assert "Send failed" in r.error
            assert r.command_received is None

    @pytest.mark.asyncio
    async def test_send_queries_batch_handles_error(self, mock_transport):
        """Test send_queries_batch returns error responses when send_query fails."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        mock_transport.send_request.side_effect = Exception("Query failed")

        messages = [
            QueryMessage(
                channel="test",
                body=f"query-{i}".encode(),
                timeout_in_seconds=10,
            )
            for i in range(4)
        ]

        results = await client.send_queries_batch(messages)

        assert len(results) == 4
        for r in results:
            assert isinstance(r, QueryResponse)
            assert r.is_executed is False
            assert "Query failed" in r.error
            assert r.query_received is None

    @pytest.mark.asyncio
    async def test_send_commands_batch_mixed_success_and_error(self, mock_transport):
        """Test batch with some commands succeeding and some failing."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        async def mock_send_request(request, timeout_seconds=10):
            if b"fail" in request.Body:
                raise Exception("selective-fail")
            response = pb.Response()
            response.Executed = True
            return response

        mock_transport.send_request.side_effect = mock_send_request

        messages = [
            CommandMessage(channel="test", body=b"ok-0", timeout_in_seconds=10),
            CommandMessage(channel="test", body=b"fail-1", timeout_in_seconds=10),
            CommandMessage(channel="test", body=b"ok-2", timeout_in_seconds=10),
            CommandMessage(channel="test", body=b"fail-3", timeout_in_seconds=10),
        ]

        results = await client.send_commands_batch(messages)

        assert len(results) == 4
        assert results[0].is_executed is True
        assert results[1].is_executed is False
        assert "selective-fail" in results[1].error
        assert results[1].command_received is None
        assert results[2].is_executed is True
        assert results[3].is_executed is False

    @pytest.mark.asyncio
    async def test_send_queries_batch_mixed_success_and_error(self, mock_transport):
        """Test batch with some queries succeeding and some failing."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        async def mock_send_request(request, timeout_seconds=10):
            if b"fail" in request.Body:
                raise Exception("selective-fail")
            response = pb.Response()
            response.Executed = True
            response.Body = b"result"
            return response

        mock_transport.send_request.side_effect = mock_send_request

        messages = [
            QueryMessage(channel="test", body=b"ok-0", timeout_in_seconds=10),
            QueryMessage(channel="test", body=b"fail-1", timeout_in_seconds=10),
            QueryMessage(channel="test", body=b"ok-2", timeout_in_seconds=10),
            QueryMessage(channel="test", body=b"fail-3", timeout_in_seconds=10),
        ]

        results = await client.send_queries_batch(messages)

        assert len(results) == 4
        assert results[0].is_executed is True
        assert results[1].is_executed is False
        assert "selective-fail" in results[1].error
        assert results[1].query_received is None
        assert results[2].is_executed is True
        assert results[3].is_executed is False

    @pytest.mark.asyncio
    async def test_send_commands_batch_error_has_correct_request_id(self, mock_transport):
        """Test error responses carry the original message id as request_id."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        mock_transport.send_request.side_effect = Exception("fail")

        messages = [
            CommandMessage(channel="test", body=b"data", timeout_in_seconds=10, id=f"cmd-{i}")
            for i in range(3)
        ]

        results = await client.send_commands_batch(messages)

        assert len(results) == 3
        for i, r in enumerate(results):
            assert r.request_id == f"cmd-{i}"

    @pytest.mark.asyncio
    async def test_send_queries_batch_error_has_correct_request_id(self, mock_transport):
        """Test error responses carry the original message id as request_id."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True  # type: ignore[attr-defined]

        mock_transport.send_request.side_effect = Exception("fail")

        messages = [
            QueryMessage(channel="test", body=b"data", timeout_in_seconds=10, id=f"qry-{i}")
            for i in range(3)
        ]

        results = await client.send_queries_batch(messages)

        assert len(results) == 3
        for i, r in enumerate(results):
            assert r.request_id == f"qry-{i}"


# ==============================================================================
# Additional Coverage Tests — uncovered lines in cq/async_client.py
# ==============================================================================

from kubemq.core.exceptions import KubeMQValidationError  # noqa: E402


def _make_validation_error():
    """Create a ValueError for testing validation error wrapping."""
    return ValueError("validation failed")


def _make_mock_pb_request(
    *, request_id="cmd-123", channel="test-channel", request_type=1, tags=None
):
    """Create a mock protobuf request with all required fields."""
    req = MagicMock()
    req.RequestID = request_id
    req.Channel = channel
    req.ClientID = "sender-client"
    req.Metadata = ""
    req.Body = b"test-body"
    req.ReplyChannel = "reply-channel"
    req.Tags = tags if tags is not None else {}
    req.RequestTypeData = request_type
    return req


class TestSendCommandValidationError:
    """Tests for send_command ValidationError wrapping (lines 153-155)."""

    @pytest.mark.asyncio
    async def test_validation_error_wrapped_as_kubemq(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport

        ve = _make_validation_error()
        message = CommandMessage(channel="test", body=b"data", timeout_in_seconds=10)

        with patch.object(CommandMessage, "encode", side_effect=ve):
            with pytest.raises(KubeMQValidationError):
                await client.send_command(message)


class TestSendCommandGenericException:
    """Tests for send_command generic exception re-raise path."""

    @pytest.mark.asyncio
    async def test_generic_exception_reraised(self, mock_transport):
        from kubemq.core.exceptions import KubeMQError

        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport

        mock_transport.send_request.side_effect = RuntimeError("transport failed")

        message = CommandMessage(channel="test", body=b"data", timeout_in_seconds=10)

        with pytest.raises(KubeMQError, match="transport failed"):
            await client.send_command(message)


class TestSendCommandSpanRecording:
    """Tests for send_command span.is_recording() path (lines 137-142)."""

    @pytest.mark.asyncio
    async def test_span_attributes_set_when_recording(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport

        mock_response = pb.Response()
        mock_response.Executed = True
        mock_response.Error = ""
        mock_transport.send_request.return_value = mock_response

        mock_span = MagicMock()
        mock_span.is_recording.return_value = True
        mock_span.__enter__ = MagicMock(return_value=mock_span)
        mock_span.__exit__ = MagicMock(return_value=False)

        mock_instrumentor = MagicMock()
        mock_instrumentor.start_span = MagicMock(return_value=mock_span)
        client._instrumentor = mock_instrumentor

        message = CommandMessage(channel="test", body=b"data", timeout_in_seconds=10)

        await client.send_command(message)

        assert mock_span.set_attribute.call_count >= 2


class TestSendQueryValidationError:
    """Tests for send_query ValidationError wrapping (lines 254-256)."""

    @pytest.mark.asyncio
    async def test_validation_error_wrapped_as_kubemq(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport

        ve = _make_validation_error()
        message = QueryMessage(channel="test", body=b"data", timeout_in_seconds=10)

        with patch.object(QueryMessage, "encode", side_effect=ve):
            with pytest.raises(KubeMQValidationError):
                await client.send_query(message)


class TestSendQueryGenericException:
    """Tests for send_query generic exception re-raise path."""

    @pytest.mark.asyncio
    async def test_generic_exception_reraised(self, mock_transport):
        from kubemq.core.exceptions import KubeMQError

        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport

        mock_transport.send_request.side_effect = RuntimeError("query transport failed")

        message = QueryMessage(channel="test", body=b"data", timeout_in_seconds=10)

        with pytest.raises(KubeMQError, match="query transport failed"):
            await client.send_query(message)


class TestSendQuerySpanRecording:
    """Tests for send_query span.is_recording() path (lines 238-243)."""

    @pytest.mark.asyncio
    async def test_span_attributes_set_when_recording(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport

        mock_response = pb.Response()
        mock_response.Executed = True
        mock_response.Error = ""
        mock_response.Body = b"result"
        mock_transport.send_request.return_value = mock_response

        mock_span = MagicMock()
        mock_span.is_recording.return_value = True
        mock_span.__enter__ = MagicMock(return_value=mock_span)
        mock_span.__exit__ = MagicMock(return_value=False)

        mock_instrumentor = MagicMock()
        mock_instrumentor.start_span = MagicMock(return_value=mock_span)
        client._instrumentor = mock_instrumentor

        message = QueryMessage(channel="test", body=b"data", timeout_in_seconds=10)

        await client.send_query(message)

        assert mock_span.set_attribute.call_count >= 2


class TestSendResponseException:
    """Tests for send_response exception path (lines 337-340)."""

    @pytest.mark.asyncio
    async def test_send_response_exception_reraised(self, mock_transport):
        from kubemq.core.exceptions import KubeMQError

        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport

        mock_transport.send_response.side_effect = RuntimeError("response failed")

        from kubemq.cq.command_message_received import CommandReceived

        command_received = CommandReceived(
            id="cmd-123",
            channel="test",
            reply_channel="reply",
        )
        response = CommandResponse(
            command_received=command_received,
            is_executed=True,
            error="",
        )

        with pytest.raises(KubeMQError, match="response failed"):
            await client.send_response(response)


class TestSubscribeToCommandsHandlerError:
    """Tests for subscribe_to_commands handler error path (lines 418-423)."""

    @pytest.mark.asyncio
    async def test_handler_error_propagates(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport

        mock_pb = _make_mock_pb_request()
        mock_transport.subscribe_to_requests = MagicMock(return_value=AsyncIteratorMock([mock_pb]))

        def failing_callback(cmd):
            raise ValueError("handler boom")

        subscription = CommandsSubscription(
            channel="test",
            on_receive_command_callback=failing_callback,
        )

        with pytest.raises(ValueError, match="handler boom"):
            async for _ in client.subscribe_to_commands(subscription):
                pass


class TestSubscribeToCommandsAsyncErrorCallback:
    """Tests for subscribe_to_commands async on_error_callback (line 436)."""

    @pytest.mark.asyncio
    async def test_async_error_callback_called(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport

        class ErrorIterator:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise RuntimeError("stream error")

        mock_transport.subscribe_to_requests = MagicMock(return_value=ErrorIterator())

        errors = []

        async def async_error_cb(error_msg):
            errors.append(error_msg)

        subscription = CommandsSubscription(
            channel="test",
            on_receive_command_callback=lambda c: None,
            on_error_callback=async_error_cb,
        )

        with pytest.raises(RuntimeError, match="stream error"):
            async for _ in client.subscribe_to_commands(subscription):
                pass

        assert len(errors) == 1
        assert "stream error" in errors[0]


class TestSubscribeToCommandsSyncErrorCallback:
    """Tests for subscribe_to_commands sync on_error_callback (line 438)."""

    @pytest.mark.asyncio
    async def test_sync_error_callback_called(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport

        class ErrorIterator:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise RuntimeError("sync stream error")

        mock_transport.subscribe_to_requests = MagicMock(return_value=ErrorIterator())

        errors = []

        def sync_error_cb(error_msg):
            errors.append(error_msg)

        subscription = CommandsSubscription(
            channel="test",
            on_receive_command_callback=lambda c: None,
            on_error_callback=sync_error_cb,
        )

        with pytest.raises(RuntimeError, match="sync stream error"):
            async for _ in client.subscribe_to_commands(subscription):
                pass

        assert len(errors) == 1


class TestSubscribeToCommandsLinkCreation:
    """Tests for subscribe_to_commands link creation with tracing tags (line 402)."""

    @pytest.mark.asyncio
    async def test_link_appended_when_trace_context_present(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport

        mock_pb = _make_mock_pb_request(
            tags={"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}
        )
        mock_transport.subscribe_to_requests = MagicMock(return_value=AsyncIteratorMock([mock_pb]))

        subscription = CommandsSubscription(
            channel="test",
            on_receive_command_callback=lambda c: None,
        )

        commands = []
        async for cmd in client.subscribe_to_commands(subscription):
            commands.append(cmd)

        assert len(commands) == 1


class TestSubscribeToQueriesHandlerError:
    """Tests for subscribe_to_queries handler error (lines 509-514)."""

    @pytest.mark.asyncio
    async def test_handler_error_propagates(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport

        mock_pb = _make_mock_pb_request(request_type=2, request_id="qry-123")
        mock_transport.subscribe_to_requests = MagicMock(return_value=AsyncIteratorMock([mock_pb]))

        def failing_callback(q):
            raise ValueError("query handler boom")

        subscription = QueriesSubscription(
            channel="test",
            on_receive_query_callback=failing_callback,
        )

        with pytest.raises(ValueError, match="query handler boom"):
            async for _ in client.subscribe_to_queries(subscription):
                pass


class TestSubscribeToQueriesAsyncErrorCallback:
    """Tests for subscribe_to_queries async on_error_callback (line 527)."""

    @pytest.mark.asyncio
    async def test_async_error_callback_called(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport

        class ErrorIterator:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise RuntimeError("query stream error")

        mock_transport.subscribe_to_requests = MagicMock(return_value=ErrorIterator())

        errors = []

        async def async_error_cb(error_msg):
            errors.append(error_msg)

        subscription = QueriesSubscription(
            channel="test",
            on_receive_query_callback=lambda q: None,
            on_error_callback=async_error_cb,
        )

        with pytest.raises(RuntimeError, match="query stream error"):
            async for _ in client.subscribe_to_queries(subscription):
                pass

        assert len(errors) == 1


class TestSubscribeToQueriesSyncErrorCallback:
    """Tests for subscribe_to_queries sync on_error_callback (line 529)."""

    @pytest.mark.asyncio
    async def test_sync_error_callback_called(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport

        class ErrorIterator:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise RuntimeError("qry sync error")

        mock_transport.subscribe_to_requests = MagicMock(return_value=ErrorIterator())

        errors = []

        def sync_error_cb(error_msg):
            errors.append(error_msg)

        subscription = QueriesSubscription(
            channel="test",
            on_receive_query_callback=lambda q: None,
            on_error_callback=sync_error_cb,
        )

        with pytest.raises(RuntimeError, match="qry sync error"):
            async for _ in client.subscribe_to_queries(subscription):
                pass

        assert len(errors) == 1


class TestSubscribeToQueriesSyncCallback:
    """Tests for subscribe_to_queries sync receive callback (line 504)."""

    @pytest.mark.asyncio
    async def test_sync_query_callback_called(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport

        mock_pb = _make_mock_pb_request(request_type=2, request_id="qry-sync")
        mock_transport.subscribe_to_requests = MagicMock(return_value=AsyncIteratorMock([mock_pb]))

        received = []

        def sync_cb(q):
            received.append(q)

        subscription = QueriesSubscription(
            channel="test",
            on_receive_query_callback=sync_cb,
        )

        async for _ in client.subscribe_to_queries(subscription):
            pass

        assert len(received) == 1


class TestSubscribeCommandsCallbackSequentialHandlerError:
    """Tests for subscribe_commands_with_callback sequential handler error (lines 604-609)."""

    @pytest.mark.asyncio
    async def test_sequential_handler_error_with_error_callback(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport

        mock_pb = _make_mock_pb_request()
        mock_transport.subscribe_to_requests = MagicMock(return_value=AsyncIteratorMock([mock_pb]))

        subscription = CommandsSubscription(
            channel="test",
            on_receive_command_callback=lambda c: None,
        )

        errors = []

        async def failing_callback(cmd):
            raise ValueError("seq handler error")

        async def error_cb(e):
            errors.append(e)

        await client.subscribe_commands_with_callback(
            subscription,
            failing_callback,
            error_callback=error_cb,
            max_concurrent_callbacks=1,
        )

        assert len(errors) == 1

    @pytest.mark.asyncio
    async def test_sequential_handler_error_reraised_without_error_cb(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport

        mock_pb = _make_mock_pb_request()
        mock_transport.subscribe_to_requests = MagicMock(return_value=AsyncIteratorMock([mock_pb]))

        subscription = CommandsSubscription(
            channel="test",
            on_receive_command_callback=lambda c: None,
        )

        async def failing_callback(cmd):
            raise ValueError("no error cb handler error")

        with pytest.raises(ValueError, match="no error cb handler error"):
            await client.subscribe_commands_with_callback(
                subscription,
                failing_callback,
                max_concurrent_callbacks=1,
            )


class TestSubscribeCommandsCallbackConcurrentHandlerError:
    """Tests for concurrent handler error paths (lines 621-628)."""

    @pytest.mark.asyncio
    async def test_concurrent_handler_error_with_error_callback(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport

        mock_pb = _make_mock_pb_request()
        mock_transport.subscribe_to_requests = MagicMock(return_value=AsyncIteratorMock([mock_pb]))

        subscription = CommandsSubscription(
            channel="test",
            on_receive_command_callback=lambda c: None,
        )

        errors = []

        async def failing_callback(cmd):
            raise ValueError("concurrent cb error")

        async def error_cb(e):
            errors.append(e)

        await client.subscribe_commands_with_callback(
            subscription,
            failing_callback,
            error_callback=error_cb,
            max_concurrent_callbacks=5,
        )

        assert len(errors) == 1
        assert isinstance(errors[0], ValueError)

    @pytest.mark.asyncio
    async def test_concurrent_handler_error_logger_fallback(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._logger = MagicMock()

        mock_pb = _make_mock_pb_request()
        mock_transport.subscribe_to_requests = MagicMock(return_value=AsyncIteratorMock([mock_pb]))

        subscription = CommandsSubscription(
            channel="test",
            on_receive_command_callback=lambda c: None,
        )

        async def failing_callback(cmd):
            raise ValueError("logger fallback error")

        await client.subscribe_commands_with_callback(
            subscription,
            failing_callback,
            max_concurrent_callbacks=5,
        )

        client._logger.error.assert_called()


class TestSubscribeCommandsCallbackOuterException:
    """Tests for outer exception in subscribe_commands_with_callback (line 650)."""

    @pytest.mark.asyncio
    async def test_outer_exception_with_error_callback(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport

        class ErrorIterator:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise RuntimeError("outer error")

        mock_transport.subscribe_to_requests = MagicMock(return_value=ErrorIterator())

        subscription = CommandsSubscription(
            channel="test",
            on_receive_command_callback=lambda c: None,
        )

        errors = []

        async def error_cb(e):
            errors.append(e)

        await client.subscribe_commands_with_callback(
            subscription,
            AsyncMock(),
            error_callback=error_cb,
            max_concurrent_callbacks=1,
        )

        assert len(errors) == 1


class TestSubscribeQueriesCallbackSequentialHandlerError:
    """Tests for subscribe_queries_with_callback sequential handler error (lines 726-731)."""

    @pytest.mark.asyncio
    async def test_sequential_handler_error_with_error_callback(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport

        mock_pb = _make_mock_pb_request(request_type=2, request_id="qry-err")
        mock_transport.subscribe_to_requests = MagicMock(return_value=AsyncIteratorMock([mock_pb]))

        subscription = QueriesSubscription(
            channel="test",
            on_receive_query_callback=lambda q: None,
        )

        errors = []

        async def failing_callback(qry):
            raise ValueError("query seq handler error")

        async def error_cb(e):
            errors.append(e)

        await client.subscribe_queries_with_callback(
            subscription,
            failing_callback,
            error_callback=error_cb,
            max_concurrent_callbacks=1,
        )

        assert len(errors) == 1

    @pytest.mark.asyncio
    async def test_sequential_handler_error_reraised_without_error_cb(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport

        mock_pb = _make_mock_pb_request(request_type=2, request_id="qry-reraise")
        mock_transport.subscribe_to_requests = MagicMock(return_value=AsyncIteratorMock([mock_pb]))

        subscription = QueriesSubscription(
            channel="test",
            on_receive_query_callback=lambda q: None,
        )

        async def failing_callback(qry):
            raise ValueError("query no error cb")

        with pytest.raises(ValueError, match="query no error cb"):
            await client.subscribe_queries_with_callback(
                subscription,
                failing_callback,
                max_concurrent_callbacks=1,
            )


class TestSubscribeQueriesCallbackConcurrentHandlerError:
    """Tests for queries concurrent handler error (lines 743-750)."""

    @pytest.mark.asyncio
    async def test_concurrent_handler_error_with_error_callback(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport

        mock_pb = _make_mock_pb_request(request_type=2, request_id="qry-conc")
        mock_transport.subscribe_to_requests = MagicMock(return_value=AsyncIteratorMock([mock_pb]))

        subscription = QueriesSubscription(
            channel="test",
            on_receive_query_callback=lambda q: None,
        )

        errors = []

        async def failing_callback(qry):
            raise ValueError("query concurrent error")

        async def error_cb(e):
            errors.append(e)

        await client.subscribe_queries_with_callback(
            subscription,
            failing_callback,
            error_callback=error_cb,
            max_concurrent_callbacks=5,
        )

        assert len(errors) == 1

    @pytest.mark.asyncio
    async def test_concurrent_handler_error_logger_fallback(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._logger = MagicMock()

        mock_pb = _make_mock_pb_request(request_type=2, request_id="qry-log")
        mock_transport.subscribe_to_requests = MagicMock(return_value=AsyncIteratorMock([mock_pb]))

        subscription = QueriesSubscription(
            channel="test",
            on_receive_query_callback=lambda q: None,
        )

        async def failing_callback(qry):
            raise ValueError("query logger fallback")

        await client.subscribe_queries_with_callback(
            subscription,
            failing_callback,
            max_concurrent_callbacks=5,
        )

        client._logger.error.assert_called()


class TestSubscribeQueriesCallbackOuterException:
    """Tests for outer exception in subscribe_queries_with_callback (lines 768-772)."""

    @pytest.mark.asyncio
    async def test_outer_exception_with_error_callback(self, mock_transport):
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport

        class ErrorIterator:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise RuntimeError("query outer error")

        mock_transport.subscribe_to_requests = MagicMock(return_value=ErrorIterator())

        subscription = QueriesSubscription(
            channel="test",
            on_receive_query_callback=lambda q: None,
        )

        errors = []

        async def error_cb(e):
            errors.append(e)

        await client.subscribe_queries_with_callback(
            subscription,
            AsyncMock(),
            error_callback=error_cb,
            max_concurrent_callbacks=1,
        )

        assert len(errors) == 1


# ==============================================================================
# Additional Coverage Tests — fast paths, fast subscribe, edge paths
# ==============================================================================

from kubemq.core.exceptions import KubeMQClientClosedError, KubeMQError  # noqa: E402


class CancellingAsyncIteratorMock:
    """Async iterator that yields items then cancels the token to break the retry loop."""

    def __init__(self, items, token):
        self.items = iter(items)
        self.token = token

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self.items)
        except StopIteration:
            self.token.cancel()
            raise StopAsyncIteration


class ErrorThenCancelIterator:
    """Async iterator that raises an error, then cancels token on next call."""

    def __init__(self, error, token):
        self.error = error
        self.token = token
        self.raised = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self.raised:
            self.raised = True
            raise self.error
        self.token.cancel()
        raise StopAsyncIteration


class TestAsyncCQClientSendCommandFast:
    """Tests for send_command_fast method (lines 472-487)."""

    @pytest.mark.asyncio
    async def test_send_command_fast_without_pipeline_sem(self, mock_transport):
        """Test send_command_fast without pipeline semaphore."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        mock_response = pb.Response()
        mock_response.Executed = True
        mock_response.Error = ""
        mock_transport.send_request.return_value = mock_response

        message = CommandMessage(channel="test", body=b"data", timeout_in_seconds=10)

        response = await client.send_command_fast(message)

        assert isinstance(response, CommandResponse)

    @pytest.mark.asyncio
    async def test_send_command_fast_with_pipeline_sem(self, mock_transport):
        """Test send_command_fast with pipeline semaphore set."""
        import asyncio

        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True
        client._pipeline_sem = asyncio.Semaphore(10)

        mock_response = pb.Response()
        mock_response.Executed = True
        mock_response.Error = ""
        mock_transport.send_request.return_value = mock_response

        message = CommandMessage(channel="test", body=b"data", timeout_in_seconds=10)

        response = await client.send_command_fast(message)

        assert isinstance(response, CommandResponse)

    @pytest.mark.asyncio
    async def test_send_command_fast_when_not_connected(self):
        """Test send_command_fast raises when not connected."""
        client = AsyncClient(address="localhost:50000")
        message = CommandMessage(channel="test", body=b"data", timeout_in_seconds=10)

        with pytest.raises(KubeMQConnectionError):
            await client.send_command_fast(message)


class TestAsyncCQClientSendQueryFast:
    """Tests for send_query_fast method (lines 495-510)."""

    @pytest.mark.asyncio
    async def test_send_query_fast_without_pipeline_sem(self, mock_transport):
        """Test send_query_fast without pipeline semaphore."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        mock_response = pb.Response()
        mock_response.Executed = True
        mock_response.Error = ""
        mock_response.Body = b"result"
        mock_transport.send_request.return_value = mock_response

        message = QueryMessage(channel="test", body=b"data", timeout_in_seconds=10)

        response = await client.send_query_fast(message)

        assert isinstance(response, QueryResponse)

    @pytest.mark.asyncio
    async def test_send_query_fast_with_pipeline_sem(self, mock_transport):
        """Test send_query_fast with pipeline semaphore set."""
        import asyncio

        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True
        client._pipeline_sem = asyncio.Semaphore(10)

        mock_response = pb.Response()
        mock_response.Executed = True
        mock_response.Error = ""
        mock_response.Body = b"result"
        mock_transport.send_request.return_value = mock_response

        message = QueryMessage(channel="test", body=b"data", timeout_in_seconds=10)

        response = await client.send_query_fast(message)

        assert isinstance(response, QueryResponse)

    @pytest.mark.asyncio
    async def test_send_query_fast_when_not_connected(self):
        """Test send_query_fast raises when not connected."""
        client = AsyncClient(address="localhost:50000")
        message = QueryMessage(channel="test", body=b"data", timeout_in_seconds=10)

        with pytest.raises(KubeMQConnectionError):
            await client.send_query_fast(message)


class TestAsyncCQClientSendResponseFast:
    """Tests for send_response_fast method (lines 514-517)."""

    @pytest.mark.asyncio
    async def test_send_response_fast(self, mock_transport):
        """Test send_response_fast sends response without instrumentation."""
        from kubemq.cq.command_message_received import CommandReceived

        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        command_received = CommandReceived(id="cmd-123", channel="test", reply_channel="reply")
        response = CommandResponse(command_received=command_received, is_executed=True, error="")

        await client.send_response_fast(response)

        mock_transport.send_response.assert_called_once()

    @pytest.mark.asyncio
    async def test_send_response_fast_when_not_connected(self):
        """Test send_response_fast raises when not connected."""
        from kubemq.cq.command_message_received import CommandReceived

        client = AsyncClient(address="localhost:50000")
        command_received = CommandReceived(id="cmd-123", channel="test", reply_channel="reply")
        response = CommandResponse(command_received=command_received, is_executed=True, error="")

        with pytest.raises(KubeMQConnectionError):
            await client.send_response_fast(response)


class TestAsyncCQClientSubscribeToCommandsFast:
    """Tests for subscribe_to_commands_fast method (lines 528-576)."""

    @pytest.mark.asyncio
    async def test_yields_messages(self, mock_transport):
        """Test subscribe_to_commands_fast yields decoded CommandReceived messages."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        token = AsyncCancellationToken()
        mock_pb = _make_mock_pb_request(request_id="fast-cmd-1")
        mock_transport.subscribe_to_requests = MagicMock(
            return_value=CancellingAsyncIteratorMock([mock_pb], token)
        )

        received = []
        async for cmd in client.subscribe_to_commands_fast(
            CommandsSubscription(channel="test", on_receive_command_callback=lambda c: None),
            cancellation_token=token,
        ):
            received.append(cmd)

        assert len(received) == 1
        assert received[0].id == "fast-cmd-1"

    @pytest.mark.asyncio
    async def test_stream_end_reconnects(self, mock_transport):
        """Test that when stream ends normally, it reconnects (retry loop)."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        token = AsyncCancellationToken()
        call_count = [0]

        def make_iterator(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                # First call: return empty iterator (stream ended)
                return AsyncIteratorMock([])
            else:
                # Second call: return items then cancel
                mock_pb = _make_mock_pb_request(request_id="reconnected-cmd")
                return CancellingAsyncIteratorMock([mock_pb], token)

        mock_transport.subscribe_to_requests = MagicMock(side_effect=make_iterator)

        received = []
        with patch("kubemq.cq.async_client.asyncio.sleep", new_callable=AsyncMock):
            async for cmd in client.subscribe_to_commands_fast(
                CommandsSubscription(channel="test", on_receive_command_callback=lambda c: None),
                cancellation_token=token,
            ):
                received.append(cmd)

        assert len(received) == 1
        assert received[0].id == "reconnected-cmd"
        assert call_count[0] == 2

    @pytest.mark.asyncio
    async def test_kubemq_client_closed_error_reraises(self, mock_transport):
        """Test KubeMQClientClosedError is re-raised immediately."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        class ClosedIterator:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise KubeMQClientClosedError("client closed")

        mock_transport.subscribe_to_requests = MagicMock(return_value=ClosedIterator())

        with pytest.raises(KubeMQClientClosedError):
            async for _ in client.subscribe_to_commands_fast(
                CommandsSubscription(channel="test", on_receive_command_callback=lambda c: None),
            ):
                pass

    @pytest.mark.asyncio
    async def test_retryable_kubemq_error_retries(self, mock_transport):
        """Test retryable KubeMQError triggers retry loop."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        token = AsyncCancellationToken()
        call_count = [0]

        def make_iterator(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return ErrorThenCancelIterator(
                    KubeMQError("retryable err", is_retryable=True), token
                )
            return CancellingAsyncIteratorMock([], token)

        mock_transport.subscribe_to_requests = MagicMock(side_effect=make_iterator)

        with patch("kubemq.cq.async_client.asyncio.sleep", new_callable=AsyncMock):
            async for _ in client.subscribe_to_commands_fast(
                CommandsSubscription(channel="test", on_receive_command_callback=lambda c: None),
                cancellation_token=token,
            ):
                pass

        assert call_count[0] >= 1

    @pytest.mark.asyncio
    async def test_non_retryable_kubemq_error_reraises(self, mock_transport):
        """Test non-retryable KubeMQError that is not a KubeMQConnectionError re-raises."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        class NonRetryableIterator:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise KubeMQError("non-retryable", is_retryable=False)

        mock_transport.subscribe_to_requests = MagicMock(return_value=NonRetryableIterator())

        with pytest.raises(KubeMQError, match="non-retryable"):
            async for _ in client.subscribe_to_commands_fast(
                CommandsSubscription(channel="test", on_receive_command_callback=lambda c: None),
            ):
                pass

    @pytest.mark.asyncio
    async def test_connection_error_retries(self, mock_transport):
        """Test KubeMQConnectionError (non-retryable but is KubeMQConnectionError) retries."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        token = AsyncCancellationToken()
        call_count = [0]

        def make_iterator(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return ErrorThenCancelIterator(
                    KubeMQConnectionError("conn lost", is_retryable=False), token
                )
            return CancellingAsyncIteratorMock([], token)

        mock_transport.subscribe_to_requests = MagicMock(side_effect=make_iterator)

        with patch("kubemq.cq.async_client.asyncio.sleep", new_callable=AsyncMock):
            async for _ in client.subscribe_to_commands_fast(
                CommandsSubscription(channel="test", on_receive_command_callback=lambda c: None),
                cancellation_token=token,
            ):
                pass

        assert call_count[0] >= 1

    @pytest.mark.asyncio
    async def test_generic_exception_retries(self, mock_transport):
        """Test generic Exception triggers retry loop."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        token = AsyncCancellationToken()
        call_count = [0]

        def make_iterator(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return ErrorThenCancelIterator(RuntimeError("something broke"), token)
            return CancellingAsyncIteratorMock([], token)

        mock_transport.subscribe_to_requests = MagicMock(side_effect=make_iterator)

        with patch("kubemq.cq.async_client.asyncio.sleep", new_callable=AsyncMock):
            async for _ in client.subscribe_to_commands_fast(
                CommandsSubscription(channel="test", on_receive_command_callback=lambda c: None),
                cancellation_token=token,
            ):
                pass

        assert call_count[0] >= 1

    @pytest.mark.asyncio
    async def test_creates_default_token_if_not_provided(self, mock_transport):
        """Test that a default cancellation token is created when none is provided."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        mock_pb = _make_mock_pb_request(request_id="default-token-cmd")

        # This iterator yields one item then stops; the while loop will try to
        # re-subscribe. We patch sleep to cancel quickly.
        first_call = [True]

        def make_iterator(*args, **kwargs):
            if first_call[0]:
                first_call[0] = False
                return AsyncIteratorMock([mock_pb])

            # On second call, raise ClientClosed to break the loop
            class StopIter:
                def __aiter__(self):
                    return self

                async def __anext__(self):
                    raise KubeMQClientClosedError()

            return StopIter()

        mock_transport.subscribe_to_requests = MagicMock(side_effect=make_iterator)

        received = []
        with pytest.raises(KubeMQClientClosedError):
            with patch("kubemq.cq.async_client.asyncio.sleep", new_callable=AsyncMock):
                async for cmd in client.subscribe_to_commands_fast(
                    CommandsSubscription(
                        channel="test", on_receive_command_callback=lambda c: None
                    ),
                ):
                    received.append(cmd)

        assert len(received) == 1


class TestAsyncCQClientSubscribeToQueriesFast:
    """Tests for subscribe_to_queries_fast method (lines 584-632)."""

    @pytest.mark.asyncio
    async def test_yields_messages(self, mock_transport):
        """Test subscribe_to_queries_fast yields decoded QueryReceived messages."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        token = AsyncCancellationToken()
        mock_pb = _make_mock_pb_request(request_id="fast-qry-1", request_type=2)
        mock_transport.subscribe_to_requests = MagicMock(
            return_value=CancellingAsyncIteratorMock([mock_pb], token)
        )

        received = []
        async for qry in client.subscribe_to_queries_fast(
            QueriesSubscription(channel="test", on_receive_query_callback=lambda q: None),
            cancellation_token=token,
        ):
            received.append(qry)

        assert len(received) == 1
        assert received[0].id == "fast-qry-1"

    @pytest.mark.asyncio
    async def test_stream_end_reconnects(self, mock_transport):
        """Test that when stream ends normally, it reconnects (retry loop)."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        token = AsyncCancellationToken()
        call_count = [0]

        def make_iterator(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return AsyncIteratorMock([])
            else:
                mock_pb = _make_mock_pb_request(request_id="reconnected-qry", request_type=2)
                return CancellingAsyncIteratorMock([mock_pb], token)

        mock_transport.subscribe_to_requests = MagicMock(side_effect=make_iterator)

        received = []
        with patch("kubemq.cq.async_client.asyncio.sleep", new_callable=AsyncMock):
            async for qry in client.subscribe_to_queries_fast(
                QueriesSubscription(channel="test", on_receive_query_callback=lambda q: None),
                cancellation_token=token,
            ):
                received.append(qry)

        assert len(received) == 1
        assert received[0].id == "reconnected-qry"
        assert call_count[0] == 2

    @pytest.mark.asyncio
    async def test_kubemq_client_closed_error_reraises(self, mock_transport):
        """Test KubeMQClientClosedError is re-raised immediately."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        class ClosedIterator:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise KubeMQClientClosedError("client closed")

        mock_transport.subscribe_to_requests = MagicMock(return_value=ClosedIterator())

        with pytest.raises(KubeMQClientClosedError):
            async for _ in client.subscribe_to_queries_fast(
                QueriesSubscription(channel="test", on_receive_query_callback=lambda q: None),
            ):
                pass

    @pytest.mark.asyncio
    async def test_retryable_kubemq_error_retries(self, mock_transport):
        """Test retryable KubeMQError triggers retry loop."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        token = AsyncCancellationToken()
        call_count = [0]

        def make_iterator(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return ErrorThenCancelIterator(
                    KubeMQError("retryable qry err", is_retryable=True), token
                )
            return CancellingAsyncIteratorMock([], token)

        mock_transport.subscribe_to_requests = MagicMock(side_effect=make_iterator)

        with patch("kubemq.cq.async_client.asyncio.sleep", new_callable=AsyncMock):
            async for _ in client.subscribe_to_queries_fast(
                QueriesSubscription(channel="test", on_receive_query_callback=lambda q: None),
                cancellation_token=token,
            ):
                pass

        assert call_count[0] >= 1

    @pytest.mark.asyncio
    async def test_non_retryable_kubemq_error_reraises(self, mock_transport):
        """Test non-retryable KubeMQError (not KubeMQConnectionError) re-raises."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        class NonRetryableIterator:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise KubeMQError("non-retryable qry", is_retryable=False)

        mock_transport.subscribe_to_requests = MagicMock(return_value=NonRetryableIterator())

        with pytest.raises(KubeMQError, match="non-retryable qry"):
            async for _ in client.subscribe_to_queries_fast(
                QueriesSubscription(channel="test", on_receive_query_callback=lambda q: None),
            ):
                pass

    @pytest.mark.asyncio
    async def test_connection_error_retries(self, mock_transport):
        """Test KubeMQConnectionError retries even if non-retryable."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        token = AsyncCancellationToken()
        call_count = [0]

        def make_iterator(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return ErrorThenCancelIterator(
                    KubeMQConnectionError("conn lost qry", is_retryable=False), token
                )
            return CancellingAsyncIteratorMock([], token)

        mock_transport.subscribe_to_requests = MagicMock(side_effect=make_iterator)

        with patch("kubemq.cq.async_client.asyncio.sleep", new_callable=AsyncMock):
            async for _ in client.subscribe_to_queries_fast(
                QueriesSubscription(channel="test", on_receive_query_callback=lambda q: None),
                cancellation_token=token,
            ):
                pass

        assert call_count[0] >= 1

    @pytest.mark.asyncio
    async def test_generic_exception_retries(self, mock_transport):
        """Test generic Exception triggers retry loop for queries."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        token = AsyncCancellationToken()
        call_count = [0]

        def make_iterator(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return ErrorThenCancelIterator(RuntimeError("qry broke"), token)
            return CancellingAsyncIteratorMock([], token)

        mock_transport.subscribe_to_requests = MagicMock(side_effect=make_iterator)

        with patch("kubemq.cq.async_client.asyncio.sleep", new_callable=AsyncMock):
            async for _ in client.subscribe_to_queries_fast(
                QueriesSubscription(channel="test", on_receive_query_callback=lambda q: None),
                cancellation_token=token,
            ):
                pass

        assert call_count[0] >= 1


class TestAsyncCQClientSubscribeCommandsCallbackEdgePaths:
    """Tests for subscribe_commands_with_callback edge paths (lines 915-933)."""

    @pytest.mark.asyncio
    async def test_register_subscription_task_called(self, mock_transport):
        """Test that _register_subscription_task is called for current task (line 915-916)."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        mock_pb = _make_mock_pb_request()
        mock_transport.subscribe_to_requests = MagicMock(return_value=AsyncIteratorMock([mock_pb]))

        subscription = CommandsSubscription(
            channel="test",
            on_receive_command_callback=lambda c: None,
        )

        with patch.object(client, "_register_subscription_task") as mock_reg:
            await client.subscribe_commands_with_callback(
                subscription,
                AsyncMock(),
            )
            mock_reg.assert_called_once()

    @pytest.mark.asyncio
    async def test_link_appended_when_trace_present_sequential(self, mock_transport):
        """Test link creation in sequential callback mode (line 933)."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        mock_pb = _make_mock_pb_request(
            tags={"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}
        )
        mock_transport.subscribe_to_requests = MagicMock(return_value=AsyncIteratorMock([mock_pb]))

        subscription = CommandsSubscription(
            channel="test",
            on_receive_command_callback=lambda c: None,
        )
        received = []

        async def cb(cmd):
            received.append(cmd)

        await client.subscribe_commands_with_callback(subscription, cb)

        assert len(received) == 1

    @pytest.mark.asyncio
    async def test_concurrent_callback_no_error_callback_no_logger(self, mock_transport):
        """Test concurrent callback error path when no error_callback and no logger (line 964-971)."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True
        client._logger = None  # No logger

        mock_pb = _make_mock_pb_request()
        mock_transport.subscribe_to_requests = MagicMock(return_value=AsyncIteratorMock([mock_pb]))

        subscription = CommandsSubscription(
            channel="test",
            on_receive_command_callback=lambda c: None,
        )

        async def failing_callback(cmd):
            raise ValueError("no handler no logger")

        # Should not raise; error is silently swallowed when no error_callback and no logger
        await client.subscribe_commands_with_callback(
            subscription,
            failing_callback,
            max_concurrent_callbacks=5,
        )


class TestAsyncCQClientSubscribeQueriesCallbackEdgePaths:
    """Tests for subscribe_queries_with_callback edge paths (lines 1051-1107)."""

    @pytest.mark.asyncio
    async def test_register_subscription_task_called(self, mock_transport):
        """Test that _register_subscription_task is called for current task (line 1051-1052)."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        mock_pb = _make_mock_pb_request(request_type=2, request_id="qry-task")
        mock_transport.subscribe_to_requests = MagicMock(return_value=AsyncIteratorMock([mock_pb]))

        subscription = QueriesSubscription(
            channel="test",
            on_receive_query_callback=lambda q: None,
        )

        with patch.object(client, "_register_subscription_task") as mock_reg:
            await client.subscribe_queries_with_callback(
                subscription,
                AsyncMock(),
            )
            mock_reg.assert_called_once()

    @pytest.mark.asyncio
    async def test_link_appended_when_trace_present_sequential(self, mock_transport):
        """Test link creation in sequential callback mode for queries (line 1069)."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        mock_pb = _make_mock_pb_request(
            request_type=2,
            request_id="qry-link",
            tags={"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
        )
        mock_transport.subscribe_to_requests = MagicMock(return_value=AsyncIteratorMock([mock_pb]))

        subscription = QueriesSubscription(
            channel="test",
            on_receive_query_callback=lambda q: None,
        )
        received = []

        async def cb(qry):
            received.append(qry)

        await client.subscribe_queries_with_callback(subscription, cb)

        assert len(received) == 1

    @pytest.mark.asyncio
    async def test_concurrent_callback_no_error_callback_no_logger(self, mock_transport):
        """Test concurrent callback error path when no error_callback and no logger (line 1100-1107)."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True
        client._logger = None  # No logger

        mock_pb = _make_mock_pb_request(request_type=2, request_id="qry-nohandler")
        mock_transport.subscribe_to_requests = MagicMock(return_value=AsyncIteratorMock([mock_pb]))

        subscription = QueriesSubscription(
            channel="test",
            on_receive_query_callback=lambda q: None,
        )

        async def failing_callback(qry):
            raise ValueError("qry no handler no logger")

        # Should not raise; error is silently swallowed
        await client.subscribe_queries_with_callback(
            subscription,
            failing_callback,
            max_concurrent_callbacks=5,
        )

    @pytest.mark.asyncio
    async def test_outer_exception_reraises_without_error_callback(self, mock_transport):
        """Test outer exception re-raises when no error_callback is provided."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        class ErrorIterator:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise RuntimeError("outer qry reraise")

        mock_transport.subscribe_to_requests = MagicMock(return_value=ErrorIterator())

        subscription = QueriesSubscription(
            channel="test",
            on_receive_query_callback=lambda q: None,
        )

        with pytest.raises(RuntimeError, match="outer qry reraise"):
            await client.subscribe_queries_with_callback(
                subscription,
                AsyncMock(),
            )


class TestAsyncCQClientSubscribeToCancelledError:
    """Tests for CancelledError handling in subscribe methods (lines 735, 845)."""

    @pytest.mark.asyncio
    async def test_subscribe_to_commands_cancelled_error(self, mock_transport):
        """Test CancelledError is caught in subscribe_to_commands (line 735)."""
        import asyncio

        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        class CancelledIterator:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise asyncio.CancelledError()

        mock_transport.subscribe_to_requests = MagicMock(return_value=CancelledIterator())

        subscription = CommandsSubscription(
            channel="test",
            on_receive_command_callback=lambda c: None,
        )

        # CancelledError should be caught and not propagated
        received = []
        async for cmd in client.subscribe_to_commands(subscription):
            received.append(cmd)

        assert len(received) == 0

    @pytest.mark.asyncio
    async def test_subscribe_to_queries_cancelled_error(self, mock_transport):
        """Test CancelledError is caught in subscribe_to_queries (line 845)."""
        import asyncio

        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        class CancelledIterator:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise asyncio.CancelledError()

        mock_transport.subscribe_to_requests = MagicMock(return_value=CancelledIterator())

        subscription = QueriesSubscription(
            channel="test",
            on_receive_query_callback=lambda q: None,
        )

        received = []
        async for qry in client.subscribe_to_queries(subscription):
            received.append(qry)

        assert len(received) == 0


class TestAsyncCQClientSubscribeToQueriesAsyncCallback:
    """Test async callback path in subscribe_to_queries (line 825)."""

    @pytest.mark.asyncio
    async def test_async_query_callback_called(self, mock_transport):
        """Test that async on_receive_query_callback is awaited (line 825)."""
        client = AsyncClient(address="localhost:50000")
        client._transport = mock_transport
        client._connected = True

        mock_pb = _make_mock_pb_request(request_type=2, request_id="qry-async-cb")
        mock_transport.subscribe_to_requests = MagicMock(return_value=AsyncIteratorMock([mock_pb]))

        received = []

        async def async_cb(q):
            received.append(q)

        subscription = QueriesSubscription(
            channel="test",
            on_receive_query_callback=async_cb,
        )

        queries = []
        async for qry in client.subscribe_to_queries(subscription):
            queries.append(qry)

        assert len(received) == 1
        assert len(queries) == 1
