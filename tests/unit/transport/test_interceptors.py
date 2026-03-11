"""Unit tests for kubemq.transport.interceptors module.

Tests for authentication interceptors (sync and async).
"""

from __future__ import annotations

from collections import namedtuple
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kubemq._internal.auth import TokenHolder
from kubemq.transport.interceptors import (
    AsyncStreamStreamAuthInterceptor,
    AsyncStreamUnaryAuthInterceptor,
    AsyncUnaryStreamAuthInterceptor,
    AsyncUnaryUnaryAuthInterceptor,
    AuthInterceptors,
    AuthInterceptorsAsync,
    _inject_auth_metadata,
)


class TestInjectAuthMetadata:
    """Tests for _inject_auth_metadata helper function."""

    def test_injects_auth_token_to_empty_metadata(self):
        """Test injecting auth token when metadata is None."""
        holder = TokenHolder("my-token")
        result = _inject_auth_metadata(None, holder)

        assert result == (("authorization", "my-token"),)

    def test_injects_auth_token_to_existing_metadata(self):
        """Test injecting auth token with existing metadata."""
        existing = (("key1", "value1"), ("key2", "value2"))
        holder = TokenHolder("my-token")
        result = _inject_auth_metadata(existing, holder)

        assert len(result) == 3
        assert ("key1", "value1") in result
        assert ("key2", "value2") in result
        assert ("authorization", "my-token") in result

    def test_returns_empty_tuple_when_no_token_and_no_metadata(self):
        """Test returns empty tuple when no token and no metadata."""
        holder = TokenHolder(None)
        result = _inject_auth_metadata(None, holder)

        assert result == ()

    def test_returns_existing_metadata_when_no_token(self):
        """Test returns existing metadata unchanged when no token."""
        existing = (("key1", "value1"),)
        holder = TokenHolder(None)
        result = _inject_auth_metadata(existing, holder)

        assert result == existing

    def test_returns_existing_metadata_when_empty_token(self):
        """Test returns existing metadata when token is empty string."""
        existing = (("key1", "value1"),)
        holder = TokenHolder("")
        result = _inject_auth_metadata(existing, holder)

        assert result == existing

    def test_returns_existing_metadata_when_whitespace_token(self):
        """Test returns existing metadata when token is whitespace."""
        existing = (("key1", "value1"),)
        holder = TokenHolder("   ")
        result = _inject_auth_metadata(existing, holder)

        assert result == existing


class TestAuthInterceptors:
    """Tests for AuthInterceptors sync class."""

    def test_init_stores_token_holder(self):
        """Test initialization stores token holder."""
        holder = TokenHolder("test-token")
        interceptor = AuthInterceptors(holder)

        assert interceptor._token_holder.token == "test-token"

    def test_intercept_call_adds_auth_to_metadata(self):
        """Test _intercept_call adds authorization to metadata."""
        holder = TokenHolder("test-token")
        interceptor = AuthInterceptors(holder)

        ClientCallDetails = namedtuple(
            "ClientCallDetails", ["method", "timeout", "metadata", "credentials"]
        )
        details = ClientCallDetails(
            method="/test/Method",
            timeout=None,
            metadata=None,
            credentials=None,
        )

        mock_continuation = MagicMock(return_value="response")
        request = MagicMock()

        result = interceptor._intercept_call(mock_continuation, details, request)  # type: ignore[arg-type]

        assert result == "response"
        mock_continuation.assert_called_once()
        call_args = mock_continuation.call_args[0]
        new_details = call_args[0]
        assert ("authorization", "test-token") in new_details.metadata

    def test_intercept_call_preserves_existing_metadata(self):
        """Test _intercept_call preserves existing metadata."""
        holder = TokenHolder("test-token")
        interceptor = AuthInterceptors(holder)

        ClientCallDetails = namedtuple(
            "ClientCallDetails", ["method", "timeout", "metadata", "credentials"]
        )
        details = ClientCallDetails(
            method="/test/Method",
            timeout=None,
            metadata=[("existing", "value")],
            credentials=None,
        )

        mock_continuation = MagicMock(return_value="response")
        request = MagicMock()

        interceptor._intercept_call(mock_continuation, details, request)

        call_args = mock_continuation.call_args[0]
        new_details = call_args[0]
        assert ("existing", "value") in new_details.metadata
        assert ("authorization", "test-token") in new_details.metadata

    def test_intercept_call_skips_empty_token(self):
        """Test _intercept_call skips empty token."""
        holder = TokenHolder("")
        interceptor = AuthInterceptors(holder)

        ClientCallDetails = namedtuple(
            "ClientCallDetails", ["method", "timeout", "metadata", "credentials"]
        )
        details = ClientCallDetails(
            method="/test/Method",
            timeout=None,
            metadata=None,
            credentials=None,
        )

        mock_continuation = MagicMock(return_value="response")
        request = MagicMock()

        interceptor._intercept_call(mock_continuation, details, request)

        call_args = mock_continuation.call_args[0]
        new_details = call_args[0]
        assert not any("authorization" in str(m) for m in new_details.metadata)

    def test_intercept_unary_unary(self):
        """Test intercept_unary_unary delegates to _intercept_call."""
        interceptor = AuthInterceptors(TokenHolder("token"))

        with patch.object(interceptor, "_intercept_call", return_value="result") as mock:
            result = interceptor.intercept_unary_unary("cont", "details", "request")

        assert result == "result"
        mock.assert_called_once_with("cont", "details", "request")

    def test_intercept_unary_stream(self):
        """Test intercept_unary_stream delegates to _intercept_call."""
        interceptor = AuthInterceptors(TokenHolder("token"))

        with patch.object(interceptor, "_intercept_call", return_value="result") as mock:
            result = interceptor.intercept_unary_stream("cont", "details", "request")

        assert result == "result"
        mock.assert_called_once_with("cont", "details", "request")

    def test_intercept_stream_unary(self):
        """Test intercept_stream_unary delegates to _intercept_call."""
        interceptor = AuthInterceptors(TokenHolder("token"))

        with patch.object(interceptor, "_intercept_call", return_value="result") as mock:
            result = interceptor.intercept_stream_unary("cont", "details", "request_iter")

        assert result == "result"
        mock.assert_called_once_with("cont", "details", "request_iter")

    def test_intercept_stream_stream(self):
        """Test intercept_stream_stream delegates to _intercept_call."""
        interceptor = AuthInterceptors(TokenHolder("token"))

        with patch.object(interceptor, "_intercept_call", return_value="result") as mock:
            result = interceptor.intercept_stream_stream("cont", "details", "request_iter")

        assert result == "result"
        mock.assert_called_once_with("cont", "details", "request_iter")


class TestAuthInterceptorsAsync:
    """Tests for AuthInterceptorsAsync class."""

    def test_init_stores_token_holder(self):
        """Test initialization stores token holder."""
        holder = TokenHolder("async-token")
        interceptor = AuthInterceptorsAsync(holder)

        assert interceptor._token_holder.token == "async-token"

    @pytest.mark.asyncio
    async def test_intercept_call_adds_auth_to_metadata(self):
        """Test _intercept_call adds authorization to metadata."""
        holder = TokenHolder("async-token")
        interceptor = AuthInterceptorsAsync(holder)

        ClientCallDetails = namedtuple(
            "ClientCallDetails", ["method", "timeout", "metadata", "credentials"]
        )
        details = ClientCallDetails(
            method="/test/Method",
            timeout=None,
            metadata=None,
            credentials=None,
        )

        mock_continuation = AsyncMock(return_value="async-response")
        request = MagicMock()

        result = await interceptor._intercept_call(mock_continuation, details, request)

        assert result == "async-response"
        mock_continuation.assert_called_once()
        call_args = mock_continuation.call_args[0]
        new_details = call_args[0]
        assert ("authorization", "async-token") in new_details.metadata

    @pytest.mark.asyncio
    async def test_intercept_unary_unary(self):
        """Test async intercept_unary_unary."""
        interceptor = AuthInterceptorsAsync(TokenHolder("token"))

        with patch.object(
            interceptor, "_intercept_call", new_callable=AsyncMock, return_value="result"
        ) as mock:
            result = await interceptor.intercept_unary_unary("cont", "details", "request")

        assert result == "result"
        mock.assert_called_once_with("cont", "details", "request")

    @pytest.mark.asyncio
    async def test_intercept_unary_stream(self):
        """Test async intercept_unary_stream."""
        interceptor = AuthInterceptorsAsync(TokenHolder("token"))

        with patch.object(
            interceptor, "_intercept_call", new_callable=AsyncMock, return_value="result"
        ) as mock:
            result = await interceptor.intercept_unary_stream("cont", "details", "request")

        assert result == "result"
        mock.assert_called_once_with("cont", "details", "request")

    @pytest.mark.asyncio
    async def test_intercept_stream_unary(self):
        """Test async intercept_stream_unary."""
        interceptor = AuthInterceptorsAsync(TokenHolder("token"))

        with patch.object(
            interceptor, "_intercept_call", new_callable=AsyncMock, return_value="result"
        ) as mock:
            result = await interceptor.intercept_stream_unary("cont", "details", "request_iter")

        assert result == "result"
        mock.assert_called_once_with("cont", "details", "request_iter")

    @pytest.mark.asyncio
    async def test_intercept_stream_stream(self):
        """Test async intercept_stream_stream."""
        interceptor = AuthInterceptorsAsync(TokenHolder("token"))

        with patch.object(
            interceptor, "_intercept_call", new_callable=AsyncMock, return_value="result"
        ) as mock:
            result = await interceptor.intercept_stream_stream("cont", "details", "request_iter")

        assert result == "result"
        mock.assert_called_once_with("cont", "details", "request_iter")


class TestAsyncUnaryUnaryAuthInterceptor:
    """Tests for AsyncUnaryUnaryAuthInterceptor."""

    def test_init_stores_token_holder(self):
        """Test initialization stores token holder."""
        holder = TokenHolder("unary-token")
        interceptor = AsyncUnaryUnaryAuthInterceptor(holder)

        assert interceptor._token_holder.token == "unary-token"

    @pytest.mark.asyncio
    async def test_intercept_unary_unary_injects_auth(self):
        """Test intercept_unary_unary injects authorization."""
        interceptor = AsyncUnaryUnaryAuthInterceptor(TokenHolder("inject-token"))

        mock_details = MagicMock()
        mock_details.method = "/test/Ping"
        mock_details.timeout = 30.0
        mock_details.metadata = None
        mock_details.credentials = None
        mock_details.wait_for_ready = False

        mock_continuation = AsyncMock(return_value="response")
        request = MagicMock()

        with patch("kubemq.transport.interceptors.grpc.aio.ClientCallDetails") as mock_ccd:
            mock_ccd.return_value = MagicMock()
            await interceptor.intercept_unary_unary(mock_continuation, mock_details, request)

        mock_continuation.assert_called_once()
        mock_ccd.assert_called_once()
        call_kwargs = mock_ccd.call_args
        # Check that auth token was injected
        metadata = call_kwargs[1]["metadata"]
        assert ("authorization", "inject-token") in metadata


class TestAsyncUnaryStreamAuthInterceptor:
    """Tests for AsyncUnaryStreamAuthInterceptor."""

    def test_init_stores_token_holder(self):
        """Test initialization stores token holder."""
        holder = TokenHolder("stream-token")
        interceptor = AsyncUnaryStreamAuthInterceptor(holder)

        assert interceptor._token_holder.token == "stream-token"

    @pytest.mark.asyncio
    async def test_intercept_unary_stream_injects_auth(self):
        """Test intercept_unary_stream injects authorization."""
        interceptor = AsyncUnaryStreamAuthInterceptor(TokenHolder("stream-token"))

        mock_details = MagicMock()
        mock_details.method = "/test/Subscribe"
        mock_details.timeout = None
        mock_details.metadata = (("existing", "value"),)
        mock_details.credentials = None
        mock_details.wait_for_ready = True

        mock_continuation = AsyncMock(return_value="stream")
        request = MagicMock()

        with patch("kubemq.transport.interceptors.grpc.aio.ClientCallDetails") as mock_ccd:
            mock_ccd.return_value = MagicMock()
            await interceptor.intercept_unary_stream(mock_continuation, mock_details, request)

        mock_continuation.assert_called_once()
        call_kwargs = mock_ccd.call_args
        metadata = call_kwargs[1]["metadata"]
        assert ("existing", "value") in metadata
        assert ("authorization", "stream-token") in metadata


class TestAsyncStreamUnaryAuthInterceptor:
    """Tests for AsyncStreamUnaryAuthInterceptor."""

    def test_init_stores_token_holder(self):
        """Test initialization stores token holder."""
        holder = TokenHolder("batch-token")
        interceptor = AsyncStreamUnaryAuthInterceptor(holder)

        assert interceptor._token_holder.token == "batch-token"

    @pytest.mark.asyncio
    async def test_intercept_stream_unary_injects_auth(self):
        """Test intercept_stream_unary injects authorization."""
        interceptor = AsyncStreamUnaryAuthInterceptor(TokenHolder("batch-token"))

        mock_details = MagicMock()
        mock_details.method = "/test/SendBatch"
        mock_details.timeout = 60.0
        mock_details.metadata = None
        mock_details.credentials = None
        mock_details.wait_for_ready = False

        mock_continuation = AsyncMock(return_value="batch-result")

        async def request_iter():
            yield "item1"
            yield "item2"

        with patch("kubemq.transport.interceptors.grpc.aio.ClientCallDetails") as mock_ccd:
            mock_ccd.return_value = MagicMock()
            await interceptor.intercept_stream_unary(
                mock_continuation, mock_details, request_iter()
            )

        mock_continuation.assert_called_once()
        call_kwargs = mock_ccd.call_args
        metadata = call_kwargs[1]["metadata"]
        assert ("authorization", "batch-token") in metadata


class TestAsyncStreamStreamAuthInterceptor:
    """Tests for AsyncStreamStreamAuthInterceptor."""

    def test_init_stores_token_holder(self):
        """Test initialization stores token holder."""
        holder = TokenHolder("bidi-token")
        interceptor = AsyncStreamStreamAuthInterceptor(holder)

        assert interceptor._token_holder.token == "bidi-token"

    @pytest.mark.asyncio
    async def test_intercept_stream_stream_injects_auth(self):
        """Test intercept_stream_stream injects authorization."""
        interceptor = AsyncStreamStreamAuthInterceptor(TokenHolder("bidi-token"))

        mock_details = MagicMock()
        mock_details.method = "/test/QueueStream"
        mock_details.timeout = None
        mock_details.metadata = None
        mock_details.credentials = None
        mock_details.wait_for_ready = True

        mock_continuation = AsyncMock(return_value="bidi-stream")

        async def request_iter():
            yield "request1"

        with patch("kubemq.transport.interceptors.grpc.aio.ClientCallDetails") as mock_ccd:
            mock_ccd.return_value = MagicMock()
            await interceptor.intercept_stream_stream(
                mock_continuation, mock_details, request_iter()
            )

        mock_continuation.assert_called_once()
        call_kwargs = mock_ccd.call_args
        metadata = call_kwargs[1]["metadata"]
        assert ("authorization", "bidi-token") in metadata

    @pytest.mark.asyncio
    async def test_intercept_stream_stream_with_no_token(self):
        """Test intercept_stream_stream with no auth token."""
        interceptor = AsyncStreamStreamAuthInterceptor(TokenHolder(None))

        mock_details = MagicMock()
        mock_details.method = "/test/QueueStream"
        mock_details.timeout = None
        mock_details.metadata = (("key", "val"),)
        mock_details.credentials = None
        mock_details.wait_for_ready = False

        mock_continuation = AsyncMock(return_value="stream")

        async def request_iter():
            yield "item"

        with patch("kubemq.transport.interceptors.grpc.aio.ClientCallDetails") as mock_ccd:
            mock_ccd.return_value = MagicMock()
            await interceptor.intercept_stream_stream(
                mock_continuation, mock_details, request_iter()
            )

        call_kwargs = mock_ccd.call_args
        metadata = call_kwargs[1]["metadata"]
        # Should preserve existing but not add auth
        assert ("key", "val") in metadata
        assert not any(m[0] == "authorization" for m in metadata)
