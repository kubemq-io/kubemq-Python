"""gRPC interceptors for authentication.

This module provides both sync and async interceptors for adding
authentication metadata to gRPC calls.
"""

from __future__ import annotations

from collections.abc import AsyncIterable
from typing import Any, Callable

import grpc
import grpc.aio
from grpc import ClientCallDetails


def _inject_auth_metadata(
    metadata: tuple[tuple[str, str], ...] | None,
    auth_token: str | None,
) -> tuple[tuple[str, str], ...]:
    """Helper to inject auth token into metadata.

    Args:
        metadata: Existing metadata tuple or None.
        auth_token: The auth token to inject.

    Returns:
        New metadata tuple with auth token added.
    """
    if not auth_token or not auth_token.strip():
        return metadata or ()

    existing = list(metadata or [])
    existing.append(("authorization", auth_token))
    return tuple(existing)


# =============================================================================
# Sync Interceptors
# =============================================================================


class AuthInterceptors(
    grpc.UnaryUnaryClientInterceptor,
    grpc.StreamUnaryClientInterceptor,
    grpc.UnaryStreamClientInterceptor,
    grpc.StreamStreamClientInterceptor,
):
    """Sync interceptor that adds auth token to all gRPC calls."""

    def __init__(self, auth_token: str):
        self.auth_token = auth_token

    def _intercept_call(
        self,
        continuation: Callable[[ClientCallDetails, Any], Any],
        client_call_details: ClientCallDetails,
        request_or_iterator: Any,
    ) -> Any:
        metadata = []
        if client_call_details.metadata is not None:
            metadata = list(client_call_details.metadata)

        if self.auth_token and self.auth_token.strip():
            metadata.append(("authorization", f"{self.auth_token}"))

        new_client_call_details = client_call_details._replace(metadata=metadata)  # type: ignore[union-attr]
        response = continuation(new_client_call_details, request_or_iterator)
        return response

    def intercept_unary_unary(self, continuation, client_call_details, request):
        return self._intercept_call(continuation, client_call_details, request)

    def intercept_unary_stream(self, continuation, client_call_details, request):
        return self._intercept_call(continuation, client_call_details, request)

    def intercept_stream_stream(self, continuation, client_call_details, request_iterator):
        return self._intercept_call(continuation, client_call_details, request_iterator)

    def intercept_stream_unary(self, continuation, client_call_details, request_iterator):
        return self._intercept_call(continuation, client_call_details, request_iterator)


# =============================================================================
# Async Interceptors - Combined class (legacy support)
# =============================================================================


class AuthInterceptorsAsync(
    grpc.aio.UnaryUnaryClientInterceptor,
    grpc.aio.StreamStreamClientInterceptor,
    grpc.aio.UnaryStreamClientInterceptor,
    grpc.aio.StreamUnaryClientInterceptor,
):
    """Async interceptor that adds auth token to all gRPC calls.

    Note: This combined class is kept for backward compatibility.
    For Phase 4 native async, use the separate interceptor classes below.
    """

    def __init__(self, auth_token: str):
        self.auth_token = auth_token

    async def _intercept_call(
        self,
        continuation: Callable[[ClientCallDetails, Any], Any],
        client_call_details: ClientCallDetails,
        request_or_iterator: Any,
    ) -> Any:
        metadata = []
        if client_call_details.metadata is not None:
            metadata = list(client_call_details.metadata)

        if self.auth_token and self.auth_token.strip():
            metadata.append(("authorization", f"{self.auth_token}"))

        new_client_call_details = client_call_details._replace(metadata=metadata)  # type: ignore[union-attr]
        response = await continuation(new_client_call_details, request_or_iterator)
        return response

    async def intercept_unary_unary(self, continuation, client_call_details, request):
        return await self._intercept_call(continuation, client_call_details, request)

    async def intercept_unary_stream(self, continuation, client_call_details, request):
        return await self._intercept_call(continuation, client_call_details, request)

    async def intercept_stream_stream(self, continuation, client_call_details, request_iterator):
        return await self._intercept_call(continuation, client_call_details, request_iterator)

    async def intercept_stream_unary(self, continuation, client_call_details, request_iterator):
        return await self._intercept_call(continuation, client_call_details, request_iterator)


# =============================================================================
# Async Interceptors - Separate classes (Phase 4 native async)
# =============================================================================


class AsyncUnaryUnaryAuthInterceptor(grpc.aio.UnaryUnaryClientInterceptor):
    """Async interceptor for unary-unary calls (ping, send_event, etc.).

    This interceptor is used for simple request-response operations like
    Ping, SendEvent, SendRequest, SendResponse, etc.
    """

    def __init__(self, auth_token: str | None = None) -> None:
        self._auth_token = auth_token

    async def intercept_unary_unary(
        self,
        continuation: Callable[..., Any],
        client_call_details: grpc.aio.ClientCallDetails,
        request: Any,
    ) -> Any:
        new_details = grpc.aio.ClientCallDetails(
            method=client_call_details.method,
            timeout=client_call_details.timeout,
            metadata=_inject_auth_metadata(client_call_details.metadata, self._auth_token),
            credentials=client_call_details.credentials,
            wait_for_ready=client_call_details.wait_for_ready,
        )
        return await continuation(new_details, request)


class AsyncUnaryStreamAuthInterceptor(grpc.aio.UnaryStreamClientInterceptor):
    """Async interceptor for unary-stream calls (subscriptions).

    This interceptor is CRITICAL for SubscribeToEvents, SubscribeToRequests, etc.
    """

    def __init__(self, auth_token: str | None = None) -> None:
        self._auth_token = auth_token

    async def intercept_unary_stream(
        self,
        continuation: Callable[..., Any],
        client_call_details: grpc.aio.ClientCallDetails,
        request: Any,
    ) -> grpc.aio.UnaryStreamCall:
        new_details = grpc.aio.ClientCallDetails(
            method=client_call_details.method,
            timeout=client_call_details.timeout,
            metadata=_inject_auth_metadata(client_call_details.metadata, self._auth_token),
            credentials=client_call_details.credentials,
            wait_for_ready=client_call_details.wait_for_ready,
        )
        return await continuation(new_details, request)


class AsyncStreamUnaryAuthInterceptor(grpc.aio.StreamUnaryClientInterceptor):
    """Async interceptor for stream-unary calls (queue send batch)."""

    def __init__(self, auth_token: str | None = None) -> None:
        self._auth_token = auth_token

    async def intercept_stream_unary(
        self,
        continuation: Callable[..., Any],
        client_call_details: grpc.aio.ClientCallDetails,
        request_iterator: AsyncIterable[Any],
    ) -> Any:
        new_details = grpc.aio.ClientCallDetails(
            method=client_call_details.method,
            timeout=client_call_details.timeout,
            metadata=_inject_auth_metadata(client_call_details.metadata, self._auth_token),
            credentials=client_call_details.credentials,
            wait_for_ready=client_call_details.wait_for_ready,
        )
        return await continuation(new_details, request_iterator)


class AsyncStreamStreamAuthInterceptor(grpc.aio.StreamStreamClientInterceptor):
    """Async interceptor for bidirectional streaming (queue upstream/downstream).

    This interceptor is CRITICAL for QueuesUpstream and QueuesDownstream.
    """

    def __init__(self, auth_token: str | None = None) -> None:
        self._auth_token = auth_token

    async def intercept_stream_stream(
        self,
        continuation: Callable[..., Any],
        client_call_details: grpc.aio.ClientCallDetails,
        request_iterator: AsyncIterable[Any],
    ) -> grpc.aio.StreamStreamCall:
        new_details = grpc.aio.ClientCallDetails(
            method=client_call_details.method,
            timeout=client_call_details.timeout,
            metadata=_inject_auth_metadata(client_call_details.metadata, self._auth_token),
            credentials=client_call_details.credentials,
            wait_for_ready=client_call_details.wait_for_ready,
        )
        return await continuation(new_details, request_iterator)
