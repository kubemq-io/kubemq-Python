"""Authentication internals for KubeMQ Python SDK.

This module contains:
- TokenHolder: Thread-safe mutable container for the current auth token.
- StaticTokenProvider: Built-in CredentialProvider wrapping a fixed token.
- TokenManager: Token lifecycle manager with caching and proactive refresh.

Not part of the public API — import from kubemq.core or kubemq directly.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from kubemq.core.types import AsyncCredentialProvider, CredentialProvider

logger = logging.getLogger("kubemq.auth")

_DEFAULT_REFRESH_BUFFER = timedelta(seconds=30)
_MIN_REFRESH_INTERVAL = timedelta(seconds=5)


class TokenHolder:
    """Thread-safe mutable container for the current auth token.

    All interceptors hold a reference to the same TokenHolder instance.
    Token reads/writes are serialized via threading.Lock (safe for both
    sync and async paths since the lock is never held across an await).
    """

    __slots__ = ("_token", "_lock")

    def __init__(self, token: Optional[str] = None) -> None:
        self._token = token
        self._lock = threading.Lock()

    @property
    def token(self) -> Optional[str]:
        with self._lock:
            return self._token

    @token.setter
    def token(self, value: Optional[str]) -> None:
        with self._lock:
            self._token = value

    @property
    def is_present(self) -> bool:
        with self._lock:
            return bool(self._token and self._token.strip())

    def __repr__(self) -> str:
        return f"TokenHolder(present={self.is_present})"


class StaticTokenProvider:
    """Built-in CredentialProvider wrapping a fixed token string.

    Implements both sync CredentialProvider and async AsyncCredentialProvider
    protocols. The token never expires (returns expires_at=None).
    Created automatically when ClientConfig.auth_token is set.
    """

    __slots__ = ("_token",)

    def __init__(self, token: str) -> None:
        if not token or not token.strip():
            raise ValueError("Static token must be a non-empty string")
        self._token = token

    def get_token(self) -> tuple[str, Optional[datetime]]:
        """Sync get_token — satisfies CredentialProvider protocol."""
        return (self._token, None)

    def __repr__(self) -> str:
        return f"StaticTokenProvider(token_present=True)"


class TokenManager:
    """Manages token lifecycle: caching, serialized refresh, proactive rotation.

    Responsibilities:
    1. Cache the current token from the CredentialProvider.
    2. Serialize calls to get_token() — at most one outstanding at a time.
    3. Reactive refresh: on UNAUTHENTICATED, invalidate cache and re-invoke.
    4. Proactive refresh: schedule refresh before expiry when expires_at provided.

    Thread safety: All public methods are async and use asyncio.Lock for
    serialization. The TokenHolder (from SPEC-AUTH-1) is updated atomically
    after each successful token acquisition.
    """

    def __init__(
        self,
        provider: CredentialProvider | AsyncCredentialProvider,
        token_holder: TokenHolder,
        refresh_buffer: timedelta = _DEFAULT_REFRESH_BUFFER,
        credential_timeout: float = 5.0,
    ) -> None:
        self._provider = provider
        self._is_async_provider = asyncio.iscoroutinefunction(
            getattr(provider, "get_token", None)
        )
        self._token_holder = token_holder
        self._refresh_buffer = refresh_buffer
        self._credential_timeout = credential_timeout

        self._lock = asyncio.Lock()
        self._cached_token: Optional[str] = None
        self._expires_at: Optional[datetime] = None
        self._refresh_task: Optional[asyncio.Task[None]] = None
        self._closed = False

    async def get_token(self) -> str:
        """Get a valid token, refreshing from provider if needed.

        Serialized: only one call to the provider runs at a time.
        """
        async with self._lock:
            if self._cached_token and not self._is_expired():
                return self._cached_token
            return await self._refresh_locked()

    async def invalidate(self) -> None:
        """Invalidate the cached token (reactive refresh on UNAUTHENTICATED).

        The next call to get_token() will re-invoke the provider.
        """
        async with self._lock:
            self._cached_token = None
            self._expires_at = None
            self._cancel_proactive_refresh()

    async def _refresh_locked(self) -> str:
        """Invoke the provider and update cache. Must be called under _lock."""
        try:
            if self._is_async_provider:
                token, expires_at = await asyncio.wait_for(
                    self._provider.get_token(),
                    timeout=self._credential_timeout,
                )
            else:
                token, expires_at = self._provider.get_token()
        except asyncio.TimeoutError as e:
            from kubemq.core.exceptions import ErrorCode, KubeMQTimeoutError

            raise KubeMQTimeoutError(
                f"Credential provider timed out after {self._credential_timeout}s",
                code=ErrorCode.CONNECTION_TIMEOUT,
                operation="credential_refresh",
            ) from e
        except Exception as e:
            from kubemq.core.exceptions import (
                ErrorCode,
                KubeMQAuthenticationError,
                KubeMQError,
            )

            if isinstance(e, KubeMQAuthenticationError):
                raise
            raise KubeMQError(
                f"Credential provider failed: {e}",
                code=ErrorCode.UNAVAILABLE,
                cause=e,
                is_retryable=True,
            ) from e

        self._cached_token = token
        self._expires_at = expires_at
        self._token_holder.token = token

        logger.debug("Token refreshed, token_present=True, expires_at=%s", expires_at)

        if expires_at:
            self._schedule_proactive_refresh(expires_at)

        return token

    def _is_expired(self) -> bool:
        if self._expires_at is None:
            return False
        return datetime.now(timezone.utc) >= self._expires_at

    def _schedule_proactive_refresh(self, expires_at: datetime) -> None:
        self._cancel_proactive_refresh()
        now = datetime.now(timezone.utc)
        refresh_at = expires_at - self._refresh_buffer
        delay = max(
            (refresh_at - now).total_seconds(),
            _MIN_REFRESH_INTERVAL.total_seconds(),
        )
        self._refresh_task = asyncio.create_task(self._proactive_refresh(delay))

    async def _proactive_refresh(self, delay: float) -> None:
        """Background task that refreshes the token before expiry."""
        try:
            await asyncio.sleep(delay)
            if self._closed:
                return
            async with self._lock:
                await self._refresh_locked()
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.warning(
                "Proactive token refresh failed; will retry on next request",
                exc_info=True,
            )

    def _cancel_proactive_refresh(self) -> None:
        if self._refresh_task and not self._refresh_task.done():
            self._refresh_task.cancel()
            self._refresh_task = None

    async def close(self) -> None:
        """Cancel background tasks and release resources."""
        self._closed = True
        self._cancel_proactive_refresh()
