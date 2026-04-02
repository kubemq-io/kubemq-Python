"""Tests for kubemq._internal.auth module."""

from __future__ import annotations

import asyncio
import threading
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from kubemq._internal.auth import StaticTokenProvider, TokenHolder, TokenManager


class TestTokenHolder:
    def test_initial_none(self):
        holder = TokenHolder()
        assert holder.token is None
        assert holder.is_present is False

    def test_initial_with_token(self):
        holder = TokenHolder("my-token")
        assert holder.token == "my-token"
        assert holder.is_present is True

    def test_set_token(self):
        holder = TokenHolder()
        holder.token = "abc"
        assert holder.token == "abc"
        assert holder.is_present is True

    def test_clear_token(self):
        holder = TokenHolder("token")
        holder.token = None
        assert holder.is_present is False

    def test_whitespace_only_not_present(self):
        holder = TokenHolder("   ")
        assert holder.is_present is False

    def test_empty_string_not_present(self):
        holder = TokenHolder("")
        assert holder.is_present is False

    def test_repr(self):
        holder = TokenHolder("secret")
        assert "present=True" in repr(holder)

    def test_thread_safety(self):
        holder = TokenHolder()
        errors = []

        def writer():
            try:
                for i in range(1000):
                    holder.token = f"token-{i}"
            except Exception as e:
                errors.append(e)

        def reader():
            try:
                for _ in range(1000):
                    _ = holder.token
                    _ = holder.is_present
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=writer) for _ in range(3)]
        threads += [threading.Thread(target=reader) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        assert errors == []


class TestStaticTokenProvider:
    def test_get_token(self):
        provider = StaticTokenProvider("fixed-token")
        token, expires_at = provider.get_token()
        assert token == "fixed-token"
        assert expires_at is None

    def test_empty_token_raises(self):
        with pytest.raises(ValueError, match="non-empty"):
            StaticTokenProvider("")

    def test_whitespace_token_raises(self):
        with pytest.raises(ValueError, match="non-empty"):
            StaticTokenProvider("   ")

    def test_repr(self):
        provider = StaticTokenProvider("abc")
        assert "token_present=True" in repr(provider)


class TestTokenManager:
    @pytest.fixture
    def holder(self):
        return TokenHolder()

    @pytest.fixture
    def sync_provider(self):
        provider = MagicMock()
        provider.get_token.return_value = ("test-token", None)
        return provider

    @pytest.mark.asyncio
    async def test_get_token_from_sync_provider(self, holder, sync_provider):
        mgr = TokenManager(sync_provider, holder)
        token = await mgr.get_token()
        assert token == "test-token"
        assert holder.token == "test-token"

    @pytest.mark.asyncio
    async def test_cached_token_returned(self, holder, sync_provider):
        mgr = TokenManager(sync_provider, holder)

        await mgr.get_token()
        await mgr.get_token()

        sync_provider.get_token.assert_called_once()

    @pytest.mark.asyncio
    async def test_invalidate_clears_cache(self, holder, sync_provider):
        mgr = TokenManager(sync_provider, holder)

        await mgr.get_token()
        await mgr.invalidate()
        await mgr.get_token()

        assert sync_provider.get_token.call_count == 2

    @pytest.mark.asyncio
    async def test_expired_token_refreshed(self, holder):
        already_expired = datetime.now(UTC) - timedelta(seconds=10)
        provider = MagicMock()
        provider.get_token.return_value = ("fresh-token", None)

        mgr = TokenManager(provider, holder)
        mgr._cached_token = "old"
        mgr._expires_at = already_expired

        token = await mgr.get_token()
        assert token == "fresh-token"

    @pytest.mark.asyncio
    async def test_non_expired_token_not_refreshed(self, holder):
        future = datetime.now(UTC) + timedelta(hours=1)
        provider = MagicMock()
        provider.get_token.return_value = ("token", future)

        mgr = TokenManager(provider, holder)
        mgr._cached_token = "cached"
        mgr._expires_at = future

        token = await mgr.get_token()
        assert token == "cached"
        provider.get_token.assert_not_called()

    @pytest.mark.asyncio
    async def test_close_sets_closed(self, holder, sync_provider):
        mgr = TokenManager(sync_provider, holder)
        await mgr.close()
        assert mgr._closed is True

    @pytest.mark.asyncio
    async def test_provider_exception_raises(self, holder):
        provider = MagicMock()
        provider.get_token.side_effect = RuntimeError("provider broken")

        mgr = TokenManager(provider, holder)

        from kubemq.core.exceptions import KubeMQError

        with pytest.raises(KubeMQError, match="provider failed"):
            await mgr.get_token()


# ==============================================================================
# Coverage extension: lines 143, 150-152, 165, 180, 190-197, 201-210, 217-218
# ==============================================================================


class TestTokenManagerAsyncProvider:
    @pytest.fixture
    def holder(self):
        return TokenHolder()

    @pytest.mark.asyncio
    async def test_async_provider_path(self, holder):
        class AsyncProvider:
            async def get_token(self):
                return ("async-token", None)

        mgr = TokenManager(AsyncProvider(), holder)
        token = await mgr.get_token()
        assert token == "async-token"
        assert holder.token == "async-token"
        await mgr.close()

    @pytest.mark.asyncio
    async def test_async_provider_timeout(self, holder):
        class SlowAsyncProvider:
            async def get_token(self):
                await asyncio.sleep(100)
                return ("token", None)

        from kubemq.core.exceptions import KubeMQTimeoutError

        mgr = TokenManager(SlowAsyncProvider(), holder, credential_timeout=0.05)
        with pytest.raises(KubeMQTimeoutError, match="timed out"):
            await mgr.get_token()
        await mgr.close()


class TestTokenManagerAuthErrorReraise:
    @pytest.fixture
    def holder(self):
        return TokenHolder()

    @pytest.mark.asyncio
    async def test_auth_error_reraised_directly(self, holder):
        from kubemq.core.exceptions import KubeMQAuthenticationError

        provider = MagicMock()
        provider.get_token.side_effect = KubeMQAuthenticationError("invalid token")

        mgr = TokenManager(provider, holder)
        with pytest.raises(KubeMQAuthenticationError, match="invalid token"):
            await mgr.get_token()
        await mgr.close()


class TestTokenManagerProactiveRefresh:
    @pytest.fixture
    def holder(self):
        return TokenHolder()

    @pytest.mark.asyncio
    async def test_proactive_refresh_scheduled(self, holder):
        future = datetime.now(UTC) + timedelta(minutes=5)
        provider = MagicMock()
        provider.get_token.return_value = ("token-1", future)

        mgr = TokenManager(provider, holder)
        await mgr.get_token()

        assert mgr._refresh_task is not None
        assert not mgr._refresh_task.done()
        await mgr.close()

    @pytest.mark.asyncio
    async def test_proactive_refresh_fires(self, holder):
        from unittest.mock import AsyncMock, patch

        call_count = [0]

        def make_token():
            call_count[0] += 1
            if call_count[0] == 1:
                return (
                    "token-1",
                    datetime.now(UTC) + timedelta(minutes=5),
                )
            return ("token-2", None)

        provider = MagicMock()
        provider.get_token.side_effect = make_token

        with patch("kubemq._internal.auth.asyncio.sleep", new_callable=AsyncMock):
            mgr = TokenManager(provider, holder)
            await mgr.get_token()

            if mgr._refresh_task:
                await mgr._refresh_task

            assert provider.get_token.call_count == 2
            assert holder.token == "token-2"
            await mgr.close()

    @pytest.mark.asyncio
    async def test_proactive_refresh_failure_is_logged(self, holder):
        from unittest.mock import AsyncMock, patch

        call_count = [0]

        def make_token():
            call_count[0] += 1
            if call_count[0] == 1:
                return (
                    "token-1",
                    datetime.now(UTC) + timedelta(minutes=5),
                )
            raise RuntimeError("refresh failed")

        provider = MagicMock()
        provider.get_token.side_effect = make_token

        with patch("kubemq._internal.auth.asyncio.sleep", new_callable=AsyncMock):
            mgr = TokenManager(provider, holder)
            await mgr.get_token()

            if mgr._refresh_task:
                await mgr._refresh_task

            assert holder.token == "token-1"
            await mgr.close()


class TestTokenManagerCloseWithRefresh:
    @pytest.fixture
    def holder(self):
        return TokenHolder()

    @pytest.mark.asyncio
    async def test_close_cancels_active_refresh_task(self, holder):
        future = datetime.now(UTC) + timedelta(minutes=5)
        provider = MagicMock()
        provider.get_token.return_value = ("token-1", future)

        mgr = TokenManager(provider, holder)
        await mgr.get_token()

        assert mgr._refresh_task is not None
        refresh_task = mgr._refresh_task

        await mgr.close()

        try:
            await refresh_task
        except asyncio.CancelledError:
            pass

        assert mgr._closed is True
        assert mgr._refresh_task is None
        assert refresh_task.cancelled()


class TestTokenManagerConcurrent:
    @pytest.fixture
    def holder(self):
        return TokenHolder()

    @pytest.mark.asyncio
    async def test_concurrent_get_token_serialized(self, holder):
        call_count = [0]

        class CountingProvider:
            async def get_token(self):
                call_count[0] += 1
                await asyncio.sleep(0.01)
                return (f"token-{call_count[0]}", None)

        mgr = TokenManager(CountingProvider(), holder)

        results = await asyncio.gather(
            mgr.get_token(),
            mgr.get_token(),
            mgr.get_token(),
        )

        assert results[0] == results[1] == results[2]
        assert call_count[0] == 1
        await mgr.close()

    @pytest.mark.asyncio
    async def test_invalidate_then_get_token(self, holder):
        call_count = [0]

        def make_token():
            call_count[0] += 1
            return (f"token-{call_count[0]}", None)

        provider = MagicMock()
        provider.get_token.side_effect = make_token

        mgr = TokenManager(provider, holder)
        t1 = await mgr.get_token()
        await mgr.invalidate()
        t2 = await mgr.get_token()

        assert t1 == "token-1"
        assert t2 == "token-2"
        assert provider.get_token.call_count == 2
        await mgr.close()
