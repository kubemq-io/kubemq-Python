"""Tests for AsyncCancellationToken and CancellationTokenBridge."""

from __future__ import annotations

import asyncio
import gc

import pytest

from kubemq.common.async_cancellation_token import (
    AsyncCancellationToken,
    CancellationTokenBridge,
)
from kubemq.common.cancellation_token import CancellationToken


class TestAsyncCancellationToken:
    """Tests for AsyncCancellationToken."""

    def test_initial_state_is_not_cancelled(self):
        """Test that a new token is not cancelled."""
        token = AsyncCancellationToken()
        assert not token.is_cancelled
        assert repr(token) == "AsyncCancellationToken(active)"

    def test_cancel_sets_is_cancelled(self):
        """Test that calling cancel() sets is_cancelled to True."""
        token = AsyncCancellationToken()
        token.cancel()
        assert token.is_cancelled
        assert repr(token) == "AsyncCancellationToken(cancelled)"

    def test_cancel_is_idempotent(self):
        """Test that calling cancel() multiple times is safe."""
        token = AsyncCancellationToken()
        token.cancel()
        token.cancel()
        token.cancel()
        assert token.is_cancelled

    @pytest.mark.asyncio
    async def test_wait_returns_true_when_cancelled(self):
        """Test that wait() returns True when token is cancelled."""
        token = AsyncCancellationToken()

        async def cancel_after_delay():
            await asyncio.sleep(0.05)
            token.cancel()

        asyncio.create_task(cancel_after_delay())
        result = await token.wait(timeout=1.0)
        assert result is True
        assert token.is_cancelled

    @pytest.mark.asyncio
    async def test_wait_returns_false_on_timeout(self):
        """Test that wait() returns False when timeout occurs."""
        token = AsyncCancellationToken()
        result = await token.wait(timeout=0.05)
        assert result is False
        assert not token.is_cancelled

    @pytest.mark.asyncio
    async def test_wait_already_cancelled(self):
        """Test that wait() returns immediately if already cancelled."""
        token = AsyncCancellationToken()
        token.cancel()
        result = await token.wait(timeout=0.05)
        assert result is True


class TestAsyncCancellationTokenParentChild:
    """Tests for parent-child relationships."""

    def test_child_inherits_cancelled_state(self):
        """Test that child token reflects parent's cancelled state."""
        parent = AsyncCancellationToken()
        child = AsyncCancellationToken(parent=parent)

        assert not child.is_cancelled
        parent.cancel()
        assert child.is_cancelled

    def test_child_cancelled_immediately_if_parent_already_cancelled(self):
        """Test that child is cancelled immediately if parent already cancelled."""
        parent = AsyncCancellationToken()
        parent.cancel()
        child = AsyncCancellationToken(parent=parent)
        assert child.is_cancelled

    def test_cancelling_child_does_not_affect_parent(self):
        """Test that cancelling child does not cancel parent."""
        parent = AsyncCancellationToken()
        child = AsyncCancellationToken(parent=parent)

        child.cancel()
        assert child.is_cancelled
        assert not parent.is_cancelled

    def test_multiple_children(self):
        """Test multiple children are cancelled when parent is cancelled."""
        parent = AsyncCancellationToken()
        child1 = AsyncCancellationToken(parent=parent)
        child2 = AsyncCancellationToken(parent=parent)
        child3 = AsyncCancellationToken(parent=parent)

        assert not child1.is_cancelled
        assert not child2.is_cancelled
        assert not child3.is_cancelled

        parent.cancel()

        assert child1.is_cancelled
        assert child2.is_cancelled
        assert child3.is_cancelled

    def test_grandchildren_cascade(self):
        """Test that cancellation cascades through multiple levels."""
        grandparent = AsyncCancellationToken()
        parent = AsyncCancellationToken(parent=grandparent)
        child = AsyncCancellationToken(parent=parent)

        assert not child.is_cancelled
        grandparent.cancel()
        assert child.is_cancelled

    def test_weak_references_allow_gc(self):
        """Test that child tokens can be garbage collected."""
        parent = AsyncCancellationToken()

        # Create and delete child
        child = AsyncCancellationToken(parent=parent)
        id(child)
        del child
        gc.collect()

        # Parent should still work (no exceptions)
        parent.cancel()
        assert parent.is_cancelled


class TestAsyncCancellationTokenLinked:
    """Tests for linked tokens."""

    def test_create_linked_with_single_parent(self):
        """Test creating a linked token with one parent."""
        parent = AsyncCancellationToken()
        linked = AsyncCancellationToken.create_linked(parent)

        assert not linked.is_cancelled
        parent.cancel()
        assert linked.is_cancelled

    def test_create_linked_with_multiple_parents(self):
        """Test that linked token cancels when ANY parent cancels."""
        parent1 = AsyncCancellationToken()
        parent2 = AsyncCancellationToken()
        parent3 = AsyncCancellationToken()
        linked = AsyncCancellationToken.create_linked(parent1, parent2, parent3)

        assert not linked.is_cancelled

        # Cancel just one parent
        parent2.cancel()
        assert linked.is_cancelled

        # Other parents are not affected
        assert not parent1.is_cancelled
        assert not parent3.is_cancelled

    def test_linked_already_cancelled_parent(self):
        """Test linked token with already cancelled parent."""
        parent1 = AsyncCancellationToken()
        parent2 = AsyncCancellationToken()
        parent2.cancel()

        linked = AsyncCancellationToken.create_linked(parent1, parent2)
        assert linked.is_cancelled

    def test_create_linked_no_parents(self):
        """Test creating a linked token with no parents."""
        linked = AsyncCancellationToken.create_linked()
        assert not linked.is_cancelled
        linked.cancel()
        assert linked.is_cancelled


class TestCancellationTokenBridge:
    """Tests for CancellationTokenBridge."""

    @pytest.mark.asyncio
    async def test_async_token_property(self):
        """Test that async_token property returns the async token."""
        sync_token = CancellationToken()
        bridge = CancellationTokenBridge(sync_token)

        assert isinstance(bridge.async_token, AsyncCancellationToken)
        assert not bridge.async_token.is_cancelled

    @pytest.mark.asyncio
    async def test_bridge_propagates_cancellation(self):
        """Test that cancelling sync token cancels async token."""
        sync_token = CancellationToken()
        bridge = CancellationTokenBridge(sync_token, poll_interval_seconds=0.01)

        await bridge.start_monitoring()

        assert not bridge.async_token.is_cancelled
        sync_token.cancel()

        # Wait for polling to detect cancellation
        await asyncio.sleep(0.05)

        assert bridge.async_token.is_cancelled

        await bridge.stop_monitoring()

    @pytest.mark.asyncio
    async def test_bridge_context_manager(self):
        """Test using bridge as async context manager."""
        sync_token = CancellationToken()
        bridge = CancellationTokenBridge(sync_token, poll_interval_seconds=0.01)

        async with bridge as b:
            assert b is bridge
            assert not bridge.async_token.is_cancelled

            sync_token.cancel()
            await asyncio.sleep(0.05)

            assert bridge.async_token.is_cancelled

    @pytest.mark.asyncio
    async def test_stop_monitoring_is_idempotent(self):
        """Test that stop_monitoring can be called multiple times."""
        sync_token = CancellationToken()
        bridge = CancellationTokenBridge(sync_token)

        await bridge.start_monitoring()
        await bridge.stop_monitoring()
        await bridge.stop_monitoring()  # Should not raise

    @pytest.mark.asyncio
    async def test_start_monitoring_is_idempotent(self):
        """Test that start_monitoring can be called multiple times."""
        sync_token = CancellationToken()
        bridge = CancellationTokenBridge(sync_token)

        await bridge.start_monitoring()
        await bridge.start_monitoring()  # Should not create another task
        await bridge.stop_monitoring()

    @pytest.mark.asyncio
    async def test_bridge_with_already_cancelled_sync_token(self):
        """Test bridge with already cancelled sync token."""
        sync_token = CancellationToken()
        sync_token.cancel()

        bridge = CancellationTokenBridge(sync_token, poll_interval_seconds=0.01)
        await bridge.start_monitoring()

        # Should propagate cancellation quickly
        await asyncio.sleep(0.05)
        assert bridge.async_token.is_cancelled

        await bridge.stop_monitoring()


class TestSyncCancellationTokenConsistency:
    """Tests for API consistency between sync and async tokens."""

    def test_sync_token_has_is_cancelled_property(self):
        """Test that sync CancellationToken has is_cancelled property."""
        token = CancellationToken()
        assert hasattr(token, "is_cancelled")
        assert not token.is_cancelled

        token.cancel()
        assert token.is_cancelled

    def test_sync_token_cancel_method(self):
        """Test that sync CancellationToken has cancel() method."""
        token = CancellationToken()
        assert hasattr(token, "cancel")

        token.cancel()
        assert token.is_cancelled

    def test_async_and_sync_tokens_have_consistent_api(self):
        """Test that both token types have consistent API."""
        sync_token = CancellationToken()
        async_token = AsyncCancellationToken()

        # Both should have cancel method
        assert hasattr(sync_token, "cancel")
        assert hasattr(async_token, "cancel")

        # Both should have is_cancelled property
        assert hasattr(sync_token, "is_cancelled")
        assert hasattr(async_token, "is_cancelled")

        # Both start as not cancelled
        assert not sync_token.is_cancelled
        assert not async_token.is_cancelled

        # Both can be cancelled
        sync_token.cancel()
        async_token.cancel()

        assert sync_token.is_cancelled
        assert async_token.is_cancelled
