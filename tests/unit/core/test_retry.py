"""Tests for kubemq._internal.retry module.

Covers REQ-ERR-3 (Auto-Retry), REQ-ERR-7 (Retry Throttling).
"""

from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kubemq._internal.retry import (
    BackoffCalculator,
    OperationSafety,
    RetryExecutor,
)
from kubemq.core.config import JitterType, RetryPolicy
from kubemq.core.exceptions import (
    ErrorCode,
    KubeMQConnectionError,
    KubeMQError,
    KubeMQTimeoutError,
    KubeMQValidationError,
)


# -----------------------------------------------------------------------
# BackoffCalculator
# -----------------------------------------------------------------------


class TestBackoffCalculator:
    def test_no_jitter_exponential(self):
        policy = RetryPolicy(
            initial_backoff_ms=100,
            max_backoff_ms=10000,
            backoff_multiplier=2.0,
            jitter=JitterType.NONE,
        )
        calc = BackoffCalculator(policy)
        assert calc.delay_ms(0) == 100.0
        assert calc.delay_ms(1) == 200.0
        assert calc.delay_ms(2) == 400.0
        assert calc.delay_ms(3) == 800.0

    def test_capped_at_max(self):
        policy = RetryPolicy(
            initial_backoff_ms=1000,
            max_backoff_ms=2000,
            backoff_multiplier=3.0,
            jitter=JitterType.NONE,
        )
        calc = BackoffCalculator(policy)
        assert calc.delay_ms(0) == 1000.0
        assert calc.delay_ms(1) == 2000.0  # 3000 capped to 2000
        assert calc.delay_ms(5) == 2000.0

    def test_full_jitter_range(self):
        policy = RetryPolicy(
            initial_backoff_ms=1000,
            max_backoff_ms=10000,
            backoff_multiplier=2.0,
            jitter=JitterType.FULL,
        )
        calc = BackoffCalculator(policy)
        samples = [calc.delay_ms(0) for _ in range(1000)]
        assert min(samples) >= 0
        assert max(samples) <= 1000.0
        assert min(samples) < 200  # statistical: should have values near 0

    def test_equal_jitter_range(self):
        policy = RetryPolicy(
            initial_backoff_ms=1000,
            max_backoff_ms=10000,
            backoff_multiplier=2.0,
            jitter=JitterType.EQUAL,
        )
        calc = BackoffCalculator(policy)
        samples = [calc.delay_ms(0) for _ in range(1000)]
        assert min(samples) >= 500.0
        assert max(samples) <= 1000.0

    def test_delay_seconds_conversion(self):
        policy = RetryPolicy(
            initial_backoff_ms=1000,
            max_backoff_ms=10000,
            backoff_multiplier=2.0,
            jitter=JitterType.NONE,
        )
        calc = BackoffCalculator(policy)
        assert calc.delay_seconds(0) == 1.0
        assert calc.delay_seconds(1) == 2.0


# -----------------------------------------------------------------------
# OperationSafety
# -----------------------------------------------------------------------


class TestOperationSafety:
    def test_safe_always_operations(self):
        err = KubeMQTimeoutError("timeout", code=ErrorCode.CONNECTION_TIMEOUT)
        assert OperationSafety.is_safe_to_retry("SendEvent", err) is True
        assert OperationSafety.is_safe_to_retry("SubscribeToEvents", err) is True

    def test_unsafe_on_deadline_with_timeout(self):
        err = KubeMQTimeoutError("timeout", code=ErrorCode.CONNECTION_TIMEOUT)
        assert OperationSafety.is_safe_to_retry("SendQueueMessage", err) is False
        assert OperationSafety.is_safe_to_retry("SendCommand", err) is False

    def test_unsafe_on_deadline_with_transient(self):
        err = KubeMQConnectionError(
            "unavailable",
            code=ErrorCode.UNAVAILABLE,
            is_retryable=True,
        )
        assert OperationSafety.is_safe_to_retry("SendQueueMessage", err) is True

    def test_unknown_operation_falls_back_to_retryable(self):
        retryable = KubeMQConnectionError("fail", code=ErrorCode.UNAVAILABLE, is_retryable=True)
        non_retryable = KubeMQValidationError("fail", code=ErrorCode.VALIDATION_ERROR)
        assert OperationSafety.is_safe_to_retry("CustomOp", retryable) is True
        assert OperationSafety.is_safe_to_retry("CustomOp", non_retryable) is False


# -----------------------------------------------------------------------
# RetryExecutor — async path
# -----------------------------------------------------------------------


class TestRetryExecutorAsync:
    async def test_success_first_attempt(self):
        policy = RetryPolicy()
        executor = RetryExecutor(policy)

        fn = AsyncMock(return_value="ok")
        result = await executor.execute("TestOp", fn)
        assert result == "ok"
        assert fn.call_count == 1

    async def test_transient_error_retried_then_succeeds(self):
        policy = RetryPolicy(
            max_retries=3,
            initial_backoff_ms=50,
            max_backoff_ms=1000,
            jitter=JitterType.NONE,
        )
        executor = RetryExecutor(policy)

        call_count = 0

        async def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise KubeMQConnectionError(
                    "unavailable",
                    code=ErrorCode.UNAVAILABLE,
                    is_retryable=True,
                )
            return "success"

        result = await executor.execute("SendEvent", flaky)
        assert result == "success"
        assert call_count == 3

    async def test_non_retryable_error_immediate(self):
        policy = RetryPolicy()
        executor = RetryExecutor(policy)

        call_count = 0

        async def fail_auth():
            nonlocal call_count
            call_count += 1
            raise KubeMQValidationError("bad input", code=ErrorCode.VALIDATION_ERROR)

        with pytest.raises(KubeMQValidationError):
            await executor.execute("TestOp", fail_auth)
        assert call_count == 1

    async def test_max_retries_zero_disables(self):
        policy = RetryPolicy(max_retries=0)
        executor = RetryExecutor(policy)

        call_count = 0

        async def fail():
            nonlocal call_count
            call_count += 1
            raise KubeMQConnectionError(
                "unavailable", code=ErrorCode.UNAVAILABLE, is_retryable=True
            )

        with pytest.raises(KubeMQConnectionError):
            await executor.execute("TestOp", fail)
        assert call_count == 1

    async def test_unknown_error_max_one_retry(self):
        policy = RetryPolicy(
            max_retries=5,
            initial_backoff_ms=50,
            max_backoff_ms=1000,
            jitter=JitterType.NONE,
        )
        executor = RetryExecutor(policy)

        call_count = 0

        async def fail_unknown():
            nonlocal call_count
            call_count += 1
            raise KubeMQError("unknown", code=ErrorCode.UNKNOWN, is_retryable=True)

        with pytest.raises(KubeMQError):
            await executor.execute("TestOp", fail_unknown)
        assert call_count == 2  # initial + 1 retry

    async def test_queue_send_deadline_not_retried(self):
        policy = RetryPolicy(
            max_retries=3,
            initial_backoff_ms=50,
            max_backoff_ms=1000,
        )
        executor = RetryExecutor(policy)

        call_count = 0

        async def fail_timeout():
            nonlocal call_count
            call_count += 1
            raise KubeMQTimeoutError("timeout")

        with pytest.raises(KubeMQTimeoutError) as exc_info:
            await executor.execute("SendQueueMessage", fail_timeout)
        assert call_count == 1
        assert exc_info.value.is_retryable is False

    async def test_events_publish_deadline_retried(self):
        policy = RetryPolicy(
            max_retries=3,
            initial_backoff_ms=50,
            max_backoff_ms=1000,
            jitter=JitterType.NONE,
        )
        executor = RetryExecutor(policy)

        call_count = 0

        async def fail_then_succeed():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise KubeMQTimeoutError("timeout")
            return "ok"

        result = await executor.execute("SendEvent", fail_then_succeed)
        assert result == "ok"
        assert call_count == 3

    async def test_retry_exhaustion_includes_context(self):
        policy = RetryPolicy(
            max_retries=2,
            initial_backoff_ms=50,
            max_backoff_ms=1000,
            jitter=JitterType.NONE,
        )
        executor = RetryExecutor(policy)

        async def always_fail():
            raise KubeMQConnectionError(
                "unavailable", code=ErrorCode.UNAVAILABLE, is_retryable=True
            )

        with pytest.raises(KubeMQConnectionError) as exc_info:
            await executor.execute("SendEvent", always_fail, channel="orders")
        err = exc_info.value
        assert err.details["retry_attempts"] == 2
        assert "retry_duration_seconds" in err.details
        assert err.operation == "SendEvent"
        assert err.channel == "orders"

    async def test_non_kubemq_exception_wrapped(self):
        policy = RetryPolicy()
        executor = RetryExecutor(policy)

        async def fail_raw():
            raise RuntimeError("unexpected")

        with pytest.raises(KubeMQError) as exc_info:
            await executor.execute("TestOp", fail_raw, channel="ch")
        assert exc_info.value.operation == "TestOp"
        assert exc_info.value.channel == "ch"
        assert isinstance(exc_info.value.cause, RuntimeError)

    async def test_enrichment_preserves_original_fields(self):
        policy = RetryPolicy(max_retries=1, initial_backoff_ms=50, max_backoff_ms=1000)
        executor = RetryExecutor(policy)

        async def fail():
            raise KubeMQConnectionError(
                "fail",
                code=ErrorCode.UNAVAILABLE,
                is_retryable=True,
                operation="OriginalOp",
                channel="original-ch",
            )

        with pytest.raises(KubeMQConnectionError) as exc_info:
            await executor.execute("FallbackOp", fail, channel="fallback-ch")
        assert exc_info.value.operation == "OriginalOp"
        assert exc_info.value.channel == "original-ch"


# -----------------------------------------------------------------------
# RetryExecutor — sync path
# -----------------------------------------------------------------------


class TestRetryExecutorSync:
    def test_success_first_attempt(self):
        policy = RetryPolicy()
        executor = RetryExecutor(policy)
        fn = MagicMock(return_value="ok")
        result = executor.execute_sync("TestOp", fn)
        assert result == "ok"
        assert fn.call_count == 1

    def test_transient_retried_then_succeeds(self):
        policy = RetryPolicy(
            max_retries=3,
            initial_backoff_ms=50,
            max_backoff_ms=1000,
            jitter=JitterType.NONE,
        )
        executor = RetryExecutor(policy)

        call_count = 0

        def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise KubeMQConnectionError(
                    "unavailable", code=ErrorCode.UNAVAILABLE, is_retryable=True
                )
            return "ok"

        result = executor.execute_sync("SendEvent", flaky)
        assert result == "ok"
        assert call_count == 3

    def test_max_retries_zero_disables(self):
        policy = RetryPolicy(max_retries=0)
        executor = RetryExecutor(policy)

        call_count = 0

        def fail():
            nonlocal call_count
            call_count += 1
            raise KubeMQConnectionError(
                "unavailable", code=ErrorCode.UNAVAILABLE, is_retryable=True
            )

        with pytest.raises(KubeMQConnectionError):
            executor.execute_sync("TestOp", fail)
        assert call_count == 1

    def test_retry_exhaustion_includes_context(self):
        policy = RetryPolicy(
            max_retries=2,
            initial_backoff_ms=50,
            max_backoff_ms=1000,
            jitter=JitterType.NONE,
        )
        executor = RetryExecutor(policy)

        def always_fail():
            raise KubeMQConnectionError(
                "unavailable", code=ErrorCode.UNAVAILABLE, is_retryable=True
            )

        with pytest.raises(KubeMQConnectionError) as exc_info:
            executor.execute_sync("SendEvent", always_fail, channel="q1")
        err = exc_info.value
        assert err.details["retry_attempts"] == 2
        assert "retry_duration_seconds" in err.details


# -----------------------------------------------------------------------
# RetryExecutor — throttling (ERR-7)
# -----------------------------------------------------------------------


class TestRetryThrottling:
    async def test_throttled_when_limit_reached(self):
        policy = RetryPolicy(
            max_retries=3,
            initial_backoff_ms=50,
            max_backoff_ms=1000,
            max_concurrent_retries=1,
            jitter=JitterType.NONE,
        )
        executor = RetryExecutor(policy)

        sem = executor._get_semaphore()
        await sem.acquire()

        call_count = 0

        async def fail_transient():
            nonlocal call_count
            call_count += 1
            raise KubeMQConnectionError(
                "unavailable", code=ErrorCode.UNAVAILABLE, is_retryable=True
            )

        with pytest.raises(KubeMQConnectionError) as exc_info:
            await executor.execute("TestOp", fail_transient)
        assert exc_info.value.details.get("retry_throttled") is True
        assert call_count == 1

        sem.release()

    async def test_unlimited_when_zero(self):
        policy = RetryPolicy(
            max_retries=2,
            initial_backoff_ms=50,
            max_backoff_ms=1000,
            max_concurrent_retries=0,
            jitter=JitterType.NONE,
        )
        executor = RetryExecutor(policy)

        call_count = 0

        async def fail_transient():
            nonlocal call_count
            call_count += 1
            raise KubeMQConnectionError(
                "unavailable", code=ErrorCode.UNAVAILABLE, is_retryable=True
            )

        with pytest.raises(KubeMQConnectionError):
            await executor.execute("SendEvent", fail_transient)
        assert call_count == 3  # 1 initial + 2 retries

    def test_sync_throttled_when_limit_reached(self):
        policy = RetryPolicy(
            max_retries=3,
            initial_backoff_ms=50,
            max_backoff_ms=1000,
            max_concurrent_retries=1,
            jitter=JitterType.NONE,
        )
        executor = RetryExecutor(policy)

        sem = executor._get_sync_semaphore()
        sem.acquire(blocking=False)

        call_count = 0

        def fail_transient():
            nonlocal call_count
            call_count += 1
            raise KubeMQConnectionError(
                "unavailable", code=ErrorCode.UNAVAILABLE, is_retryable=True
            )

        with pytest.raises(KubeMQConnectionError) as exc_info:
            executor.execute_sync("TestOp", fail_transient)
        assert exc_info.value.details.get("retry_throttled") is True
        assert call_count == 1

        sem.release()

    async def test_slot_freed_after_retry_completes(self):
        policy = RetryPolicy(
            max_retries=1,
            initial_backoff_ms=50,
            max_backoff_ms=1000,
            max_concurrent_retries=1,
            jitter=JitterType.NONE,
        )
        executor = RetryExecutor(policy)

        call_count = 0

        async def fail_then_succeed():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise KubeMQConnectionError(
                    "unavailable", code=ErrorCode.UNAVAILABLE, is_retryable=True
                )
            return "ok"

        result = await executor.execute("SendEvent", fail_then_succeed)
        assert result == "ok"

        call_count = 0
        result2 = await executor.execute("SendEvent", fail_then_succeed)
        assert result2 == "ok"
