"""Retry infrastructure for KubeMQ Python SDK.

Provides BackoffCalculator, OperationSafety, and RetryExecutor.
Implements REQ-ERR-3 (Auto-Retry), REQ-ERR-7 (Retry Throttling).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import time
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from kubemq.core.config import JitterType, RetryPolicy
from kubemq.core.exceptions import (
    ErrorCode,
    KubeMQError,
)

T = TypeVar("T")

logger = logging.getLogger("kubemq.retry")


class BackoffCalculator:
    """Compute backoff delays with configurable jitter.

    Shared by RetryExecutor (spec 01) and ReconnectionManager (spec 02).
    """

    def __init__(self, policy: RetryPolicy) -> None:
        self._initial_ms = policy.initial_backoff_ms
        self._max_ms = policy.max_backoff_ms
        self._multiplier = policy.backoff_multiplier
        self._jitter = policy.jitter

    def delay_ms(self, attempt: int) -> float:
        """Return the backoff delay in milliseconds for the given attempt (0-based)."""
        base = min(
            self._max_ms,
            self._initial_ms * (self._multiplier**attempt),
        )

        if self._jitter == JitterType.FULL:
            return random.uniform(0, base)
        elif self._jitter == JitterType.EQUAL:
            half = base / 2.0
            return half + random.uniform(0, half)
        else:
            return base

    def delay_seconds(self, attempt: int) -> float:
        """Return the backoff delay in seconds for the given attempt."""
        return self.delay_ms(attempt) / 1000.0


class OperationSafety:
    """Retry safety rules per operation type.

    Not all operations are safe to retry on ambiguous failures.
    """

    SAFE_ALWAYS: frozenset[str] = frozenset(
        {
            "SendEvent",
            "SendEventStore",
            "Subscribe",
            "SubscribeToEvents",
            "SubscribeToEventsStore",
            "SubscribeToCommands",
            "SubscribeToQueries",
        }
    )

    UNSAFE_ON_DEADLINE: frozenset[str] = frozenset(
        {
            "SendQueueMessage",
            "SendQueueMessagesBatch",
            "SendCommand",
            "SendQuery",
            "AckAllQueueMessages",
            "ReceiveQueueMessages",
        }
    )

    @classmethod
    def is_safe_to_retry(cls, operation: str, error: KubeMQError) -> bool:
        """Determine if retrying is safe for the given operation and error."""
        if operation in cls.SAFE_ALWAYS:
            return True
        if operation in cls.UNSAFE_ON_DEADLINE:
            if error.code == ErrorCode.CONNECTION_TIMEOUT:
                return False
            return error.is_retryable
        return error.is_retryable


class RetryExecutor:
    """Execute async callables with retry logic.

    Respects RetryPolicy, error classification, and operation safety rules.
    Throttles concurrent retries via asyncio.Semaphore (SPEC-ERR-7).

    asyncio.CancelledError (BaseException in Python 3.9+) is intentionally
    NOT caught — retrying a cancelled task is semantically wrong.
    """

    def __init__(self, policy: RetryPolicy) -> None:
        import threading

        self._policy = policy
        self._backoff = BackoffCalculator(policy)
        limit = policy.max_concurrent_retries
        effective_limit = limit if limit > 0 else 2**31
        self._semaphore = asyncio.Semaphore(effective_limit)
        self._sync_semaphore = threading.Semaphore(effective_limit)

    def _get_semaphore(self) -> asyncio.Semaphore:
        return self._semaphore

    def _get_sync_semaphore(self) -> Any:
        return self._sync_semaphore

    async def execute(
        self,
        operation_name: str,
        fn: Callable[..., Awaitable[T]],
        *args: Any,
        channel: str | None = None,
        request_id: str = "",
        **kwargs: Any,
    ) -> T:
        """Execute an async callable with retry logic.

        Args:
            operation_name: Name for logging and error context.
            fn: The async callable to execute.
            *args: Positional arguments for fn.
            channel: Channel name for error context.
            request_id: Shared request ID across retries.
            **kwargs: Keyword arguments for fn.

        Returns:
            The result of fn(*args, **kwargs).

        Raises:
            KubeMQError: After all retries exhausted, or on non-retryable error.
        """
        if self._policy.max_retries == 0:
            return await fn(*args, **kwargs)

        last_error: KubeMQError | None = None
        start_time = time.monotonic()

        for attempt in range(self._policy.max_retries + 1):
            try:
                return await fn(*args, **kwargs)
            except KubeMQError as e:
                enriched = _enrich_error(e, operation_name, channel, request_id)
                last_error = enriched

                if not enriched.is_retryable:
                    raise enriched from e

                if enriched.code == ErrorCode.UNKNOWN and attempt >= 1:
                    raise enriched from e

                if not OperationSafety.is_safe_to_retry(operation_name, enriched):
                    raise enriched.__class__(
                        enriched.message,
                        code=enriched.code,
                        operation=enriched.operation,
                        channel=enriched.channel,
                        request_id=enriched.request_id,
                        cause=enriched.cause,
                        is_retryable=False,
                        details=enriched.details,
                    ) from e

                if attempt >= self._policy.max_retries:
                    break

                sem = self._get_semaphore()
                acquired = await _try_acquire_async(sem)
                if not acquired:
                    enriched.details["retry_throttled"] = True
                    logger.warning(
                        "Retry throttled for %s (concurrent limit %d reached)",
                        operation_name,
                        self._policy.max_concurrent_retries,
                    )
                    raise enriched from e

                try:
                    delay = self._backoff.delay_seconds(attempt)
                    logger.debug(
                        "Retry %d/%d for %s after %.1fs (error: %s)",
                        attempt + 1,
                        self._policy.max_retries,
                        operation_name,
                        delay,
                        enriched.message,
                    )
                    await asyncio.sleep(delay)
                finally:
                    sem.release()

            except Exception as e:
                if isinstance(e, KubeMQError):
                    raise
                raise KubeMQError(
                    str(e),
                    operation=operation_name,
                    channel=channel,
                    cause=e,
                ) from e

        elapsed = time.monotonic() - start_time
        if last_error is None:
            raise RuntimeError("Retry loop ended without capturing an error")
        last_error.details["retry_attempts"] = self._policy.max_retries
        last_error.details["retry_duration_seconds"] = round(elapsed, 2)
        raise last_error from last_error.cause

    def execute_sync(
        self,
        operation_name: str,
        fn: Callable[..., T],
        *args: Any,
        channel: str | None = None,
        request_id: str = "",
        **kwargs: Any,
    ) -> T:
        """Execute a sync callable with retry logic.

        Mirrors the async execute() but uses time.sleep() and
        threading.Semaphore for the sync path.
        """
        if self._policy.max_retries == 0:
            return fn(*args, **kwargs)

        last_error: KubeMQError | None = None
        start_time = time.monotonic()

        for attempt in range(self._policy.max_retries + 1):
            try:
                return fn(*args, **kwargs)
            except KubeMQError as e:
                enriched = _enrich_error(e, operation_name, channel, request_id)
                last_error = enriched

                if not enriched.is_retryable:
                    raise enriched from e

                if enriched.code == ErrorCode.UNKNOWN and attempt >= 1:
                    raise enriched from e

                if not OperationSafety.is_safe_to_retry(operation_name, enriched):
                    raise enriched.__class__(
                        enriched.message,
                        code=enriched.code,
                        operation=enriched.operation,
                        channel=enriched.channel,
                        request_id=enriched.request_id,
                        cause=enriched.cause,
                        is_retryable=False,
                        details=enriched.details,
                    ) from e

                if attempt >= self._policy.max_retries:
                    break

                sem = self._get_sync_semaphore()
                if not sem.acquire(blocking=False):
                    enriched.details["retry_throttled"] = True
                    logger.warning(
                        "Retry throttled for %s",
                        operation_name,
                    )
                    raise enriched from e

                try:
                    delay = self._backoff.delay_seconds(attempt)
                    logger.debug(
                        "Retry %d/%d for %s after %.1fs",
                        attempt + 1,
                        self._policy.max_retries,
                        operation_name,
                        delay,
                    )
                    time.sleep(delay)
                finally:
                    sem.release()

            except Exception as e:
                if isinstance(e, KubeMQError):
                    raise
                raise KubeMQError(
                    str(e),
                    operation=operation_name,
                    channel=channel,
                    cause=e,
                ) from e

        elapsed = time.monotonic() - start_time
        if last_error is None:
            raise RuntimeError("Retry loop ended without capturing an error")
        last_error.details["retry_attempts"] = self._policy.max_retries
        last_error.details["retry_duration_seconds"] = round(elapsed, 2)
        raise last_error from last_error.cause


async def _try_acquire_async(sem: asyncio.Semaphore) -> bool:
    """Non-blocking acquire attempt for an asyncio.Semaphore.

    Avoids accessing the private ``_value`` attribute (PY-17).
    Schedules the acquire as a task and yields control once to
    let it run; if the semaphore is available, the task completes
    immediately on that iteration.
    """
    task = asyncio.ensure_future(sem.acquire())
    await asyncio.sleep(0)
    if task.done():
        return True
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    return False


def _enrich_error(
    error: KubeMQError,
    operation_name: str,
    channel: str | None,
    request_id: str,
) -> KubeMQError:
    """Create an enriched copy of an error with context fields filled in."""
    return error.__class__(
        error.message,
        code=error.code,
        operation=error.operation or operation_name,
        channel=error.channel or channel,
        request_id=request_id or error.request_id,
        cause=error.cause,
        is_retryable=error.is_retryable,
        details={**error.details},
    )
