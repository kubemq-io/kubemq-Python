"""Python version compatibility utilities."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any, TypeVar

T = TypeVar("T")

# Thread pool for async compatibility
# Note: Use Optional[] instead of | None for runtime compatibility with Python 3.9
_compat_executor: ThreadPoolExecutor | None = None


def _get_executor() -> ThreadPoolExecutor:
    """Get or create the compatibility thread pool executor."""
    global _compat_executor
    if _compat_executor is None:
        _compat_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="kubemq-compat")
    return _compat_executor


async def run_in_thread(func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    """Run a synchronous function in a thread pool.

    This provides a consistent interface that works on Python 3.9+
    using asyncio.to_thread, with graceful handling if unavailable.

    Args:
        func: The synchronous function to run
        *args: Positional arguments for the function
        **kwargs: Keyword arguments for the function

    Returns:
        The result of the function call
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_get_executor(), lambda: func(*args, **kwargs))


def cleanup_executor() -> None:
    """Clean up the compatibility thread pool executor.

    Call this during application shutdown if needed.
    """
    global _compat_executor
    if _compat_executor is not None:
        _compat_executor.shutdown(wait=False)
        _compat_executor = None
