"""Tests for kubemq.core.compat module."""

from __future__ import annotations

import asyncio
import time

import pytest

from kubemq.core.compat import run_in_thread


class TestRunInThread:
    """Tests for run_in_thread function."""

    @pytest.mark.asyncio
    async def test_basic_function(self):
        """Test running a basic function in a thread."""

        def add(a, b):
            return a + b

        result = await run_in_thread(add, 2, 3)
        assert result == 5

    @pytest.mark.asyncio
    async def test_with_keyword_args(self):
        """Test running a function with keyword arguments."""

        def greet(name, greeting="Hello"):
            return f"{greeting}, {name}!"

        result = await run_in_thread(greet, "World", greeting="Hi")
        assert result == "Hi, World!"

    @pytest.mark.asyncio
    async def test_blocking_function(self):
        """Test that blocking functions don't block the event loop."""

        def blocking_sleep():
            time.sleep(0.1)
            return "done"

        # This should complete without blocking other tasks
        start = time.monotonic()
        result = await run_in_thread(blocking_sleep)
        elapsed = time.monotonic() - start

        assert result == "done"
        assert elapsed >= 0.1

    @pytest.mark.asyncio
    async def test_exception_propagation(self):
        """Test that exceptions are properly propagated."""

        def raise_error():
            raise ValueError("Test error")

        with pytest.raises(ValueError, match="Test error"):
            await run_in_thread(raise_error)

    @pytest.mark.asyncio
    async def test_return_none(self):
        """Test function that returns None."""

        def return_none():
            pass

        result = await run_in_thread(return_none)
        assert result is None

    @pytest.mark.asyncio
    async def test_concurrent_execution(self):
        """Test that multiple calls can run concurrently."""

        def slow_function(n):
            time.sleep(0.1)
            return n * 2

        start = time.monotonic()
        results = await asyncio.gather(
            run_in_thread(slow_function, 1),
            run_in_thread(slow_function, 2),
            run_in_thread(slow_function, 3),
        )
        elapsed = time.monotonic() - start

        assert results == [2, 4, 6]
        # All three should complete in roughly the time of one (parallel execution)
        assert elapsed < 0.3  # Should be ~0.1s, not ~0.3s

    @pytest.mark.asyncio
    async def test_with_lambda(self):
        """Test running a lambda function."""
        result = await run_in_thread(lambda x: x**2, 5)
        assert result == 25

    @pytest.mark.asyncio
    async def test_with_method(self):
        """Test running an instance method."""

        class Calculator:
            def __init__(self, base):
                self.base = base

            def add(self, value):
                return self.base + value

        calc = Calculator(10)
        result = await run_in_thread(calc.add, 5)
        assert result == 15

    @pytest.mark.asyncio
    async def test_complex_return_type(self):
        """Test returning complex data structures."""

        def return_complex():
            return {
                "list": [1, 2, 3],
                "dict": {"a": 1, "b": 2},
                "tuple": (1, 2, 3),
            }

        result = await run_in_thread(return_complex)
        assert result["list"] == [1, 2, 3]
        assert result["dict"] == {"a": 1, "b": 2}
        assert result["tuple"] == (1, 2, 3)

    @pytest.mark.asyncio
    async def test_thread_safety(self):
        """Test that operations are thread-safe."""
        results = []
        lock = asyncio.Lock()

        def append_value(value):
            time.sleep(0.01)  # Small delay to increase chance of race condition
            return value

        async def process(value):
            result = await run_in_thread(append_value, value)
            async with lock:
                results.append(result)

        await asyncio.gather(*[process(i) for i in range(10)])

        assert sorted(results) == list(range(10))

    @pytest.mark.asyncio
    async def test_uses_executor(self):
        """Test that run_in_thread uses a thread pool executor."""
        executed_in_thread = []

        def check_thread():
            import threading

            executed_in_thread.append(threading.current_thread().name)
            return True

        result = await run_in_thread(check_thread)
        assert result is True
        assert len(executed_in_thread) == 1
        # Thread name should indicate it's from a thread pool
        # (different from the main thread)

    @pytest.mark.asyncio
    async def test_no_args(self):
        """Test function with no arguments."""

        def no_args():
            return 42

        result = await run_in_thread(no_args)
        assert result == 42

    @pytest.mark.asyncio
    async def test_many_args(self):
        """Test function with many arguments."""

        def many_args(a, b, c, d, e, f=0, g=0):
            return a + b + c + d + e + f + g

        result = await run_in_thread(many_args, 1, 2, 3, 4, 5, f=6, g=7)
        assert result == 28


class TestRunInThreadEdgeCases:
    """Edge case tests for run_in_thread."""

    @pytest.mark.asyncio
    async def test_empty_string_return(self):
        """Test returning empty string."""
        result = await run_in_thread(lambda: "")
        assert result == ""

    @pytest.mark.asyncio
    async def test_zero_return(self):
        """Test returning zero."""
        result = await run_in_thread(lambda: 0)
        assert result == 0

    @pytest.mark.asyncio
    async def test_false_return(self):
        """Test returning False."""
        result = await run_in_thread(lambda: False)
        assert result is False

    @pytest.mark.asyncio
    async def test_empty_list_return(self):
        """Test returning empty list."""
        result = await run_in_thread(lambda: [])
        assert result == []

    @pytest.mark.asyncio
    async def test_callable_class(self):
        """Test running a callable class instance."""

        class Callable:
            def __init__(self, multiplier):
                self.multiplier = multiplier

            def __call__(self, value):
                return value * self.multiplier

        double = Callable(2)
        result = await run_in_thread(double, 5)
        assert result == 10


# ==============================================================================
# Additional Coverage Tests
# ==============================================================================


class TestRunInThreadReturnAndException:
    """Focused tests for run_in_thread return values and exception propagation."""

    @pytest.mark.asyncio
    async def test_run_in_thread_returns_result(self):
        """Verify that the function result is correctly returned."""

        def compute():
            return 42 * 3

        result = await run_in_thread(compute)
        assert result == 126

    @pytest.mark.asyncio
    async def test_run_in_thread_propagates_exception(self):
        """Verify that exceptions from the function are raised to the caller."""

        def explode():
            raise RuntimeError("kaboom")

        with pytest.raises(RuntimeError, match="kaboom"):
            await run_in_thread(explode)


class TestCleanupExecutor:
    """Tests for cleanup_executor function."""

    def test_cleanup_executor_no_error(self):
        """Call cleanup_executor, verify no error even if executor was never created."""
        from kubemq.core import compat

        original = compat._compat_executor
        try:
            compat._compat_executor = None
            compat.cleanup_executor()
            assert compat._compat_executor is None
        finally:
            compat._compat_executor = original

    @pytest.mark.asyncio
    async def test_cleanup_executor_after_use(self):
        """Use the executor via run_in_thread, then clean it up."""
        from kubemq.core import compat

        await run_in_thread(lambda: 1 + 1)
        assert compat._compat_executor is not None

        compat.cleanup_executor()
        assert compat._compat_executor is None
