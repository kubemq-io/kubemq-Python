"""Baseline performance benchmark tests for comparing thread-wrapped vs native async.

This test file measures and compares the performance characteristics of:
1. Native async clients (AsyncPubSubClient, AsyncQueuesClient, AsyncCQClient)
2. Thread-wrapped async methods (Client.*_async())

These tests are marked as integration tests and require a running KubeMQ server.

Usage:
    # Run benchmarks only
    uv run pytest tests/benchmarks/ -v -m integration

    # Skip benchmarks in normal test runs
    uv run pytest tests/ -v --ignore=tests/benchmarks/
"""

from __future__ import annotations

import statistics
import time
from dataclasses import dataclass

import pytest

# Mark all tests in this module as integration tests
pytestmark = [pytest.mark.integration, pytest.mark.slow]


@dataclass
class BenchmarkResult:
    """Result from a benchmark run."""

    name: str
    count: int
    latencies: list[float]

    @property
    def mean_ms(self) -> float:
        """Mean latency in milliseconds."""
        return statistics.mean(self.latencies) * 1000

    @property
    def median_ms(self) -> float:
        """Median latency in milliseconds."""
        return statistics.median(self.latencies) * 1000

    @property
    def stdev_ms(self) -> float:
        """Standard deviation in milliseconds."""
        if len(self.latencies) < 2:
            return 0.0
        return statistics.stdev(self.latencies) * 1000

    @property
    def min_ms(self) -> float:
        """Minimum latency in milliseconds."""
        return min(self.latencies) * 1000

    @property
    def max_ms(self) -> float:
        """Maximum latency in milliseconds."""
        return max(self.latencies) * 1000

    @property
    def throughput(self) -> float:
        """Operations per second."""
        total_time = sum(self.latencies)
        if total_time == 0:
            return 0.0
        return len(self.latencies) / total_time

    def report(self) -> str:
        """Generate a report string."""
        return f"""
{self.name}:
  Count: {self.count}
  Mean:  {self.mean_ms:.3f} ms
  Median: {self.median_ms:.3f} ms
  Stdev: {self.stdev_ms:.3f} ms
  Min:   {self.min_ms:.3f} ms
  Max:   {self.max_ms:.3f} ms
  Throughput: {self.throughput:.1f} ops/sec
"""


def compare_results(native: BenchmarkResult, wrapped: BenchmarkResult) -> str:
    """Generate a comparison report."""
    improvement = (wrapped.mean_ms - native.mean_ms) / wrapped.mean_ms * 100
    throughput_improvement = (native.throughput - wrapped.throughput) / wrapped.throughput * 100

    return f"""
=== Comparison: {native.name} vs {wrapped.name} ===
Native async mean: {native.mean_ms:.3f} ms
Thread-wrapped mean: {wrapped.mean_ms:.3f} ms
Latency improvement: {improvement:.1f}% faster
Throughput improvement: {throughput_improvement:.1f}% higher
"""


class TestPubSubBenchmarks:
    """Benchmarks for PubSub operations."""

    @pytest.fixture
    def kubemq_address(self) -> str:
        """KubeMQ server address."""
        return "localhost:50000"

    @pytest.mark.asyncio
    async def test_native_async_event_send_latency(self, kubemq_address: str):
        """Benchmark native async event send latency."""
        pytest.skip("Requires running KubeMQ server")

        from kubemq.pubsub import AsyncClient as AsyncPubSubClient, EventMessage

        count = 100
        latencies = []

        async with AsyncPubSubClient(address=kubemq_address) as client:
            message = EventMessage(channel="bench-channel", body=b"benchmark data")

            for _ in range(count):
                start = time.perf_counter()
                await client.send_event(message)
                latencies.append(time.perf_counter() - start)

        result = BenchmarkResult(
            name="Native Async PubSub Send",
            count=count,
            latencies=latencies,
        )
        print(result.report())

    @pytest.mark.asyncio
    async def test_thread_wrapped_event_send_latency(self, kubemq_address: str):
        """Benchmark thread-wrapped async event send latency."""
        pytest.skip("Requires running KubeMQ server")

        from kubemq.pubsub import Client as SyncPubSubClient, EventMessage

        count = 100
        latencies = []

        with SyncPubSubClient(address=kubemq_address) as client:
            message = EventMessage(channel="bench-channel", body=b"benchmark data")

            for _ in range(count):
                start = time.perf_counter()
                await client.send_events_message_async(message)
                latencies.append(time.perf_counter() - start)

        result = BenchmarkResult(
            name="Thread-Wrapped PubSub Send",
            count=count,
            latencies=latencies,
        )
        print(result.report())


class TestQueuesBenchmarks:
    """Benchmarks for Queues operations."""

    @pytest.fixture
    def kubemq_address(self) -> str:
        """KubeMQ server address."""
        return "localhost:50000"

    @pytest.mark.asyncio
    async def test_native_async_queue_send_latency(self, kubemq_address: str):
        """Benchmark native async queue send latency."""
        pytest.skip("Requires running KubeMQ server")

        from kubemq.queues import AsyncClient as AsyncQueuesClient, QueueMessage

        count = 100
        latencies = []

        async with AsyncQueuesClient(address=kubemq_address) as client:
            message = QueueMessage(channel="bench-queue", body=b"benchmark data")

            for _ in range(count):
                start = time.perf_counter()
                await client.send_queue_message(message)
                latencies.append(time.perf_counter() - start)

        result = BenchmarkResult(
            name="Native Async Queues Send",
            count=count,
            latencies=latencies,
        )
        print(result.report())


class TestCQBenchmarks:
    """Benchmarks for CQ operations."""

    @pytest.fixture
    def kubemq_address(self) -> str:
        """KubeMQ server address."""
        return "localhost:50000"

    @pytest.mark.asyncio
    async def test_native_async_command_latency(self, kubemq_address: str):
        """Benchmark native async command latency."""
        pytest.skip("Requires running KubeMQ server")

        from kubemq.cq import AsyncClient as AsyncCQClient, CommandMessage

        count = 100
        latencies = []

        async with AsyncCQClient(address=kubemq_address) as client:
            message = CommandMessage(
                channel="bench-commands",
                body=b"benchmark data",
                timeout_in_seconds=10,
            )

            for _ in range(count):
                start = time.perf_counter()
                try:
                    await client.send_command(message)
                except Exception:
                    pass  # May timeout if no responder
                latencies.append(time.perf_counter() - start)

        result = BenchmarkResult(
            name="Native Async CQ Command",
            count=count,
            latencies=latencies,
        )
        print(result.report())


class TestConcurrencyBenchmarks:
    """Benchmarks for concurrent operations."""

    @pytest.fixture
    def kubemq_address(self) -> str:
        """KubeMQ server address."""
        return "localhost:50000"

    @pytest.mark.asyncio
    async def test_native_async_concurrent_sends(self, kubemq_address: str):
        """Benchmark concurrent event sends with native async."""
        pytest.skip("Requires running KubeMQ server")

        from kubemq.pubsub import AsyncClient as AsyncPubSubClient, EventMessage

        count = 1000
        concurrent = 100

        async with AsyncPubSubClient(address=kubemq_address) as client:
            messages = [
                EventMessage(channel="bench-channel", body=f"msg-{i}".encode())
                for i in range(count)
            ]

            start = time.perf_counter()
            results = await client.send_events_batch(messages, max_concurrent=concurrent)
            elapsed = time.perf_counter() - start

            success_count = sum(1 for r in results if r.sent)
            throughput = count / elapsed

            print(f"""
Native Async Concurrent Sends:
  Total messages: {count}
  Concurrency: {concurrent}
  Success: {success_count}
  Total time: {elapsed:.3f} s
  Throughput: {throughput:.1f} msgs/sec
""")


class TestMemoryBenchmarks:
    """Benchmarks for memory usage."""

    @pytest.mark.asyncio
    async def test_native_async_memory_baseline(self):
        """Measure baseline memory for native async client."""
        pytest.skip("Requires running KubeMQ server and memory profiling")

        import tracemalloc

        from kubemq.pubsub import AsyncClient as AsyncPubSubClient

        tracemalloc.start()

        clients = []
        for _ in range(10):
            client = AsyncPubSubClient(address="localhost:50000")
            clients.append(client)

        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        print(f"""
Native Async Memory (10 clients):
  Current: {current / 1024:.1f} KB
  Peak: {peak / 1024:.1f} KB
  Per client: {current / 10 / 1024:.1f} KB
""")

    @pytest.mark.asyncio
    async def test_thread_wrapped_memory_baseline(self):
        """Measure baseline memory for thread-wrapped client."""
        pytest.skip("Requires running KubeMQ server and memory profiling")

        import tracemalloc

        from kubemq.pubsub import Client as SyncPubSubClient

        tracemalloc.start()

        clients = []
        for _ in range(10):
            client = SyncPubSubClient(address="localhost:50000")
            clients.append(client)

        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        print(f"""
Thread-Wrapped Memory (10 clients):
  Current: {current / 1024:.1f} KB
  Peak: {peak / 1024:.1f} KB
  Per client: {current / 10 / 1024:.1f} KB
""")
