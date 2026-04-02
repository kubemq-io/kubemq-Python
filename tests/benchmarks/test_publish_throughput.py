"""REQ-PERF-1: Publish throughput benchmark (1KB, msgs/sec)."""

from __future__ import annotations

import asyncio

import pytest

pytestmark = [pytest.mark.benchmark, pytest.mark.integration]


class TestPublishThroughput:
    """Measure event publish throughput (fire-and-forget)."""

    def test_sync_publish_throughput_1kb(self, benchmark, kubemq_address: str, payload_1kb: bytes):
        """Benchmark: sync publish throughput with 1KB payload."""
        from kubemq.pubsub import Client as PubSubClient, EventMessage

        client = PubSubClient(address=kubemq_address)
        msg = EventMessage(channel="bench-throughput", body=payload_1kb)

        def publish():
            client.send_event(msg)

        benchmark.pedantic(publish, iterations=100, rounds=5, warmup_rounds=1)
        client.close()

    def test_async_publish_throughput_1kb(self, benchmark, kubemq_address: str, payload_1kb: bytes):
        """Benchmark: async publish throughput with 1KB payload (sync wrapper)."""
        from kubemq.pubsub import AsyncClient as AsyncPubSubClient, EventMessage

        async def _run_one():
            async with AsyncPubSubClient(address=kubemq_address) as client:
                msg = EventMessage(channel="bench-throughput-async", body=payload_1kb)
                await client.send_event(msg)

        benchmark(lambda: asyncio.run(_run_one()))
