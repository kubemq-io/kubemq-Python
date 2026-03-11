"""REQ-PERF-1: Connection setup time benchmark."""

from __future__ import annotations

import asyncio
import time

import pytest

pytestmark = [pytest.mark.benchmark, pytest.mark.integration]


class TestConnectionSetup:
    """Measure time from client construction to first successful operation."""

    def test_sync_connection_setup_time(self, benchmark, kubemq_address: str):
        """Benchmark: sync client connection setup (time to first ping)."""

        def connect_and_ping():
            from kubemq.pubsub import Client as PubSubClient

            client = PubSubClient(address=kubemq_address)
            client.ping()
            client.close()

        benchmark.pedantic(connect_and_ping, iterations=10, rounds=3, warmup_rounds=1)

    def test_async_connection_setup_time(self, benchmark, kubemq_address: str):
        """Benchmark: async client connection setup (sync wrapper)."""
        from kubemq.pubsub import AsyncClient as AsyncPubSubClient

        async def _connect_once():
            async with AsyncPubSubClient(address=kubemq_address) as client:
                await client.ping()

        benchmark(lambda: asyncio.run(_connect_once()))
