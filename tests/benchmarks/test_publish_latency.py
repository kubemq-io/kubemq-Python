"""REQ-PERF-1: Publish latency benchmark (1KB, p50/p99)."""

from __future__ import annotations

import statistics
import time

import pytest

pytestmark = [pytest.mark.benchmark, pytest.mark.integration]


class TestPublishLatency:
    """Measure publish latency percentiles for event store (confirmed delivery)."""

    def test_event_store_latency_1kb(
        self, benchmark, kubemq_address: str, payload_1kb: bytes
    ):
        """Benchmark: event store send latency with 1KB payload (p50/p99)."""
        from kubemq.pubsub import Client as PubSubClient, EventStoreMessage

        client = PubSubClient(address=kubemq_address)
        msg = EventStoreMessage(channel="bench-latency", body=payload_1kb)

        latencies: list[float] = []

        def send_and_record():
            start = time.perf_counter()
            client.send_event_store(msg)
            latencies.append(time.perf_counter() - start)

        benchmark.pedantic(send_and_record, iterations=50, rounds=3, warmup_rounds=1)

        if latencies:
            sorted_lat = sorted(latencies)
            p50_idx = int(len(sorted_lat) * 0.50)
            p99_idx = int(len(sorted_lat) * 0.99)
            benchmark.extra_info["p50_ms"] = sorted_lat[p50_idx] * 1000
            benchmark.extra_info["p99_ms"] = sorted_lat[p99_idx] * 1000
            benchmark.extra_info["mean_ms"] = statistics.mean(latencies) * 1000

        client.close()
