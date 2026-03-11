"""REQ-PERF-1: Queue roundtrip latency benchmark (1KB, p50/p99)."""

from __future__ import annotations

import statistics
import time
import uuid

import pytest

pytestmark = [pytest.mark.benchmark, pytest.mark.integration]


class TestQueueRoundtrip:
    """Measure queue send -> receive roundtrip latency."""

    def test_queue_roundtrip_latency_1kb(
        self, benchmark, kubemq_address: str, payload_1kb: bytes
    ):
        """Benchmark: queue send+receive roundtrip with 1KB payload."""
        from kubemq.queues import Client as QueuesClient, QueueMessage

        client = QueuesClient(address=kubemq_address)
        channel = f"bench-roundtrip-{uuid.uuid4().hex[:8]}"

        latencies: list[float] = []

        def roundtrip():
            msg = QueueMessage(channel=channel, body=payload_1kb)
            start = time.perf_counter()
            client.send_queue_message(msg)
            client.receive_queue_messages(
                channel=channel, max_messages=1, wait_timeout_seconds=5
            )
            latencies.append(time.perf_counter() - start)

        benchmark.pedantic(roundtrip, iterations=30, rounds=3, warmup_rounds=1)

        if latencies:
            sorted_lat = sorted(latencies)
            p50_idx = int(len(sorted_lat) * 0.50)
            p99_idx = int(len(sorted_lat) * 0.99)
            benchmark.extra_info["p50_ms"] = sorted_lat[p50_idx] * 1000
            benchmark.extra_info["p99_ms"] = sorted_lat[p99_idx] * 1000

        client.close()
