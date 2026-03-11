"""Shared fixtures for KubeMQ performance benchmarks (SPEC-PERF-1)."""

from __future__ import annotations

import os

import pytest

KUBEMQ_ADDRESS = os.environ.get("KUBEMQ_BENCHMARK_ADDRESS", "localhost:50000")
BENCHMARK_PAYLOAD_1KB = b"x" * 1024
BENCHMARK_PAYLOAD_64B = b"x" * 64
BENCHMARK_PAYLOAD_64KB = b"x" * 65536


def requires_kubemq_server() -> None:
    """Skip if KUBEMQ_BENCHMARK_ADDRESS is not set or server is unreachable."""
    if not os.environ.get("KUBEMQ_BENCHMARK_ADDRESS"):
        pytest.skip(
            "Set KUBEMQ_BENCHMARK_ADDRESS to run benchmarks "
            "(e.g. KUBEMQ_BENCHMARK_ADDRESS=localhost:50000)"
        )


@pytest.fixture
def kubemq_address() -> str:
    requires_kubemq_server()
    return KUBEMQ_ADDRESS


@pytest.fixture
def payload_1kb() -> bytes:
    return BENCHMARK_PAYLOAD_1KB


@pytest.fixture
def payload_64b() -> bytes:
    return BENCHMARK_PAYLOAD_64B


@pytest.fixture
def payload_64kb() -> bytes:
    return BENCHMARK_PAYLOAD_64KB
