"""SPEC-PERF-3: Unit tests for bounded send queue behavior."""

from __future__ import annotations

import queue

import pytest


class TestBoundedSendQueue:
    def test_queue_rejects_when_full(self):
        """Verify bounded queue raises queue.Full when at capacity."""
        q: queue.Queue[str] = queue.Queue(maxsize=2)
        q.put("a")
        q.put("b")
        with pytest.raises(queue.Full):
            q.put_nowait("c")

    def test_default_queue_size_is_reasonable(self):
        """Default should be large enough for burst but not unbounded."""
        from kubemq.pubsub.event_sender import DEFAULT_SEND_QUEUE_SIZE

        assert 1_000 <= DEFAULT_SEND_QUEUE_SIZE <= 100_000

    def test_upstream_default_queue_size_is_reasonable(self):
        from kubemq.queues.upstream_sender import DEFAULT_SEND_QUEUE_SIZE

        assert 1_000 <= DEFAULT_SEND_QUEUE_SIZE <= 100_000

    def test_downstream_default_queue_size_is_reasonable(self):
        from kubemq.queues.downstream_receiver import DEFAULT_RECEIVE_QUEUE_SIZE

        assert 1_000 <= DEFAULT_RECEIVE_QUEUE_SIZE <= 100_000

    def test_client_config_has_max_send_queue_size(self):
        """ClientConfig exposes the max_send_queue_size knob."""
        from kubemq.core.config import ClientConfig

        cfg = ClientConfig()
        assert cfg.max_send_queue_size == 10_000

        cfg2 = ClientConfig(max_send_queue_size=500)
        assert cfg2.max_send_queue_size == 500

    def test_buffer_full_error_available(self):
        """KubeMQBufferFullError is importable and carries buffer_size."""
        from kubemq.core.exceptions import KubeMQBufferFullError

        err = KubeMQBufferFullError("full", buffer_size=100)
        assert err.buffer_size == 100
        assert "full" in str(err)
