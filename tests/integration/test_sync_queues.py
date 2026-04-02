"""Sync integration tests for Queues client.

Requires a running KubeMQ server. Run with:
    pytest tests/integration/test_sync_queues.py -v -m integration
"""

from __future__ import annotations

import uuid

import pytest

from kubemq.queues import Client as QueuesClient, QueueMessage

pytestmark = [pytest.mark.integration, pytest.mark.timeout(60)]


class TestSyncQueuesConnection:
    """Sync Queues connection integration tests."""

    def test_connect_and_ping(self, sync_queues_client: QueuesClient) -> None:
        server_info = sync_queues_client.ping()
        assert server_info is not None
        assert server_info.host is not None


class TestSyncQueuesSend:
    """Sync Queues send integration tests."""

    def test_send_single_message(self, sync_queues_client: QueuesClient) -> None:
        channel = f"test-queues-sync-{uuid.uuid4().hex[:8]}"
        result = sync_queues_client.send_queue_message(
            QueueMessage(channel=channel, body=b"queue-test-payload")
        )
        assert result.is_error is False

    def test_send_multiple_messages(self, sync_queues_client: QueuesClient) -> None:
        channel = f"test-queues-sync-multi-{uuid.uuid4().hex[:8]}"
        for i in range(5):
            result = sync_queues_client.send_queue_message(
                QueueMessage(channel=channel, body=f"message {i}".encode())
            )
            assert result.is_error is False


class TestSyncQueuesReceive:
    """Sync Queues receive integration tests."""

    def test_send_and_receive_queue_message(self, sync_queues_client: QueuesClient) -> None:
        channel = f"test-queues-sync-recv-{uuid.uuid4().hex[:8]}"

        sync_queues_client.send_queue_message(
            QueueMessage(channel=channel, body=b"queue-recv-payload")
        )

        response = sync_queues_client.receive_queue_messages(
            channel=channel,
            max_messages=1,
            wait_timeout_in_seconds=10,
            auto_ack=True,
        )
        assert response.is_error is False
        assert len(response.messages) > 0
        assert response.messages[0].body == b"queue-recv-payload"
