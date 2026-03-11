"""Sync integration tests for Commands/Queries client.

Requires a running KubeMQ server. Run with:
    pytest tests/integration/test_sync_cq.py -v -m integration
"""

from __future__ import annotations

import threading
import time
import uuid

import pytest

from kubemq.common.cancellation_token import CancellationToken
from kubemq.cq import (
    Client as CQClient,
    CommandMessage,
    CommandResponseMessage,
    CommandsSubscription,
    QueriesSubscription,
    QueryMessage,
    QueryResponseMessage,
)

pytestmark = [pytest.mark.integration, pytest.mark.timeout(60)]


class TestSyncCQConnection:
    """Sync CQ connection integration tests."""

    def test_connect_and_ping(self, sync_cq_client: CQClient) -> None:
        server_info = sync_cq_client.ping()
        assert server_info is not None
        assert server_info.host is not None


class TestSyncCQCommands:
    """Sync Commands round-trip integration tests."""

    def test_command_roundtrip(self, sync_cq_client: CQClient) -> None:
        channel = f"test-cmd-sync-{uuid.uuid4().hex[:8]}"
        received = threading.Event()
        cancel = CancellationToken()

        def on_command(cmd):
            sync_cq_client.send_response_message(
                CommandResponseMessage(command_received=cmd, is_executed=True)
            )
            received.set()

        def on_error(err):
            pass

        sub_thread = threading.Thread(
            target=sync_cq_client.subscribe_to_commands,
            args=(
                CommandsSubscription(
                    channel=channel,
                    group="test-group",
                    on_receive_command_callback=on_command,
                    on_error_callback=on_error,
                ),
                cancel,
            ),
            daemon=True,
        )
        sub_thread.start()
        time.sleep(1)

        result = sync_cq_client.send_command(
            CommandMessage(
                channel=channel,
                body=b"cmd-payload",
                timeout_in_seconds=10,
            )
        )
        assert result.is_executed is True
        assert received.wait(timeout=10)

        cancel.cancel()
        sub_thread.join(timeout=5)


class TestSyncCQQueries:
    """Sync Queries round-trip integration tests."""

    def test_query_roundtrip(self, sync_cq_client: CQClient) -> None:
        channel = f"test-query-sync-{uuid.uuid4().hex[:8]}"
        received = threading.Event()
        cancel = CancellationToken()

        def on_query(query):
            sync_cq_client.send_response_message(
                QueryResponseMessage(
                    query_received=query,
                    is_executed=True,
                    body=b"query-response",
                )
            )
            received.set()

        def on_error(err):
            pass

        sub_thread = threading.Thread(
            target=sync_cq_client.subscribe_to_queries,
            args=(
                QueriesSubscription(
                    channel=channel,
                    group="test-group",
                    on_receive_query_callback=on_query,
                    on_error_callback=on_error,
                ),
                cancel,
            ),
            daemon=True,
        )
        sub_thread.start()
        time.sleep(1)

        result = sync_cq_client.send_query(
            QueryMessage(
                channel=channel,
                body=b"query-payload",
                timeout_in_seconds=10,
            )
        )
        assert result.is_executed is True
        assert result.body == b"query-response"
        assert received.wait(timeout=10)

        cancel.cancel()
        sub_thread.join(timeout=5)
