"""Sync integration tests for PubSub client.

Requires a running KubeMQ server. Run with:
    pytest tests/integration/test_sync_pubsub.py -v -m integration
"""

from __future__ import annotations

import threading
import time
import uuid

import pytest

from kubemq.common.cancellation_token import CancellationToken
from kubemq.pubsub import Client as PubSubClient, EventMessage, EventsSubscription

pytestmark = [pytest.mark.integration, pytest.mark.timeout(60)]


class TestSyncPubSubConnection:
    """Sync PubSub connection integration tests."""

    def test_connect_and_ping(self, sync_pubsub_client: PubSubClient) -> None:
        server_info = sync_pubsub_client.ping()
        assert server_info is not None
        assert server_info.host is not None


class TestSyncPubSubSend:
    """Sync PubSub send integration tests."""

    def test_send_single_event(self, sync_pubsub_client: PubSubClient) -> None:
        channel = f"test-events-sync-{uuid.uuid4().hex[:8]}"
        message = EventMessage(
            channel=channel,
            body=b"sync-test-payload",
            metadata="test",
        )
        sync_pubsub_client.send_events_message(message)

    def test_send_multiple_events(self, sync_pubsub_client: PubSubClient) -> None:
        channel = f"test-events-sync-multi-{uuid.uuid4().hex[:8]}"
        for i in range(5):
            sync_pubsub_client.send_events_message(
                EventMessage(channel=channel, body=f"message {i}".encode())
            )


class TestSyncPubSubSubscription:
    """Sync PubSub subscribe integration tests."""

    def test_send_and_receive_event(self, sync_pubsub_client: PubSubClient) -> None:
        channel = f"test-events-sync-sub-{uuid.uuid4().hex[:8]}"
        received = []
        event = threading.Event()
        cancel = CancellationToken()

        def on_event(msg):
            received.append(msg)
            event.set()

        def on_error(err):
            pass

        sub_thread = threading.Thread(
            target=sync_pubsub_client.subscribe_to_events,
            args=(
                EventsSubscription(
                    channel=channel,
                    group="test-group",
                    on_receive_event_callback=on_event,
                    on_error_callback=on_error,
                ),
                cancel,
            ),
            daemon=True,
        )
        sub_thread.start()
        time.sleep(1)

        sync_pubsub_client.send_events_message(
            EventMessage(channel=channel, body=b"sync-sub-payload", metadata="test")
        )

        assert event.wait(timeout=10), "Timed out waiting for event"
        assert len(received) >= 1
        assert received[0].body == b"sync-sub-payload"

        cancel.cancel()
        sub_thread.join(timeout=5)
