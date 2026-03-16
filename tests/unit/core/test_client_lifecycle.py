"""Unit tests for client lifecycle edge cases.

Verifies:
- Operations on closed client raise KubeMQClientClosedError
- Resource leak detection (unclosed coroutines)
- Empty/nil payloads handled correctly
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from kubemq.core.exceptions import KubeMQClientClosedError, KubeMQValidationError


class TestClosedClientOperations:
    """Operations on a closed client must raise KubeMQClientClosedError."""

    async def test_send_event_after_close_raises(self) -> None:
        from kubemq.pubsub import AsyncClient, EventMessage

        client = AsyncClient(address="localhost:50000", client_id="test-closed")
        transport = MagicMock()
        transport.is_connected = True
        transport.close = AsyncMock()
        client._transport = transport

        await client.close()
        assert client._closed is True

        with pytest.raises(KubeMQClientClosedError):
            await client.send_event(EventMessage(channel="ch", body=b"data"))

    async def test_ping_after_close_raises(self) -> None:
        from kubemq.pubsub import AsyncClient

        client = AsyncClient(address="localhost:50000", client_id="test-closed-ping")
        transport = MagicMock()
        transport.is_connected = True
        transport.close = AsyncMock()
        client._transport = transport

        await client.close()

        with pytest.raises(KubeMQClientClosedError):
            await client.ping()

    def test_sync_client_send_after_close_raises(self) -> None:
        from kubemq.pubsub import Client, EventMessage

        client = Client(address="localhost:50000", client_id="test-closed-sync")
        transport = MagicMock()
        transport.is_connected.return_value = True
        transport.close = MagicMock()
        client._transport = transport

        client.close()
        assert client._closed is True

        with pytest.raises(KubeMQClientClosedError):
            client.send_events_message(EventMessage(channel="ch", body=b"data"))

    async def test_close_is_idempotent(self) -> None:
        from kubemq.pubsub import AsyncClient

        client = AsyncClient(address="localhost:50000", client_id="test-idempotent")
        transport = MagicMock()
        transport.is_connected = True
        transport.close = AsyncMock()
        client._transport = transport

        await client.close()
        await client.close()
        assert client._closed is True


class TestOversizedMessages:
    """Oversized messages exceeding max_send_size produce a validation error."""

    def test_oversized_body_raises_validation_error(self) -> None:
        from kubemq.pubsub import Client as PubSubClient, EventMessage

        client = PubSubClient(
            address="localhost:50000",
            client_id="test-oversize",
            max_send_size=1024,
        )
        transport = MagicMock()
        transport.is_connected = True
        client._transport = transport

        oversized_msg = EventMessage(channel="ch", body=b"x" * 1025)

        with pytest.raises(KubeMQValidationError, match="exceeds maximum"):
            client.send_events_message(oversized_msg)

    def test_message_within_limit_not_rejected(self) -> None:
        from kubemq.pubsub import Client as PubSubClient

        client = PubSubClient(
            address="localhost:50000",
            client_id="test-ok-size",
            max_send_size=2048,
        )
        client._validate_message_size(b"x" * 100)

    def test_queue_message_oversized_raises(self) -> None:
        from kubemq.queues import Client as QueuesClient

        client = QueuesClient(
            address="localhost:50000",
            client_id="test-oversize-q",
            max_send_size=512,
        )
        client._validate_message_size(b"x" * 100)

        with pytest.raises(KubeMQValidationError, match="exceeds maximum"):
            client._validate_message_size(b"x" * 600)


class TestEmptyPayloads:
    """Empty/nil payloads must be handled without crashes."""

    def test_empty_body_event_message_with_metadata(self) -> None:
        from kubemq.pubsub import EventMessage

        msg = EventMessage(channel="ch", body=b"", metadata="has-metadata")
        assert msg.body == b""
        assert msg.metadata == "has-metadata"

    def test_none_metadata_defaults(self) -> None:
        from kubemq.pubsub import EventMessage

        msg = EventMessage(channel="ch", body=b"data")
        assert msg.metadata is None

    def test_event_message_requires_channel(self) -> None:
        from pydantic import ValidationError

        from kubemq.pubsub import EventMessage

        with pytest.raises(ValidationError):
            EventMessage(channel="", body=b"data")

    def test_queue_message_empty_body_with_metadata(self) -> None:
        from kubemq.queues import QueueMessage

        msg = QueueMessage(channel="ch", body=b"", metadata="m")
        assert msg.body == b""

    def test_command_message_with_body(self) -> None:
        from kubemq.cq import CommandMessage

        msg = CommandMessage(channel="ch", body=b"cmd", timeout_in_seconds=10)
        assert msg.body == b"cmd"


class TestResourceLeakDetection:
    """Verify client close cleans up async resources."""

    async def test_close_cleans_up_tasks(self) -> None:
        tasks_before = len(asyncio.all_tasks())

        from kubemq.pubsub import AsyncClient

        client = AsyncClient(address="localhost:50000", client_id="test-leak")
        transport = MagicMock()
        transport.is_connected = True
        transport.close = AsyncMock()
        client._transport = transport

        await client.close()

        tasks_after = len(asyncio.all_tasks())
        assert tasks_after <= tasks_before

    async def test_queues_close_cleans_up(self) -> None:
        tasks_before = len(asyncio.all_tasks())

        from kubemq.queues import AsyncClient

        client = AsyncClient(address="localhost:50000", client_id="test-leak-queues")
        transport = MagicMock()
        transport.is_connected = True
        transport.close = AsyncMock()
        client._transport = transport

        await client.close()

        tasks_after = len(asyncio.all_tasks())
        assert tasks_after <= tasks_before
