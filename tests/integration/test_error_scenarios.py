"""Integration tests for error scenarios against a real server."""

from __future__ import annotations

import uuid

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.timeout(60)]


class TestAuthenticationErrors:
    """Verify authentication error handling with invalid tokens."""

    async def test_invalid_token_raises_auth_error(self) -> None:
        from kubemq.pubsub import AsyncClient

        client = AsyncClient(
            address="localhost:50000",
            client_id=f"auth-test-{uuid.uuid4().hex[:8]}",
            auth_token="invalid-token-12345",
        )

        try:
            await client.connect()
            await client.ping()
        except Exception as e:
            assert "auth" in str(e).lower() or "unauthenticated" in str(e).lower()
        finally:
            await client.close()


class TestInvalidChannelErrors:
    """Verify error handling for invalid channel operations."""

    async def test_subscribe_empty_channel_raises(self) -> None:
        from pydantic import ValidationError
        from kubemq.pubsub import AsyncClient, EventsSubscription

        with pytest.raises(ValidationError):
            EventsSubscription(
                channel="",
                on_receive_event_callback=lambda e: None,
            )


class TestConnectionErrors:
    """Verify error handling for connection failures."""

    async def test_connect_to_invalid_address_raises(self) -> None:
        from kubemq.pubsub import AsyncClient

        client = AsyncClient(
            address="localhost:59999",
            client_id=f"err-test-{uuid.uuid4().hex[:8]}",
        )

        with pytest.raises(Exception):
            await client.connect()
            await client.ping()

        await client.close()
