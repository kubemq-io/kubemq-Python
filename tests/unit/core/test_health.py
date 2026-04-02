"""Unit tests for kubemq.core.health module.

Tests for HealthStatus, HealthCheck, HealthReport, HealthChecker, and AsyncHealthChecker.
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from kubemq.core.health import (
    AsyncHealthChecker,
    HealthCheck,
    HealthChecker,
    HealthReport,
    HealthStatus,
)


class TestHealthStatus:
    """Tests for HealthStatus enum."""

    def test_healthy_value(self):
        """Test HEALTHY value."""
        assert HealthStatus.HEALTHY.value == "healthy"

    def test_degraded_value(self):
        """Test DEGRADED value."""
        assert HealthStatus.DEGRADED.value == "degraded"

    def test_unhealthy_value(self):
        """Test UNHEALTHY value."""
        assert HealthStatus.UNHEALTHY.value == "unhealthy"


class TestHealthCheck:
    """Tests for HealthCheck dataclass."""

    def test_default_values(self):
        """Test default values are set correctly."""
        check = HealthCheck(
            name="test",
            status=HealthStatus.HEALTHY,
        )

        assert check.name == "test"
        assert check.status == HealthStatus.HEALTHY
        assert check.message == ""
        assert check.duration_ms == 0.0
        assert check.details == {}

    def test_custom_values(self):
        """Test custom values are set correctly."""
        check = HealthCheck(
            name="connection",
            status=HealthStatus.UNHEALTHY,
            message="Connection failed",
            duration_ms=125.5,
            details={"error": "timeout"},
        )

        assert check.name == "connection"
        assert check.status == HealthStatus.UNHEALTHY
        assert check.message == "Connection failed"
        assert check.duration_ms == 125.5
        assert check.details == {"error": "timeout"}

    def test_health_check_equality(self):
        """Test health check equality."""
        check1 = HealthCheck(name="test", status=HealthStatus.HEALTHY)
        check2 = HealthCheck(name="test", status=HealthStatus.HEALTHY)

        assert check1 == check2


class TestHealthReport:
    """Tests for HealthReport dataclass."""

    def test_default_values(self):
        """Test default values are set correctly."""
        report = HealthReport(status=HealthStatus.HEALTHY)

        assert report.status == HealthStatus.HEALTHY
        assert report.checks == []
        assert report.timestamp > 0

    def test_custom_values(self):
        """Test custom values are set correctly."""
        checks = [
            HealthCheck(name="conn", status=HealthStatus.HEALTHY),
        ]
        report = HealthReport(
            status=HealthStatus.HEALTHY,
            checks=checks,
            timestamp=1234567890.0,
        )

        assert report.status == HealthStatus.HEALTHY
        assert len(report.checks) == 1
        assert report.timestamp == 1234567890.0

    def test_is_healthy_returns_true(self):
        """Test is_healthy returns True for HEALTHY status."""
        report = HealthReport(status=HealthStatus.HEALTHY)

        assert report.is_healthy() is True

    def test_is_healthy_returns_false_for_degraded(self):
        """Test is_healthy returns False for DEGRADED status."""
        report = HealthReport(status=HealthStatus.DEGRADED)

        assert report.is_healthy() is False

    def test_is_healthy_returns_false_for_unhealthy(self):
        """Test is_healthy returns False for UNHEALTHY status."""
        report = HealthReport(status=HealthStatus.UNHEALTHY)

        assert report.is_healthy() is False

    def test_is_unhealthy_returns_true(self):
        """Test is_unhealthy returns True for UNHEALTHY status."""
        report = HealthReport(status=HealthStatus.UNHEALTHY)

        assert report.is_unhealthy() is True

    def test_is_unhealthy_returns_false_for_healthy(self):
        """Test is_unhealthy returns False for HEALTHY status."""
        report = HealthReport(status=HealthStatus.HEALTHY)

        assert report.is_unhealthy() is False

    def test_is_unhealthy_returns_false_for_degraded(self):
        """Test is_unhealthy returns False for DEGRADED status."""
        report = HealthReport(status=HealthStatus.DEGRADED)

        assert report.is_unhealthy() is False


class TestHealthChecker:
    """Tests for HealthChecker class."""

    def test_init_stores_client(self):
        """Test initialization stores client."""
        mock_client = MagicMock()
        checker = HealthChecker(mock_client)

        assert checker._client is mock_client

    def test_check_healthy_connection(self):
        """Test check returns healthy report on successful ping."""
        mock_client = MagicMock()
        mock_server_info = MagicMock()
        mock_server_info.host = "localhost:50000"
        mock_server_info.version = "1.0.0"
        mock_server_info.server_up_time_seconds = 3600
        mock_client.ping.return_value = mock_server_info

        checker = HealthChecker(mock_client)
        report = checker.check()

        assert report.status == HealthStatus.HEALTHY
        assert len(report.checks) == 1
        assert report.checks[0].name == "connection"
        assert report.checks[0].status == HealthStatus.HEALTHY
        assert "Connected to localhost:50000" in report.checks[0].message

    def test_check_unhealthy_connection(self):
        """Test check returns unhealthy report on failed ping."""
        mock_client = MagicMock()
        mock_client.ping.side_effect = Exception("Connection refused")

        checker = HealthChecker(mock_client)
        report = checker.check()

        assert report.status == HealthStatus.UNHEALTHY
        assert len(report.checks) == 1
        assert report.checks[0].name == "connection"
        assert report.checks[0].status == HealthStatus.UNHEALTHY
        assert "Connection refused" in report.checks[0].message

    def test_check_measures_duration(self):
        """Test check measures duration."""
        mock_client = MagicMock()
        mock_server_info = MagicMock()
        mock_server_info.host = "localhost"
        mock_server_info.version = "1.0"
        mock_server_info.server_up_time_seconds = 0
        mock_client.ping.return_value = mock_server_info

        checker = HealthChecker(mock_client)
        report = checker.check()

        assert report.checks[0].duration_ms >= 0

    def test_check_includes_details_on_success(self):
        """Test check includes server details on success."""
        mock_client = MagicMock()
        mock_server_info = MagicMock()
        mock_server_info.host = "kubemq-server"
        mock_server_info.version = "2.0.0"
        mock_server_info.server_up_time_seconds = 7200
        mock_client.ping.return_value = mock_server_info

        checker = HealthChecker(mock_client)
        report = checker.check()

        details = report.checks[0].details
        assert details["host"] == "kubemq-server"
        assert details["version"] == "2.0.0"
        assert details["uptime_seconds"] == 7200

    def test_determine_overall_status_all_healthy(self):
        """Test determine_overall_status returns HEALTHY when all checks healthy."""
        checker = HealthChecker(MagicMock())
        checks = [
            HealthCheck(name="c1", status=HealthStatus.HEALTHY),
            HealthCheck(name="c2", status=HealthStatus.HEALTHY),
        ]

        status = checker._determine_overall_status(checks)

        assert status == HealthStatus.HEALTHY

    def test_determine_overall_status_any_unhealthy(self):
        """Test determine_overall_status returns UNHEALTHY when any check unhealthy."""
        checker = HealthChecker(MagicMock())
        checks = [
            HealthCheck(name="c1", status=HealthStatus.HEALTHY),
            HealthCheck(name="c2", status=HealthStatus.UNHEALTHY),
        ]

        status = checker._determine_overall_status(checks)

        assert status == HealthStatus.UNHEALTHY

    def test_determine_overall_status_degraded(self):
        """Test determine_overall_status returns DEGRADED for mixed non-unhealthy."""
        checker = HealthChecker(MagicMock())
        checks = [
            HealthCheck(name="c1", status=HealthStatus.HEALTHY),
            HealthCheck(name="c2", status=HealthStatus.DEGRADED),
        ]

        status = checker._determine_overall_status(checks)

        assert status == HealthStatus.DEGRADED


class TestAsyncHealthChecker:
    """Tests for AsyncHealthChecker class."""

    def test_init_stores_client(self):
        """Test initialization stores client."""
        mock_client = MagicMock()
        checker = AsyncHealthChecker(mock_client)

        assert checker._client is mock_client

    @pytest.mark.asyncio
    async def test_check_healthy_connection(self):
        """Test check returns healthy report on successful ping."""
        mock_client = MagicMock()
        mock_server_info = MagicMock()
        mock_server_info.host = "localhost:50000"
        mock_server_info.version = "1.0.0"
        mock_server_info.server_up_time_seconds = 3600

        async def mock_ping():
            return mock_server_info

        mock_client.ping = mock_ping

        checker = AsyncHealthChecker(mock_client)
        report = await checker.check()

        assert report.status == HealthStatus.HEALTHY
        assert len(report.checks) == 1
        assert report.checks[0].name == "connection"
        assert report.checks[0].status == HealthStatus.HEALTHY

    @pytest.mark.asyncio
    async def test_check_unhealthy_connection(self):
        """Test check returns unhealthy report on failed ping."""
        mock_client = MagicMock()

        async def mock_ping():
            raise Exception("Connection refused")

        mock_client.ping = mock_ping

        checker = AsyncHealthChecker(mock_client)
        report = await checker.check()

        assert report.status == HealthStatus.UNHEALTHY
        assert report.checks[0].status == HealthStatus.UNHEALTHY
        assert "Connection refused" in report.checks[0].message

    @pytest.mark.asyncio
    async def test_check_timeout(self):
        """Test check handles timeout."""
        mock_client = MagicMock()

        async def slow_ping():
            await asyncio.sleep(10)
            return MagicMock()

        mock_client.ping = slow_ping

        checker = AsyncHealthChecker(mock_client)
        report = await checker.check(timeout_seconds=0.01)

        assert report.status == HealthStatus.UNHEALTHY
        assert "timed out" in report.checks[0].message

    @pytest.mark.asyncio
    async def test_check_includes_details_on_success(self):
        """Test check includes server details on success."""
        mock_client = MagicMock()
        mock_server_info = MagicMock()
        mock_server_info.host = "async-server"
        mock_server_info.version = "3.0.0"
        mock_server_info.server_up_time_seconds = 1800

        async def mock_ping():
            return mock_server_info

        mock_client.ping = mock_ping

        checker = AsyncHealthChecker(mock_client)
        report = await checker.check()

        details = report.checks[0].details
        assert details["host"] == "async-server"
        assert details["version"] == "3.0.0"
        assert details["uptime_seconds"] == 1800

    def test_determine_overall_status_all_healthy(self):
        """Test determine_overall_status returns HEALTHY when all checks healthy."""
        checker = AsyncHealthChecker(MagicMock())
        checks = [
            HealthCheck(name="c1", status=HealthStatus.HEALTHY),
            HealthCheck(name="c2", status=HealthStatus.HEALTHY),
        ]

        status = checker._determine_overall_status(checks)

        assert status == HealthStatus.HEALTHY

    def test_determine_overall_status_any_unhealthy(self):
        """Test determine_overall_status returns UNHEALTHY when any check unhealthy."""
        checker = AsyncHealthChecker(MagicMock())
        checks = [
            HealthCheck(name="c1", status=HealthStatus.HEALTHY),
            HealthCheck(name="c2", status=HealthStatus.UNHEALTHY),
        ]

        status = checker._determine_overall_status(checks)

        assert status == HealthStatus.UNHEALTHY

    @pytest.mark.asyncio
    async def test_check_measures_duration(self):
        """Test check measures duration."""
        mock_client = MagicMock()
        mock_server_info = MagicMock()
        mock_server_info.host = "localhost"
        mock_server_info.version = "1.0"
        mock_server_info.server_up_time_seconds = 0

        async def mock_ping():
            return mock_server_info

        mock_client.ping = mock_ping

        checker = AsyncHealthChecker(mock_client)
        report = await checker.check()

        assert report.checks[0].duration_ms >= 0
