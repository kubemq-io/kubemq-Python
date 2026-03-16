"""Health checking utilities for KubeMQ Python SDK."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from kubemq.core.client import AsyncBaseClient, BaseClient, NativeAsyncBaseClient


class HealthStatus(Enum):
    """Health status enumeration."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


@dataclass
class HealthCheck:
    """Individual health check result."""

    name: str
    status: HealthStatus
    message: str = ""
    duration_ms: float = 0.0
    details: dict[str, object] = field(default_factory=dict)


@dataclass
class HealthReport:
    """Complete health report for a client."""

    status: HealthStatus
    checks: list[HealthCheck] = field(default_factory=list)
    timestamp: float = field(default_factory=time.time)

    def is_healthy(self) -> bool:
        """Check if overall status is healthy."""
        return self.status == HealthStatus.HEALTHY

    def is_unhealthy(self) -> bool:
        """Check if overall status is unhealthy."""
        return self.status == HealthStatus.UNHEALTHY


class HealthChecker:
    """Synchronous health checker for KubeMQ clients."""

    def __init__(self, client: BaseClient) -> None:
        """Initialize the health checker.

        Args:
            client: The KubeMQ client to check.
        """
        self._client = client

    def check(self, timeout_seconds: float = 5.0) -> HealthReport:
        """Perform a health check.

        Args:
            timeout_seconds: Maximum time to wait for health check.

        Returns:
            HealthReport with check results.
        """
        checks = []

        # Connection check
        conn_check = self._check_connection(timeout_seconds)
        checks.append(conn_check)

        # Determine overall status
        status = self._determine_overall_status(checks)

        return HealthReport(
            status=status,
            checks=checks,
            timestamp=time.time(),
        )

    def _check_connection(self, timeout: float) -> HealthCheck:
        """Check connection to KubeMQ server."""
        start = time.monotonic()
        try:
            server_info = self._client.ping()
            duration_ms = (time.monotonic() - start) * 1000

            return HealthCheck(
                name="connection",
                status=HealthStatus.HEALTHY,
                message=f"Connected to {server_info.host}",
                duration_ms=duration_ms,
                details={
                    "host": server_info.host,
                    "version": server_info.version,
                    "uptime_seconds": server_info.server_up_time_seconds,
                },
            )
        except Exception as e:
            duration_ms = (time.monotonic() - start) * 1000
            return HealthCheck(
                name="connection",
                status=HealthStatus.UNHEALTHY,
                message=f"Connection failed: {e}",
                duration_ms=duration_ms,
            )

    def _determine_overall_status(self, checks: list[HealthCheck]) -> HealthStatus:
        """Determine overall health status from individual checks."""
        if all(c.status == HealthStatus.HEALTHY for c in checks):
            return HealthStatus.HEALTHY
        elif any(c.status == HealthStatus.UNHEALTHY for c in checks):
            return HealthStatus.UNHEALTHY
        else:
            return HealthStatus.DEGRADED


class AsyncHealthChecker:
    """Async health checker for KubeMQ clients.

    Works with both NativeAsyncBaseClient (Phase 4 native async)
    and AsyncBaseClient (Phase 3 thread-wrapped async).

    Example:
        async with AsyncPubSubClient(address="localhost:50000") as client:
            checker = AsyncHealthChecker(client)
            report = await checker.check()
            if report.is_healthy():
                print("Client is healthy")
    """

    def __init__(
        self,
        client: NativeAsyncBaseClient | AsyncBaseClient,
    ) -> None:
        """Initialize the async health checker.

        Args:
            client: The async KubeMQ client to check.
        """
        self._client = client

    async def check(self, timeout_seconds: float = 5.0) -> HealthReport:
        """Perform an async health check.

        Args:
            timeout_seconds: Maximum time to wait for health check.

        Returns:
            HealthReport with check results.
        """
        checks = []

        # Connection check
        conn_check = await self._check_connection(timeout_seconds)
        checks.append(conn_check)

        # Determine overall status
        status = self._determine_overall_status(checks)

        return HealthReport(
            status=status,
            checks=checks,
            timestamp=time.time(),
        )

    async def _check_connection(self, timeout: float) -> HealthCheck:
        """Check connection to KubeMQ server."""
        start = time.monotonic()
        try:
            server_info = await asyncio.wait_for(
                self._client.ping(),
                timeout=timeout,
            )
            duration_ms = (time.monotonic() - start) * 1000

            return HealthCheck(
                name="connection",
                status=HealthStatus.HEALTHY,
                message=f"Connected to {server_info.host}",
                duration_ms=duration_ms,
                details={
                    "host": server_info.host,
                    "version": server_info.version,
                    "uptime_seconds": server_info.server_up_time_seconds,
                },
            )
        except TimeoutError:
            duration_ms = (time.monotonic() - start) * 1000
            return HealthCheck(
                name="connection",
                status=HealthStatus.UNHEALTHY,
                message=f"Connection timed out after {timeout}s",
                duration_ms=duration_ms,
            )
        except Exception as e:
            duration_ms = (time.monotonic() - start) * 1000
            return HealthCheck(
                name="connection",
                status=HealthStatus.UNHEALTHY,
                message=f"Connection failed: {e}",
                duration_ms=duration_ms,
            )

    def _determine_overall_status(self, checks: list[HealthCheck]) -> HealthStatus:
        """Determine overall health status from individual checks."""
        if all(c.status == HealthStatus.HEALTHY for c in checks):
            return HealthStatus.HEALTHY
        elif any(c.status == HealthStatus.UNHEALTHY for c in checks):
            return HealthStatus.UNHEALTHY
        else:
            return HealthStatus.DEGRADED
