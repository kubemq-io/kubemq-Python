"""Tests for kubemq.transport.server_info module."""

from __future__ import annotations

import pytest

from kubemq.transport.server_info import ServerInfo


class TestServerInfoValidation:

    def test_valid_construction(self):
        info = ServerInfo(
            host="localhost",
            version="1.0.0",
            server_start_time=1000,
            server_up_time_seconds=500,
        )
        assert info.host == "localhost"
        assert info.version == "1.0.0"
        assert info.server_start_time == 1000
        assert info.server_up_time_seconds == 500

    def test_negative_start_time_raises(self):
        with pytest.raises(ValueError, match="non-negative"):
            ServerInfo(
                host="h",
                version="v",
                server_start_time=-1,
                server_up_time_seconds=0,
            )

    def test_negative_uptime_raises(self):
        with pytest.raises(ValueError, match="non-negative"):
            ServerInfo(
                host="h",
                version="v",
                server_start_time=0,
                server_up_time_seconds=-5,
            )

    def test_frozen_model(self):
        info = ServerInfo(
            host="h", version="v", server_start_time=0, server_up_time_seconds=0
        )
        with pytest.raises(Exception):
            info.host = "new"


class TestServerInfoStr:

    def test_str_representation(self):
        info = ServerInfo(
            host="my-host",
            version="2.0",
            server_start_time=100,
            server_up_time_seconds=50,
        )
        s = str(info)
        assert "my-host" in s
        assert "2.0" in s
        assert "100" in s
        assert "50" in s
