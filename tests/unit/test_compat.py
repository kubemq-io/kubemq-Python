"""Tests for client-server compatibility checking."""

from __future__ import annotations

import logging

from kubemq._internal.compat import (
    MAX_TESTED_SERVER_VERSION,
    MIN_TESTED_SERVER_VERSION,
    check_server_compatibility,
    parse_version_tuple,
)


class TestParseVersionTuple:
    def test_basic_version(self):
        assert parse_version_tuple("2.4.1") == (2, 4, 1)

    def test_version_with_v_prefix(self):
        assert parse_version_tuple("v2.4.1") == (2, 4, 1)

    def test_two_part_version(self):
        assert parse_version_tuple("2.4") == (2, 4)

    def test_invalid_version_returns_none(self):
        assert parse_version_tuple("not-a-version") is None

    def test_empty_string_returns_none(self):
        assert parse_version_tuple("") is None

    def test_single_number(self):
        assert parse_version_tuple("3") == (3,)

    def test_four_part_version(self):
        assert parse_version_tuple("1.2.3.4") == (1, 2, 3, 4)


class TestCheckServerCompatibility:
    def test_in_range_no_warning(self, caplog):
        with caplog.at_level(logging.WARNING):
            check_server_compatibility(MIN_TESTED_SERVER_VERSION, logging.getLogger("test"))
        assert len(caplog.records) == 0

    def test_max_version_no_warning(self, caplog):
        with caplog.at_level(logging.WARNING):
            check_server_compatibility(MAX_TESTED_SERVER_VERSION, logging.getLogger("test"))
        assert len(caplog.records) == 0

    def test_mid_range_no_warning(self, caplog):
        with caplog.at_level(logging.WARNING):
            check_server_compatibility("2.5.0", logging.getLogger("test"))
        assert len(caplog.records) == 0

    def test_below_min_warns(self, caplog):
        with caplog.at_level(logging.WARNING):
            check_server_compatibility("1.0.0", logging.getLogger("test"))
        assert any("older than the tested minimum" in r.message for r in caplog.records)

    def test_above_max_warns(self, caplog):
        with caplog.at_level(logging.WARNING):
            check_server_compatibility("99.0.0", logging.getLogger("test"))
        assert any("newer than the tested maximum" in r.message for r in caplog.records)

    def test_unparseable_version_warns(self, caplog):
        with caplog.at_level(logging.WARNING):
            check_server_compatibility("unknown", logging.getLogger("test"))
        assert any("Could not parse" in r.message for r in caplog.records)

    def test_connection_always_proceeds(self):
        """check_server_compatibility never raises."""
        logger = logging.getLogger("test")
        check_server_compatibility("0.0.1", logger)
        check_server_compatibility("999.999.999", logger)
        check_server_compatibility("garbage!!!", logger)
