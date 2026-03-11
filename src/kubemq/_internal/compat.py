"""Client-server compatibility check utility.

Validates server version against the tested compatibility range
and emits advisory warnings. Never blocks connections.
"""

from __future__ import annotations

import logging
import re
from typing import Optional

MIN_TESTED_SERVER_VERSION = "2.4.0"
MAX_TESTED_SERVER_VERSION = "2.6.0"


def parse_version_tuple(version_str: str) -> Optional[tuple[int, ...]]:
    """Parse a version string like '2.4.1' into a comparable tuple.

    Returns None if the version string cannot be parsed.
    """
    match = re.match(r"^v?(\d+(?:\.\d+)*)", version_str)
    if not match:
        return None
    return tuple(int(part) for part in match.group(1).split("."))


def check_server_compatibility(
    server_version: str,
    logger: logging.Logger,
) -> None:
    """Log a warning if server version is outside the tested range.

    Does NOT raise — connection always proceeds.
    """
    parsed = parse_version_tuple(server_version)
    if parsed is None:
        logger.warning(
            "Could not parse server version '%s'; "
            "compatibility cannot be verified. "
            "Tested range: %s – %s",
            server_version,
            MIN_TESTED_SERVER_VERSION,
            MAX_TESTED_SERVER_VERSION,
        )
        return

    min_ver = parse_version_tuple(MIN_TESTED_SERVER_VERSION)
    max_ver = parse_version_tuple(MAX_TESTED_SERVER_VERSION)

    if min_ver and parsed < min_ver:
        logger.warning(
            "KubeMQ server version '%s' is older than the tested minimum (%s). "
            "The SDK may not function correctly. "
            "See COMPATIBILITY.md for the full matrix.",
            server_version,
            MIN_TESTED_SERVER_VERSION,
        )
    elif max_ver and parsed > max_ver:
        logger.warning(
            "KubeMQ server version '%s' is newer than the tested maximum (%s). "
            "Some features may behave unexpectedly. "
            "See COMPATIBILITY.md for the full matrix.",
            server_version,
            MAX_TESTED_SERVER_VERSION,
        )
