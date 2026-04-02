"""Payload encoding/decoding with CRC32 integrity verification."""

from __future__ import annotations

import json
import os
import random
import time
import zlib
from dataclasses import dataclass


@dataclass
class Message:
    """Wire format for burn-in message payload."""

    sdk: str
    pattern: str
    producer_id: str
    sequence: int
    timestamp_ns: int
    payload_padding: str = ""


def encode(
    sdk: str,
    pattern: str,
    producer_id: str,
    sequence: int,
    target_size: int,
) -> tuple[bytes, str]:
    """Encode a burn-in message, returning (body, crc_hex).

    Random padding is added to reach target_size.
    CRC32 is computed on the final JSON body.
    """
    msg = {
        "sdk": sdk,
        "pattern": pattern,
        "producer_id": producer_id,
        "sequence": sequence,
        "timestamp_ns": time.time_ns(),
    }

    # Estimate base size without serializing twice.
    # Padding key overhead: ',"payload_padding":"..."' = 22 chars + padding
    base_estimate = 50 + len(sdk) + len(pattern) + len(producer_id) + 20  # ~digits
    overhead = 22  # key + quotes + comma
    if target_size > base_estimate + overhead:
        padding_len = target_size - base_estimate - overhead
        if padding_len > 0:
            msg["payload_padding"] = _random_padding(padding_len)

    body = json.dumps(msg, separators=(",", ":")).encode()
    crc = zlib.crc32(body) & 0xFFFFFFFF
    crc_hex = f"{crc:08x}"
    return body, crc_hex


def decode(body: bytes) -> Message:
    """Decode a burn-in message from JSON body."""
    d = json.loads(body)
    return Message(
        sdk=d.get("sdk", ""),
        pattern=d.get("pattern", ""),
        producer_id=d.get("producer_id", ""),
        sequence=d.get("sequence", 0),
        timestamp_ns=d.get("timestamp_ns", 0),
        payload_padding=d.get("payload_padding", ""),
    )


def verify_crc(body: bytes, crc_hex: str) -> bool:
    """Verify CRC32 of body matches expected hex string."""
    actual = zlib.crc32(body) & 0xFFFFFFFF
    return f"{actual:08x}" == crc_hex


class SizeDistribution:
    """Weighted random message size selection."""

    def __init__(self, spec: str) -> None:
        self.sizes: list[int] = []
        self.weights: list[int] = []
        self.total = 0
        for pair in spec.split(","):
            pair = pair.strip()
            if ":" not in pair:
                continue
            size_s, weight_s = pair.split(":", 1)
            self.sizes.append(int(size_s))
            self.weights.append(int(weight_s))
            self.total += int(weight_s)

    def select_size(self) -> int:
        """Select a random size based on weights."""
        r = random.randint(1, self.total)
        cumulative = 0
        for size, weight in zip(self.sizes, self.weights):
            cumulative += weight
            if r <= cumulative:
                return size
        return self.sizes[-1] if self.sizes else 1024


def _random_padding(n: int) -> str:
    """Generate n bytes of random printable ASCII (33-126).

    Uses os.urandom().hex() for speed (~4µs vs ~36µs for chr() loop).
    Hex chars (0-9, a-f) are all printable ASCII, safe for JSON.
    """
    return os.urandom((n + 1) // 2).hex()[:n]


# ---------------------------------------------------------------------------
# Benchmark-mode fast payload (binary struct, no JSON, no CRC per-message)
# ---------------------------------------------------------------------------
import struct

# Header: 8-byte sequence (uint64) + 8-byte timestamp_ns (uint64) = 16 bytes
_BENCH_HEADER = struct.Struct("!QQ")
_BENCH_HEADER_SIZE = _BENCH_HEADER.size  # 16 bytes

# Pre-allocated padding buffer (created once, reused)
_bench_padding: bytes = b""


def _ensure_bench_padding(size: int) -> None:
    """Ensure the pre-allocated padding buffer is large enough."""
    global _bench_padding
    if len(_bench_padding) < size:
        _bench_padding = os.urandom(max(size, 4096))


def encode_fast(sequence: int, target_size: int) -> bytes:
    """Encode a benchmark payload: binary header + pre-allocated padding.

    No JSON, no CRC, no per-message random generation.
    Returns raw bytes (no crc_hex — caller should use empty string).
    """
    header = _BENCH_HEADER.pack(sequence, time.time_ns())
    pad_len = max(0, target_size - _BENCH_HEADER_SIZE)
    if pad_len > 0:
        _ensure_bench_padding(pad_len)
        return header + _bench_padding[:pad_len]
    return header


def decode_fast(body: bytes) -> tuple[int, int]:
    """Decode a benchmark payload. Returns (sequence, timestamp_ns).

    No JSON parsing, just struct unpack of first 16 bytes.
    """
    if len(body) >= _BENCH_HEADER_SIZE:
        return _BENCH_HEADER.unpack_from(body, 0)
    return 0, 0
