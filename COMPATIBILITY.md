# KubeMQ Python SDK — Compatibility Matrix

## SDK ↔ Server Compatibility

| SDK Version | Server ≥ 2.2 | Server 2.4.x | Server 2.5.x | Server 2.6.x | Notes |
|-------------|-------------|-------------|-------------|-------------|-------|
| v3.x        | ✅          | ✅          | ✅          | ✅          | Legacy sync-only API |
| v4.0.x      | ⚠️           | ✅          | ✅          | ✅          | v4 tested against 2.4+ |

✅ = Fully tested   ⚠️ = Expected to work, not actively tested   ❌ = Not supported

## Python Version Compatibility

| SDK Version | Python 3.9 | Python 3.10 | Python 3.11 | Python 3.12 | Python 3.13 |
|-------------|-----------|------------|------------|------------|------------|
| v3.x        | ✅        | ✅         | ✅         | ✅         | ⚠️          |
| v4.0.x      | ✅         | ✅          | ✅         | ✅         | ✅         |

## How We Test

- CI runs the full test suite against Python 3.9, 3.10, 3.11, 3.12, and 3.13.
- Server compatibility is tested against the latest patch of each minor version in the range.
- The SDK logs a WARNING if the server version is outside the tested range but does NOT refuse the connection.

## Updating This Matrix

When releasing a new SDK version:
1. Update the matrix rows above.
2. Update `MIN_TESTED_SERVER_VERSION` / `MAX_TESTED_SERVER_VERSION` in `src/kubemq/_internal/compat.py`.
