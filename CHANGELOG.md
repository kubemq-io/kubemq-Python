# Changelog

All notable changes to the KubeMQ Python SDK will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [4.0.0] - 2026-03-16

### Added
- Native async clients: `AsyncPubSubClient`, `AsyncQueuesClient`, `AsyncCQClient`
  with `async with` context manager support
- `AsyncCancellationToken` for cooperative async cancellation
- GS-aligned verb aliases: `publish_event()`, `publish_event_store()`,
  `send_queue_message()`, `receive_queue_messages()`, `send_command()`,
  `send_query()` (old names deprecated, removed in v5)
- `ClientConfig` dataclass for centralized configuration
- `TLSConfig` and `KeepAliveConfig` frozen dataclasses
- `RetryPolicy` and `OperationTimeouts` configuration types
- Health checking: `HealthChecker`, `AsyncHealthChecker`, `HealthReport`
- `CancellationToken` and `AsyncCancellationToken` for subscription lifecycle
- Pydantic v2 message models with validation
- Channel statistics types: `PubSubChannel`, `QueuesChannel`, `CQChannel`
- Typed exception hierarchy: `KubeMQError`, `KubeMQConnectionError`,
  `KubeMQAuthenticationError`, `KubeMQTimeoutError`, `KubeMQValidationError`,
  `KubeMQChannelError`, `KubeMQMessageError`, `KubeMQTransactionError`,
  `KubeMQConfigurationError`, `KubeMQCircuitOpenError`, `KubeMQClientClosedError`,
  `KubeMQConnectionNotReadyError`, `KubeMQBufferFullError`,
  `KubeMQCancellationError`, `KubeMQTransportError`, `KubeMQHandlerError`,
  `KubeMQStreamBrokenError`
- `ErrorCode` and `ErrorCategory` enums for programmatic error inspection
- `classify_error()` and `from_grpc_error()` utility functions
- Context manager support (`with`/`async with`) for all client types
- Full type annotations and `py.typed` marker (PEP 561)
- OpenTelemetry integration via `kubemq[otel]` optional dependency
- Structured logging with `StdLibLoggerAdapter` and `NoOpLogger`
- Auto-generated API documentation with mkdocs + mkdocstrings
- Comprehensive examples for all 5 messaging patterns

### Changed
- **BREAKING:** Minimum Python version raised from 3.8 to 3.11
- **BREAKING:** Package structure reorganized under `src/kubemq/` layout —
  all imports from `kubemq` top-level
  (e.g., `from kubemq import PubSubClient` instead of
  `from kubemq.pubsub import Client`)
- **BREAKING:** Client constructors accept keyword arguments via flat parameters
  or `ClientConfig` object instead of individual positional parameters
- **BREAKING:** `tls` parameter changed from `bool` to `TLSConfig` dataclass;
  `tls_cert_file`, `tls_key_file`, `tls_ca_file` moved into `TLSConfig`
- **BREAKING:** Sync `*_async()` methods on sync clients replaced by dedicated
  `AsyncPubSubClient`, `AsyncQueuesClient`, `AsyncCQClient` classes
- **BREAKING:** `disable_auto_reconnect` parameter replaced by `auto_reconnect`
  (inverted boolean)
- **BREAKING:** Keep-alive parameters consolidated into `KeepAliveConfig` dataclass
- Event/message models now use Pydantic v2 `BaseModel` with validation
- gRPC errors wrapped in typed `KubeMQ*Error` exceptions instead of raw
  `grpc.RpcError`
- Build system changed from setuptools to hatchling
- gRPC dependency pinned to `>=1.51.0` for async interceptor support
- Version detection via `importlib.metadata` (single source of truth)

### Removed
- **BREAKING:** Legacy `kubemq.commandquery` module (replaced by `kubemq.cq`)
- **BREAKING:** Legacy `kubemq.events` module (replaced by `kubemq.pubsub`)
- **BREAKING:** Global singleton client pattern
- **BREAKING:** `Client` class from top-level `kubemq` package (use pattern-specific
  clients: `PubSubClient`, `QueuesClient`, `CQClient`)
- **BREAKING:** `*_async()` wrapper methods on sync clients (deprecated, use
  dedicated async clients)
- setuptools build configuration (`setup.py`, `setup.cfg`)
- External cookbook repository reference

### Fixed
- Connection handling now properly closes gRPC channels on context manager exit
- Thread safety improvements in sync client subscription management
- Subscription streams auto-retry with exponential backoff on transient errors

## [3.6.0] - 2024-01-15

### Added
- Queue stream message support
- Improved error messages for connection failures

### Fixed
- Connection retry logic for transient gRPC errors
- Memory leak in long-running subscription handlers

## [3.5.0] - 2023-08-10

### Added
- Events store subscription with replay options
- Start position configuration for events store

[Unreleased]: https://github.com/kubemq-io/kubemq-Python/compare/v4.0.0...HEAD
[4.0.0]: https://github.com/kubemq-io/kubemq-Python/compare/v3.6.0...v4.0.0
[3.6.0]: https://github.com/kubemq-io/kubemq-Python/compare/v3.5.0...v3.6.0
[3.5.0]: https://github.com/kubemq-io/kubemq-Python/releases/tag/v3.5.0
