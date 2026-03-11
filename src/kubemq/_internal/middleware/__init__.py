"""Middleware pipeline for the Protocol layer.

This package houses the middleware components that sit between the
Public API layer and the Transport layer in the 3-layer architecture::

    ┌─────────────────────────────────┐
    │  Public API Layer               │  ← User-facing types, client classes
    │  (__init__.py, pubsub/, etc.)   │     No gRPC imports
    ├─────────────────────────────────┤
    │  Protocol Layer                 │  ← Error mapping, retry, auth, OTel
    │  (_internal/middleware/)        │     Wraps TransportProtocol
    ├─────────────────────────────────┤
    │  Transport Layer                │  ← gRPC stubs, connection, keepalive
    │  (transport/, grpc/)            │     Only layer importing grpc
    └─────────────────────────────────┘

Pipeline ordering: PublicAPI → Retry → Auth → OTel → Transport

Middleware implementations are owned by their respective specs:
    - RetryExecutor: 01-error-handling-spec.md (SPEC-ERR-3)
    - TokenManager: 03-auth-security-spec.md
    - KubeMQInstrumentor: 05-observability-spec.md

This package provides the pipeline structure and wiring.
"""
