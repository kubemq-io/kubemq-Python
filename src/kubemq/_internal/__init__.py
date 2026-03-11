"""Internal implementation package — NOT part of the public API.

Modules under ``kubemq._internal`` are private by naming convention
(leading underscore). They may change without notice between minor
versions. Users should import exclusively from ``kubemq`` or the
public subpackages (``kubemq.core``, ``kubemq.pubsub``, etc.).

Subpackages:
    middleware/   Protocol/middleware layer (retry, auth, OTel pipeline)
    transport/    Transport-layer internals (connection state machine)

Modules:
    auth.py          Token holder for auth interceptors
    compat.py        Server version compatibility check
    deprecation.py   PEP 565-compliant deprecation decorators
    logging.py       Logger implementations (NoOpLogger, StdLibLoggerAdapter)
    telemetry.py     OTel integration with graceful degradation
    retry.py         Retry executor and backoff calculator
"""
