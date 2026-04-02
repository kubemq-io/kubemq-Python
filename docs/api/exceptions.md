# Exceptions API Reference

## Exception Hierarchy

All exceptions inherit from `KubeMQError`:

```
KubeMQError
├── KubeMQConnectionError
│   ├── KubeMQConnectionNotReadyError
│   └── KubeMQStreamBrokenError
├── KubeMQAuthenticationError
├── KubeMQTimeoutError
├── KubeMQValidationError
│   └── KubeMQConfigurationError
├── KubeMQChannelError
├── KubeMQMessageError
│   └── KubeMQBufferFullError
├── KubeMQTransactionError
├── KubeMQTransportError
├── KubeMQHandlerError
├── KubeMQCancellationError
├── KubeMQClientClosedError
└── KubeMQCircuitOpenError
```

## Base Exception

::: kubemq.core.exceptions.KubeMQError

## Connection Errors

::: kubemq.core.exceptions.KubeMQConnectionError

::: kubemq.core.exceptions.KubeMQConnectionNotReadyError

::: kubemq.core.exceptions.KubeMQStreamBrokenError

## Authentication Errors

::: kubemq.core.exceptions.KubeMQAuthenticationError

## Operation Errors

::: kubemq.core.exceptions.KubeMQTimeoutError

::: kubemq.core.exceptions.KubeMQValidationError

::: kubemq.core.exceptions.KubeMQConfigurationError

::: kubemq.core.exceptions.KubeMQChannelError

::: kubemq.core.exceptions.KubeMQMessageError

::: kubemq.core.exceptions.KubeMQBufferFullError

::: kubemq.core.exceptions.KubeMQTransactionError

## Transport and Handler Errors

::: kubemq.core.exceptions.KubeMQTransportError

::: kubemq.core.exceptions.KubeMQHandlerError

## Lifecycle Errors

::: kubemq.core.exceptions.KubeMQCancellationError

::: kubemq.core.exceptions.KubeMQClientClosedError

::: kubemq.core.exceptions.KubeMQCircuitOpenError

## Utilities

::: kubemq.core.exceptions.from_grpc_error

::: kubemq.core.exceptions.classify_error

::: kubemq.core.exceptions.ErrorCode

::: kubemq.core.exceptions.ErrorCategory
