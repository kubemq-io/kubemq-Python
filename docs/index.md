# Documentation Index

This project's documentation is organized following the [Diataxis framework](https://diataxis.fr/) into four categories.

## Tutorials (Learning-oriented)

Step-by-step guides that teach through doing.

- [Getting Started with Events](tutorials/getting-started-events.md)
- [Building a Task Queue](tutorials/building-a-task-queue.md)
- [Request-Reply with Commands](tutorials/request-reply-with-commands.md)

## How-To Guides (Task-oriented)

Practical guides for specific tasks.

- [Connect with TLS/mTLS](how-to/connect-with-tls.md)
- [Handle Errors and Retries](how-to/handle-errors-and-retries.md)
- [Use Consumer Groups](how-to/use-consumer-groups.md)
- [Monitor with OpenTelemetry](how-to/monitor-with-opentelemetry.md)

## Reference (Information-oriented)

Technical descriptions of the machinery.

- [README](../README.md) — Installation, quickstart, configuration reference
- [API Reference: Config](api/config.md)
- [API Reference: PubSub](api/pubsub.md)
- [API Reference: CQ (Commands & Queries)](api/cq.md)
- [API Reference: Queues](api/queues.md)
- [API Reference: Types](api/types.md)
- [API Reference: Exceptions](api/exceptions.md)
- [CHANGELOG](../CHANGELOG.md)
- [COMPATIBILITY](../COMPATIBILITY.md)
- [CONTRIBUTING](../CONTRIBUTING.md)

## Explanation (Understanding-oriented)

Background and context.

- [KubeMQ Concepts](CONCEPTS.md)
- [Troubleshooting](troubleshooting.md)
- [Migration v3 → v4](migration-v3-to-v4.md)
- [SECURITY](../SECURITY.md)
- [Breaking Changes](../BREAKING_CHANGES.md)
- [Deprecation Policy](../DEPRECATION-POLICY.md)
- [Performance](../PERFORMANCE.md)

## Guides

Channel-specific usage guides.

- [Configuration](guide/configuration.md)
- [Events](guide/events.md)
- [Events Store](guide/events-store.md)
- [Queues](guide/queues.md)
- [Commands](guide/commands.md)
- [Queries](guide/queries.md)
- [Error Handling](guide/error-handling.md)
- [Security](guide/security.md)

## Scenario Guides

End-to-end architecture walkthroughs.

- [Event-Driven Processing Pipeline](scenarios/event-driven-pipeline.md)
- [Implementing CQRS with KubeMQ](scenarios/microservice-cqrs.md)
