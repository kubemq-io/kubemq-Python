"""Example: OpenTelemetry setup — configure tracing and metrics with OTel providers.

Requires the optional 'otel' dependency:
    pip install kubemq[otel]
    pip install opentelemetry-sdk opentelemetry-exporter-otlp
"""

from __future__ import annotations

from kubemq import ClientConfig, EventMessage, PubSubClient

# OpenTelemetry imports — require optional otel dependency
try:
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor, ConsoleSpanExporter
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import ConsoleMetricExporter, PeriodicExportingMetricReader

    HAS_OTEL = True
except ImportError:
    HAS_OTEL = False


def main() -> None:
    if not HAS_OTEL:
        print(
            "OpenTelemetry SDK not installed. Install with:\n"
            "  pip install kubemq[otel] opentelemetry-sdk"
        )
        print("\nShowing configuration pattern without actual OTel providers:")

        # Even without OTel SDK installed, you can still use the config fields
        config = ClientConfig(
            address="localhost:50000",
            client_id="python-observability-opentelemetry-setup-client",
            tracer_provider=None,
            meter_provider=None,
        )
        print(f"Config created: {config}")
        return

    # Set up OpenTelemetry tracing
    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(
        SimpleSpanProcessor(ConsoleSpanExporter())
    )
    trace.set_tracer_provider(tracer_provider)

    # Set up OpenTelemetry metrics
    metric_reader = PeriodicExportingMetricReader(
        ConsoleMetricExporter(),
        export_interval_millis=5000,
    )
    meter_provider = MeterProvider(metric_readers=[metric_reader])

    # Configure KubeMQ client with OTel providers
    config = ClientConfig(
        address="localhost:50000",
        client_id="python-observability-opentelemetry-setup-client",
        tracer_provider=tracer_provider,
        meter_provider=meter_provider,
        # Cardinality management for metrics
        max_channel_cardinality=100,
        channel_allowlist=["python-observability.*"],
    )

    with PubSubClient(config=config) as client:
        info = client.ping()
        print(f"Connected to {info.host} with OTel instrumentation")

        # Operations will emit traces and metrics
        client.publish_event(
            EventMessage(
                channel="python-observability.opentelemetry-setup",
                body=b"Traced and metered message",
            )
        )
        print("Event sent with OpenTelemetry tracing and metrics")

    # Shutdown OTel providers
    tracer_provider.shutdown()
    meter_provider.shutdown()
    print("OTel providers shut down")


if __name__ == "__main__":
    main()
