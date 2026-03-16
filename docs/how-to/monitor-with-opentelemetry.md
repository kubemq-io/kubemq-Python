# How to Monitor with OpenTelemetry

Instrument your KubeMQ Python client with OpenTelemetry tracing and metrics for end-to-end observability.

## Install Dependencies

```bash
pip install kubemq[otel]
pip install opentelemetry-sdk
```

For production exporters:

```bash
# Console exporter (included in opentelemetry-sdk)
# OTLP exporter for Jaeger/Grafana/etc.
pip install opentelemetry-exporter-otlp
```

## Configure Tracing

Create a `TracerProvider` and pass it to the KubeMQ client via `ClientConfig`:

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, ConsoleSpanExporter

from kubemq import ClientConfig, EventMessage, PubSubClient

# Set up tracing with console output
tracer_provider = TracerProvider()
tracer_provider.add_span_processor(
    SimpleSpanProcessor(ConsoleSpanExporter())
)
trace.set_tracer_provider(tracer_provider)

config = ClientConfig(
    address="localhost:50000",
    client_id="traced-client",
    tracer_provider=tracer_provider,
)

with PubSubClient(config=config) as client:
    client.publish_event(
        EventMessage(
            channel="traced.events",
            body=b"hello from traced client",
        )
    )
    print("Trace exported — check console output above")

tracer_provider.shutdown()
```

The SDK automatically creates spans for every operation (`publish_event`, `send_command_request`, `send_query_request`, etc.) with channel name, client ID, and error status as span attributes.

## Configure Metrics

Create a `MeterProvider` and pass it via `ClientConfig`:

```python
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    ConsoleMetricExporter,
    PeriodicExportingMetricReader,
)

from kubemq import ClientConfig, EventMessage, PubSubClient

metric_reader = PeriodicExportingMetricReader(
    ConsoleMetricExporter(),
    export_interval_millis=5000,
)
meter_provider = MeterProvider(metric_readers=[metric_reader])

config = ClientConfig(
    address="localhost:50000",
    client_id="metered-client",
    meter_provider=meter_provider,
    max_channel_cardinality=100,
)

with PubSubClient(config=config) as client:
    for i in range(10):
        client.publish_event(
            EventMessage(channel="metered.events", body=f"event-{i}".encode())
        )

import time
time.sleep(6)
meter_provider.shutdown()
print("Metrics exported — check console output above")
```

`max_channel_cardinality` caps the number of unique channel labels to prevent high-cardinality metric series.

## Combined Tracing + Metrics

Pass both providers to a single `ClientConfig`:

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    ConsoleMetricExporter,
    PeriodicExportingMetricReader,
)

from kubemq import ClientConfig, EventMessage, PubSubClient

# Tracing
tracer_provider = TracerProvider()
tracer_provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
trace.set_tracer_provider(tracer_provider)

# Metrics
metric_reader = PeriodicExportingMetricReader(
    ConsoleMetricExporter(), export_interval_millis=5000
)
meter_provider = MeterProvider(metric_readers=[metric_reader])

config = ClientConfig(
    address="localhost:50000",
    client_id="fully-instrumented",
    tracer_provider=tracer_provider,
    meter_provider=meter_provider,
    max_channel_cardinality=100,
    channel_allowlist=["traced.*", "metered.*"],
)

with PubSubClient(config=config) as client:
    info = client.ping()
    print(f"Connected with OTel: {info.host}")

    client.publish_event(
        EventMessage(channel="traced.events", body=b"instrumented message")
    )

tracer_provider.shutdown()
meter_provider.shutdown()
```

`channel_allowlist` restricts which channels emit metrics, giving you fine-grained control over metric cardinality.

## Export to Jaeger via OTLP

Replace console exporters with OTLP for production:

```python
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

exporter = OTLPSpanExporter(endpoint="http://localhost:4318/v1/traces")
tracer_provider = TracerProvider()
tracer_provider.add_span_processor(BatchSpanProcessor(exporter))
```

Then view traces at `http://localhost:16686` (Jaeger UI).

## Provider Shutdown

Always shut down providers before exiting to flush pending telemetry:

```python
try:
    # ... application logic ...
    pass
finally:
    tracer_provider.shutdown()
    meter_provider.shutdown()
```

For async applications, use the async shutdown methods if available.

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `ImportError: No module named 'opentelemetry'` | OTel SDK not installed | Run `pip install opentelemetry-sdk` |
| No traces appear | `tracer_provider` not passed in `ClientConfig` | Set `tracer_provider=tp` in `ClientConfig` |
| No metrics appear | `meter_provider` not passed or export interval too long | Set `meter_provider=mp` and reduce `export_interval_millis` |
| High-cardinality warnings | Too many unique channel names | Lower `max_channel_cardinality` or use `channel_allowlist` |
| Traces missing after program exits | Provider not shut down | Always call `tracer_provider.shutdown()` before exit |
| OTLP exporter connection refused | Collector not running | Start the OTLP collector at the configured endpoint |
