# Installation

## Requirements

- **Python 3.11+**
- A running [KubeMQ server](https://github.com/kubemq-io/kubemq-community)

## Install from PyPI

```bash
pip install kubemq
```

## Optional Dependencies

```bash
pip install kubemq[docs]    # mkdocs + mkdocstrings for API docs
pip install kubemq[otel]    # OpenTelemetry tracing integration
```

## Start KubeMQ Server

The fastest way to get a KubeMQ server running locally:

```bash
docker run -d -p 8080:8080 -p 50000:50000 kubemq/kubemq-community
```

Verify it's running:

```bash
curl http://localhost:8080/health
```

## Verify Installation

```python
import kubemq
print(f"KubeMQ SDK version: {kubemq.__version__}")

from kubemq import PubSubClient
with PubSubClient(address="localhost:50000") as client:
    info = client.ping()
    print(f"Connected to KubeMQ {info.version} at {info.host}")
```

## Development Installation

```bash
git clone https://github.com/kubemq-io/kubemq-Python.git
cd kubemq-Python
pip install -e ".[docs]"
```
