FROM --platform=$BUILDPLATFORM python:3.12-slim AS builder

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy SDK sources (parent directory)
COPY src/ /app/sdk/src/
COPY pyproject.toml /app/sdk/pyproject.toml

# Copy burnin app
COPY burnin/pyproject.toml /app/burnin/pyproject.toml
COPY burnin/src/ /app/burnin/src/
COPY burnin/burnin-config.yaml /app/burnin/burnin-config.yaml

# Install dependencies
WORKDIR /app/burnin
RUN uv sync --no-dev

FROM --platform=$TARGETPLATFORM python:3.12-slim

# Create non-root user
RUN groupadd -r burnin && useradd -r -g burnin -s /bin/false burnin

WORKDIR /app/burnin

# Copy installed environment
COPY --from=builder /app /app
COPY --from=builder /usr/local/bin/uv /usr/local/bin/uv

USER burnin

EXPOSE 8888

ENTRYPOINT ["uv", "run", "python", "-m", "burnin"]
CMD ["--config", "/etc/burnin/config.yaml"]
