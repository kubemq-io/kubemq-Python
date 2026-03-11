# KubeMQ Python SDK — Security Policy

## Reporting Vulnerabilities

If you discover a security vulnerability in this SDK, please report it responsibly:

1. **Do NOT open a public issue.**
2. Email [security@kubemq.io](mailto:security@kubemq.io) with details.
3. We will acknowledge receipt within 48 hours and provide a timeline for a fix.

## Dependency Management

- Runtime dependencies are scanned automatically by [Dependabot](.github/dependabot.yml).
- `pip-audit` is run in CI to detect known vulnerabilities.
- See `pyproject.toml` for the full dependency justification table.

### Dependency Justification

| Dependency | Purpose | Runtime Required? |
|-----------|---------|-------------------|
| `grpcio>=1.51.0` | gRPC transport for server communication | Yes |
| `protobuf>=4.21.0` | Protocol Buffers serialization | Yes |
| `pydantic>=2.0.0` | Data validation and settings | Yes |
| `PyJWT>=2.6.0` | JWT token handling for auth | Yes |
| `grpcio-tools>=1.51.0` | Protobuf code generation | No — dev only |

## SBOM Generation (Optional)

To generate a CycloneDX SBOM for a release:

```
pip install cyclonedx-bom
cyclonedx-py environment --output sbom.json --format json
```

This is recommended for compliance but not required for every release.
