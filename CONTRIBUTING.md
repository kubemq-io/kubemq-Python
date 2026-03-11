# Contributing to KubeMQ Python SDK

Thank you for your interest in contributing! This guide covers the development
workflow, commit conventions, and release process.

## Development Setup

### Prerequisites

- Python 3.9+ (3.11+ recommended for development)
- [uv](https://docs.astral.sh/uv/) (recommended) or pip
- Docker (for integration tests)

### Getting Started

```bash
# Clone the repository
git clone https://github.com/kubemq-io/kubemq-Python.git
cd kubemq-Python

# Create a virtual environment and install dependencies
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows

# Install in editable mode with dev dependencies
pip install -e ".[dev]"

# Or using uv
uv sync --all-extras
```

### Running Tests

```bash
# Unit tests
pytest tests/unit/ -v

# Unit tests with coverage
pytest tests/unit/ --cov=kubemq --cov-report=term-missing

# Integration tests (requires running KubeMQ server)
pytest tests/integration/ -m integration --timeout=60
```

### Code Quality

```bash
# Lint
ruff check src/

# Format
ruff format src/

# Type check
mypy src/kubemq/ --ignore-missing-imports
```

## Commit Messages

We follow [Conventional Commits](https://www.conventionalcommits.org/) format.
This helps maintain a clear project history and enables automated changelog
generation in the future.

### Format

```
type(scope): short description

[optional body]

[optional footer(s)]
```

### Types

| Type | Description | Version Bump |
|------|------------|-------------|
| `feat` | New feature | MINOR |
| `fix` | Bug fix | PATCH |
| `docs` | Documentation only | None |
| `test` | Adding or updating tests | None |
| `refactor` | Code change that neither fixes a bug nor adds a feature | None |
| `perf` | Performance improvement | None |
| `chore` | Maintenance tasks (deps, CI, build) | None |
| `ci` | CI/CD changes | None |

### Breaking Changes

Append `!` after the type or include `BREAKING CHANGE:` in the footer:

```
feat!: remove legacy event handler API

BREAKING CHANGE: EventHandler class removed. Use EventsSubscription instead.
```

### Scopes (Optional)

Use the module or area being changed:

- `pubsub`, `queues`, `cq` — domain modules
- `transport` — gRPC transport layer
- `config` — configuration
- `errors` — error handling
- `deps` — dependencies
- `ci` — CI/CD pipeline

### Examples

```
feat(queues): add batch send support for queue messages
fix(transport): handle gRPC UNAVAILABLE during reconnection
docs: update README with async client examples
test(pubsub): add unit tests for event store subscriptions
chore(deps): update grpcio to 1.60.0
ci: add Python 3.13 to test matrix
```

## Pull Request Process

1. Fork the repository and create a feature branch from `v4`
2. Make your changes following the coding standards below
3. Add or update tests for your changes
4. Ensure all tests pass: `pytest tests/unit/ -v`
5. Ensure code passes linting: `ruff check src/ && ruff format --check src/`
6. Submit a pull request targeting the `v4` branch

### PR Title

Use the same Conventional Commits format for PR titles. The PR title becomes
the merge commit message.

## Coding Standards

- **Style:** Enforced by [ruff](https://docs.astral.sh/ruff/) (see `pyproject.toml`)
- **Types:** All public APIs must have type annotations
- **Docstrings:** Public classes and functions require docstrings (Google style)
- **Tests:** New features require unit tests; bug fixes require regression tests
- **Async:** Prefer `async`/`await` patterns; sync wrappers are secondary

## Release Process

Releases are automated via GitHub Actions. The process:

1. Update `CHANGELOG.md` with the new version's changes under `## [X.Y.Z] - YYYY-MM-DD`
2. Update `version` in `pyproject.toml` to the release version (e.g., `4.0.0`)
3. Commit: `chore(release): prepare X.Y.Z`
4. Tag: `git tag vX.Y.Z`
5. Push: `git push && git push --tags`
6. CI validates → tests → builds → publishes to PyPI → creates GitHub Release
7. After release, update `pyproject.toml` version to next dev (`4.1.0.dev0`)
8. Commit: `chore(release): begin X.Y+1.Z development`

See `.github/workflows/release.yml` for the full pipeline.

## Versioning

This project follows [Semantic Versioning 2.0.0](https://semver.org/):

- **MAJOR** (X.0.0): Breaking API changes
- **MINOR** (0.X.0): New backward-compatible features
- **PATCH** (0.0.X): Backward-compatible bug fixes

Pre-release versions use [PEP 440](https://peps.python.org/pep-0440/) format:
`4.0.0.dev0`, `4.0.0a1`, `4.0.0b1`, `4.0.0rc1`.

## License

By contributing, you agree that your contributions will be licensed under the
MIT License that covers this project.
