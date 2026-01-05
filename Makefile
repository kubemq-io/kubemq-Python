.PHONY: help all clean test install-dependencies install-git-hooks format format-check lint lint-check \
        typecheck-fast typecheck-strict typecheck-all security-check \
        test-unit test-unit-cov test-integration test-all quality quality-check

# ==============================================================================
# HELP
# ==============================================================================
help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "STANDARD TARGETS"
	@echo "  all                   Run quality checks and unit tests"
	@echo "  test                  Alias for test-unit"
	@echo "  clean                 Clean Python and tool caches"
	@echo ""
	@echo "INSTALLATION"
	@echo "  install-dependencies  Install all Python packages (dev, lint, typechecking)"
	@echo "  install-git-hooks     Install pre-commit hooks (runs quality checks on commit)"
	@echo ""
	@echo "LOCAL DEVELOPMENT (auto-fix enabled)"
	@echo "  format                Auto-fix formatting with ruff"
	@echo "  lint                  Lint and auto-fix with ruff"
	@echo "  quality               Run all git hook checks (format, lint, typecheck, security)"
	@echo ""
	@echo "CI / READ-ONLY (no auto-fix)"
	@echo "  format-check          Check formatting only"
	@echo "  lint-check            Check linting only"
	@echo "  quality-check         All git hook checks without auto-fixing"
	@echo ""
	@echo "TYPE CHECKING"
	@echo "  typecheck-fast        Fast checks: pyrefly, ty"
	@echo "  typecheck-strict      Strict checks: basedpyright, mypy"
	@echo "  typecheck-all         Run all type checkers"
	@echo ""
	@echo "SECURITY"
	@echo "  security-check        Scan code (bandit) and dependencies (pip-audit)"
	@echo ""
	@echo "TESTING"
	@echo "  test-unit             Run unit tests"
	@echo "  test-unit-cov         Run unit tests with coverage (60% min)"
	@echo "  test-integration      Run integration tests"
	@echo "  test-all              Run all tests (unit + integration)"

# ==============================================================================
# STANDARD TARGETS
# ==============================================================================
all: quality test-unit

test: test-unit

clean:
	@echo "▶ Cleaning caches..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	rm -rf .pytest_cache .mypy_cache .ruff_cache htmlcov coverage.xml .coverage
	@echo "✓ Caches cleaned!"

# ==============================================================================
# INSTALLATION
# ==============================================================================
install-dependencies:
	@echo "▶ Installing dependencies..."
	uv sync --frozen --all-groups

install-git-hooks:
	@echo "▶ Installing git hooks..."
	uv run pre-commit install
	@echo "✓ Pre-commit hooks installed!"

# ==============================================================================
# CODE FORMATTING
# ==============================================================================
format:
	@echo "▶ Formatting code (ruff)..."
	uv run ruff format src tests/unit tests/integration

format-check:
	@echo "▶ Checking formatting (ruff)..."
	uv run ruff format --check src tests/unit tests/integration

# ==============================================================================
# LINTING
# ==============================================================================
lint:
	@echo "▶ Linting code (ruff)..."
	uv run ruff check --fix src tests/unit tests/integration

lint-check:
	@echo "▶ Checking linting (ruff)..."
	uv run ruff check src tests/unit tests/integration

# ==============================================================================
# TYPE CHECKING
# ==============================================================================
typecheck-fast:
	@echo "▶ Type checking (pyrefly)..."
	uv run pyrefly check src tests/unit tests/integration
	@echo "▶ Type checking (ty)..."
	uv run ty check src tests/unit tests/integration

typecheck-strict:
	@echo "▶ Type checking (basedpyright)..."
	uv run basedpyright src tests/unit
	@echo "▶ Type checking (mypy)..."
	uv run mypy src tests/unit tests/integration

typecheck-all: typecheck-fast typecheck-strict

# ==============================================================================
# SECURITY
# ==============================================================================
security-check:
	@echo "▶ Security scan (bandit)..."
	uv run bandit -r src tests -c pyproject.toml -lll
	@echo "▶ Dependency audit (pip-audit)..."
	uv run pip-audit

# ==============================================================================
# TESTING
# ==============================================================================
test-unit:
	@echo "▶ Running unit tests..."
	uv run pytest tests/unit/ tests/integration

test-unit-cov:
	@echo "▶ Running unit tests with coverage..."
	uv run pytest tests/unit/ \
		--cov=src --cov-report=term-missing --cov-report=xml --cov-fail-under=60

test-integration:
	@echo "▶ Running integration tests..."
	uv run pytest tests/integration/

test-all: test-unit test-integration

# ==============================================================================
# COMBINED TARGETS
# ==============================================================================
quality: format lint typecheck-fast security-check
	@echo "✓ All commit-level quality checks passed!"

quality-check: format-check lint-check typecheck-fast security-check
	@echo "✓ All commit-level quality checks passed!"
